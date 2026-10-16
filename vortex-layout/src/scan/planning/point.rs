// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Projects one row from flat leaves without constructing intermediate pipeline ports.

use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::StructFields;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_io::request::IoBatch;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequest;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_scan::planning::morsel::Morsel;
use vortex_scan::planning::morsel::MorselOutput;
use vortex_scan::planning::planner::State;
use vortex_session::VortexSession;

use crate::plan::physical::Concat;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::Pack;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::SegmentScanPlan;
use crate::plan::physical::Zoned;
use crate::plan::physical::operators::assemble_struct;
use crate::plan::physical::operators::decode_segment;
use crate::scan::planning::ScanPlans;
use crate::segments::SegmentId;

#[derive(Default)]
struct Inputs {
    segments: Vec<SegmentId>,
    positions: Option<FxHashMap<SegmentId, usize>>,
}

impl Inputs {
    fn insert(&mut self, segment: SegmentId) -> usize {
        // Narrow projections avoid a hash table; wide schemas avoid quadratic lookup work.
        if self.positions.is_none() && self.segments.len() == 16 {
            self.positions = Some(
                self.segments
                    .iter()
                    .enumerate()
                    .map(|(index, &id)| (id, index))
                    .collect(),
            );
        }
        if let Some(positions) = &mut self.positions {
            return *positions.entry(segment).or_insert_with(|| {
                let index = self.segments.len();
                self.segments.push(segment);
                index
            });
        }
        if let Some(index) = self.segments.iter().position(|&id| id == segment) {
            return index;
        }
        let index = self.segments.len();
        self.segments.push(segment);
        index
    }
}

enum Projection {
    Leaf {
        plan: SegmentScanPlan,
        row: usize,
        input: usize,
    },
    Struct {
        fields: StructFields,
        nullable: bool,
        children: Vec<Projection>,
    },
}

impl Projection {
    fn try_new(plan: &PlanRef, row: u64, inputs: &mut Inputs) -> VortexResult<Option<Self>> {
        vortex_ensure!(
            row < plan.row_count(),
            "Point projection row is out of bounds"
        );
        if let Some(scan) = plan.as_opt::<SegmentScan>() {
            let input = inputs.insert(scan.segment_id());
            return Ok(Some(Self::Leaf {
                plan: scan.clone(),
                row: usize::try_from(row)?,
                input,
            }));
        }
        if let Some(concat) = plan.as_opt::<Concat>() {
            let index = concat
                .row_offsets()
                .partition_point(|&offset| offset <= row)
                .checked_sub(1)
                .ok_or_else(|| vortex_err!("Concat has no child for point projection"))?;
            return Self::try_new(
                &concat.child_required(index)?,
                row - concat.row_offsets()[index],
                inputs,
            );
        }
        if let Some(filter) = plan.as_opt::<Filter>() {
            // Filter carries the incoming selection, which is all true for this one-row scope.
            return Self::try_new(&filter.child_plan()?, row, inputs);
        }
        if let Some(zoned) = plan.as_opt::<Zoned>() {
            return match zoned.data_plan()? {
                Some(data) => Self::try_new(&data, row, inputs),
                None => Ok(None),
            };
        }
        if let Some(stats) = plan.as_opt::<LegacyStats>() {
            return Self::try_new(&stats.data_plan()?, row, inputs);
        }
        if let Some(pack) = plan.as_opt::<Pack>() {
            let mut children = Vec::with_capacity(pack.children().len());
            for child in pack.children().iter_refs() {
                let Some(child) = Self::try_new(child?, row, inputs)? else {
                    return Ok(None);
                };
                children.push(child);
            }
            return Ok(Some(Self::Struct {
                fields: pack.fields().clone(),
                nullable: pack.dtype().is_nullable(),
                children,
            }));
        }
        Ok(None)
    }

    fn execute(
        self,
        inputs: &[Option<BufferHandle>],
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        match self {
            Self::Leaf { plan, row, input } => {
                let bytes = inputs[input]
                    .as_ref()
                    .ok_or_else(|| vortex_err!("Point projection ran before its read completed"))?;
                decode_segment(&plan, bytes.clone(), session)?.slice(row..row + 1)
            }
            Self::Struct {
                fields,
                nullable,
                children,
            } => {
                let mut arrays = children
                    .into_iter()
                    .map(|child| child.execute(inputs, session))
                    .collect::<VortexResult<Vec<_>>>()?;
                let validity =
                    if nullable {
                        Validity::Array(arrays.pop().ok_or_else(|| {
                            vortex_err!("Nullable point projection has no validity")
                        })?)
                    } else {
                        Validity::NonNullable
                    };
                Ok(assemble_struct(&fields, arrays, 1, validity).into_array())
            }
        }
    }
}

/// Reads the leaves needed by one row and assembles their projected values.
pub(super) struct PointProjectionMorsel {
    session: VortexSession,
    projection: Option<Projection>,
    requests: Option<IoBatch>,
    inputs: Vec<Option<BufferHandle>>,
    pending: usize,
}

impl PointProjectionMorsel {
    pub(super) fn try_new(plans: &ScanPlans, row: u64) -> VortexResult<Option<Self>> {
        let mut inputs = Inputs::default();
        let Some(projection) = Projection::try_new(&plans.projection, row, &mut inputs)? else {
            return Ok(None);
        };
        let requests = inputs
            .segments
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let location = plans
                    .locations
                    .get(**id as usize)
                    .ok_or_else(|| vortex_err!("Segment {id} has no known location"))?;
                Ok(IoRequest {
                    intent: IoIntent::Fetch,
                    request: IoRequestId(u32::try_from(index)?),
                    target: location.target(),
                })
            })
            .collect::<VortexResult<IoBatch>>()?;
        Ok(Some(Self {
            session: plans.session.clone(),
            projection: Some(projection),
            requests: Some(requests),
            inputs: vec![None; inputs.segments.len()],
            pending: inputs.segments.len(),
        }))
    }
}

impl IoConsumer for PointProjectionMorsel {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        let IoResult::Bytes(bytes) = result else {
            vortex_panic!("Point projection read answered with a size");
        };
        let input = self
            .inputs
            .get_mut(request.0 as usize)
            .vortex_expect("point projection request is outstanding");
        assert!(input.replace(bytes).is_none(), "point read delivered twice");
        self.pending -= 1;
    }
}

impl Morsel for PointProjectionMorsel {
    fn state(&self) -> State {
        if self.projection.is_none() {
            State::Done
        } else if self.requests.is_some() || self.pending == 0 {
            State::NeedsCompute
        } else {
            State::Waiting
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        if let Some(requests) = self.requests.take()
            && !requests.is_empty()
        {
            return Ok(MorselOutput::NeedsIO(requests));
        }
        let projection = self
            .projection
            .take()
            .ok_or_else(|| vortex_err!("Point projection computed after completion"))?;
        Ok(MorselOutput::Batch(
            projection.execute(&self.inputs, &self.session)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::dtype::DType;
    use vortex_array::dtype::PType;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::layouts::flat::FlatLayout;
    use crate::plan::physical::lower;

    #[test]
    fn accepts_the_selection_wrapper_of_a_lowered_flat_layout() -> VortexResult<()> {
        let id = SegmentId::from(0);
        let layout = FlatLayout::new(
            20,
            DType::Primitive(PType::I32, false.into()),
            id,
            ReadContext::new(Vec::new()),
        )
        .into_layout();
        let plan = lower(&layout)?;
        for row in [0, 19] {
            let mut inputs = Inputs::default();
            assert!(Projection::try_new(&plan, row, &mut inputs)?.is_some());
            assert_eq!(inputs.segments, vec![id]);
        }
        assert!(Projection::try_new(&plan, 20, &mut Inputs::default()).is_err());
        Ok(())
    }
}
