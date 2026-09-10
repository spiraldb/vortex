// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;

/// Struct is almost nothing: identity edges to each field, then a zip.
///
/// Every field shares the same row domain and is pulled under the same hint, and every field comes back dense
/// over the range (or filtered by the same mask), so the zip is a zip.
pub struct StructExec {
    names: FieldNames,
    fields: Vec<Child>,
    validity: Option<Child>,

    domain: RowDomain,
    state: State,
}

enum State {
    Pulling {
        validity: Option<ArrayRef>,
        validity_done: bool,
        field: usize,
        fields: Vec<ArrayRef>,
    },
    Emitted,
}

impl StructExec {
    /// Build a struct operator over one child per projected field.
    pub fn new(
        names: FieldNames,
        fields: Vec<Child>,
        validity: Option<Child>,
        domain: RowDomain,
    ) -> Self {
        debug_assert_eq!(names.len(), fields.len());
        let state = State::Pulling {
            validity: None,
            validity_done: validity.is_none(),
            field: 0,
            fields: Vec::with_capacity(fields.len()),
        };
        Self {
            names,
            fields,
            validity,
            domain,
            state,
        }
    }
}

impl Operator for StructExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        let mut result = LookAhead::Complete;
        for child in self.fields.iter_mut().chain(self.validity.iter_mut()) {
            result = result.merge(child.look_ahead(cx)?);
        }
        Ok(result)
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        match &mut self.state {
            State::Emitted => Ok(Step::Finished),
            State::Pulling {
                validity,
                validity_done,
                field,
                fields,
            } => {
                if !*validity_done {
                    let child = self.validity.as_mut().vortex_expect("validity child");
                    match child.next(hint, cx)? {
                        Step::Batch(batch) => {
                            *validity = Some(batch.value.into_array()?);
                            *validity_done = true;
                        }
                        Step::Blocked => return Ok(Step::Blocked),
                        Step::Finished => {
                            return Err(vortex_err!("struct validity child produced no value"));
                        }
                    }
                }
                while *field < self.fields.len() {
                    match self.fields[*field].next(hint, cx)? {
                        Step::Batch(batch) => {
                            fields.push(batch.value.into_array()?);
                            *field += 1;
                        }
                        Step::Blocked => return Ok(Step::Blocked),
                        Step::Finished => {
                            return Err(vortex_err!(
                                "struct child {} produced no value",
                                self.fields[*field].id()
                            ));
                        }
                    }
                }

                // Below a filter the children hold only the selected rows, so the length is theirs.
                let fields = std::mem::take(fields);
                let validity_array = validity.take();
                let len = fields
                    .first()
                    .map(|field| field.len())
                    .or_else(|| validity_array.as_ref().map(|validity| validity.len()))
                    .unwrap_or(hint.len());
                let validity = validity_array.map_or(Validity::NonNullable, Validity::Array);
                let array =
                    StructArray::try_new(self.names.clone(), fields, len, validity)?.into_array();
                self.state = State::Emitted;
                Ok(Step::Batch(Batch::array(
                    self.domain.range().clone(),
                    array,
                )))
            }
        }
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        for field in &mut self.fields {
            field.close(cx);
        }
        if let Some(validity) = &mut self.validity {
            validity.close(cx);
        }
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        let fields: Vec<String> = self
            .names
            .iter()
            .zip(self.fields.iter())
            .map(|(name, child)| format!("{name}: node {}", child.id()))
            .collect();
        format!("Struct{{{}}}", fields.join(", "))
    }
}
