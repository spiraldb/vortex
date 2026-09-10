// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::DictArray;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::plan::ExactPlan;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;

/// A dictionary values array paired with range-scoped codes.
///
/// Look-ahead covers the whole values domain, unless this scan already decoded them, and the codes follow
/// under the hint. Every morsel uses the same values plan identity to share decoded values
/// through the scan-wide cache.
pub struct DictExec {
    values: Child,
    codes: Child,
    values_len: usize,
    /// Identity of the values plan, shared by every morsel using this dictionary.
    values_key: ExactPlan,

    domain: RowDomain,
    state: State,
}

enum State {
    Pulling { values: Option<ArrayRef> },
    Emitted,
}

impl DictExec {
    /// Build a dictionary operator from its full values child and row-aligned codes child.
    pub fn new(
        values_key: ExactPlan,
        values: Child,
        codes: Child,
        values_len: usize,
        domain: RowDomain,
    ) -> Self {
        Self {
            values,
            codes,
            values_len,
            values_key,
            domain,
            state: State::Pulling { values: None },
        }
    }
}

impl Operator for DictExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        let values = if cx.dictionary(&self.values_key).is_none() {
            self.values.look_ahead(cx)?
        } else {
            LookAhead::Complete
        };
        Ok(values.merge(self.codes.look_ahead(cx)?))
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        match &mut self.state {
            State::Emitted => Ok(Step::Finished),
            State::Pulling { values } => {
                if values.is_none() {
                    // Another morsel may have published the values since this morsel's look-ahead.
                    *values = Some(match cx.dictionary(&self.values_key) {
                        Some(shared) => shared,
                        None => match self.values.next(&Mask::new_true(self.values_len), cx)? {
                            Step::Batch(batch) => cx.publish_dictionary(
                                self.values_key.clone(),
                                batch.value.into_array()?,
                            ),
                            Step::Blocked => return Ok(Step::Blocked),
                            Step::Finished => {
                                return Err(vortex_err!("dictionary values produced no value"));
                            }
                        },
                    });
                }
                let codes = match self.codes.next(hint, cx)? {
                    Step::Batch(batch) => batch.value.into_array()?,
                    Step::Blocked => return Ok(Step::Blocked),
                    Step::Finished => {
                        return Err(vortex_err!("dictionary codes produced no value"));
                    }
                };
                let values = values.take().vortex_expect("values resolved above");
                let array = DictArray::try_new(codes, values)?.into_array();
                self.state = State::Emitted;
                Ok(Step::Batch(Batch::array(
                    self.domain.range().clone(),
                    array,
                )))
            }
        }
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        // A cache hit skips look-ahead on the values, but their pre-counted leases still belong to
        // this morsel and must be released.
        self.values.close(cx);
        self.codes.close(cx);
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        format!(
            "Dict(values node {} of {} values, codes node {})",
            self.values.id(),
            self.values_len,
            self.codes.id()
        )
    }
}
