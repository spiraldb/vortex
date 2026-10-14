// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::arrays::filter::FilterReduceAdaptor;
use vortex_array::arrays::slice::SliceReduceAdaptor;
use vortex_array::optimizer::rules::ParentRuleSet;

use crate::Patched;

pub(crate) const PARENT_RULES: ParentRuleSet<Patched> = ParentRuleSet::new(&[
    ParentRuleSet::lift(&FilterReduceAdaptor(Patched)),
    ParentRuleSet::lift(&SliceReduceAdaptor(Patched)),
]);
