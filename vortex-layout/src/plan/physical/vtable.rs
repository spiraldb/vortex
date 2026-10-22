// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;

use vortex_array::DeserializeMetadata;
use vortex_array::SerializeMetadata;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_mask::Mask;
use vortex_session::registry::Id;

use crate::plan::physical::ChildRows;
use crate::plan::physical::PlanChildren;
use crate::plan::physical::pipeline::Chain;
use crate::plan::physical::pipeline::Compiler;
use crate::plan::physical::typed::Plan;

/// A unique identifier for a plan operator.
pub type PlanId = Id;

/// Operator-specific behavior for a typed [`Plan`].
///
/// Common fields — dtype, row count, and children — are stored outside the erased operator data.
/// Implementations own only their operator-specific data, its metadata codec, and a callback for
/// refreshing cached data after generic child replacement.
///
/// Operators describe physical work over a row domain. Their identity and operator-specific data
/// do not depend on the source layout kind. The common lazy-child storage may nevertheless own a
/// hidden source-layout reference used for on-demand lowering.
pub trait PlanVTable: 'static + Clone + Sized + Send + Sync + Debug {
    /// Operator-specific data, excluding children.
    ///
    /// Children belong in [`PlanParts::children`](crate::plan::physical::PlanParts::children) so that
    /// traversal and rewriting can discover them generically.
    type PlanData: 'static + Send + Sync + Clone + Debug;

    /// Serialized form of [`PlanData`](Self::PlanData).
    type Metadata: SerializeMetadata + DeserializeMetadata + Debug;

    /// Returns the globally unique operator ID.
    fn id(&self) -> PlanId;

    /// Writes operator-specific fields after the plan's standard display summary.
    fn fmt(plan: &Plan<Self>, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = (plan, formatter);
        Ok(())
    }

    /// Returns the serializable metadata for this operator.
    ///
    /// Returns `None` when the operator holds state that cannot be serialized.
    fn metadata(plan: &Plan<Self>) -> Option<Self::Metadata>;

    /// Refreshes cloned operator data after the common child container is replaced.
    ///
    /// The plan layer clones [`PlanData`](Self::PlanData), replaces the children externally, and
    /// invokes this callback. Implementations validate the new children and update any derived
    /// values in `data`; they do not rebuild the plan itself.
    fn with_children(
        plan: &Plan<Self>,
        children: &PlanChildren,
        data: &mut Self::PlanData,
    ) -> VortexResult<()> {
        let _ = (plan, children, data);
        Ok(())
    }

    /// Returns the display name of the child at `index`.
    fn child_name(plan: &Plan<Self>, index: usize) -> Cow<'_, str> {
        let _ = plan;
        Cow::Owned(format!("child[{index}]"))
    }

    /// Compiles this plan over `rows` of its row domain, restricted to `mask`, into a chain:
    /// a source and the stages after it, producing the rows `mask` selects in row order, or
    /// `None` when that is no row. `rows` lies within the plan's row domain and `mask` is as long
    /// as `rows`.
    ///
    /// A plan compiles its children through `compiler` and either adds stages to a child's
    /// chain or joins several children's chains with a source of its own. Compiling does no IO.
    fn compile(
        plan: &Plan<Self>,
        rows: Range<u64>,
        mask: &Mask,
        compiler: &mut Compiler<'_>,
    ) -> VortexResult<Option<Chain>> {
        let _ = (rows, mask, compiler);
        vortex_bail!("Plan {} has no pipeline implementation", plan.vtable().id())
    }

    /// How the rows of the child at `index` relate to this plan's rows, so the segments a plan
    /// reads over some rows can be found without compiling it. See
    /// [`visit_segments`](crate::plan::physical::visit_segments).
    ///
    /// Every child of a plan that compiles is read for some of its parent's rows or whole. A
    /// child the optimizer removes before any walk, such as the zone table of a zoned data plan,
    /// is an error to ask about, as is any child of a plan with no pipeline implementation.
    fn child_rows(plan: &Plan<Self>, index: usize) -> VortexResult<ChildRows> {
        let _ = index;
        vortex_bail!("Plan {} has no segment walk", plan.vtable().id())
    }
}
