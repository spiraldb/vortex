// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Skipping-index interface and its implementations.
//!
//! This module also provides the session extension used to register skip index
//! implementations. Pass the bound aggregate returned by [`SkipIndex::aggregate_fn`]
//! to `WriteStrategyBuilder::with_field_aggregate_additions` to index a field while retaining defaults.
//!
//! # Difference from a locating index
//!
//! Unlike a locating index, a skip index summarizes a zone. It does not locate
//! matching rows. It can only prove that a zone cannot match a predicate.

use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::AggregateFnVTableExt;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::scalar_fn::ScalarFnPluginRef;
use vortex_array::scalar_fn::session::ScalarFnSessionExt;
use vortex_array::stats::StatsSessionExt;
use vortex_array::stats::rewrite::StatsRewriteRuleRef;
use vortex_session::SessionExt;

pub mod bloom;

/// A configured skip index used when writing zoned layouts.
///
/// Skip indexes rely on an aggregate function to summarize each zone. Vortex
/// builds these summaries during writes and uses them during reads to prove
/// that a zone cannot match a predicate and prune it.
///
/// # Usage
///
/// First, register the components needed to use the index through
/// [`SkipIndexSessionExt::register_skip_index`]. When writing, use
/// [`SkipIndex::aggregate_fn`] to bind the index options and pass the aggregate to
/// `WriteStrategyBuilder::with_field_aggregate_additions` for the field to be indexed.
/// Use `WriteStrategyBuilder::with_field_aggregates` to replace the defaults instead. Zone length
/// is controlled by `WriteStrategyBuilder::with_row_block_size`.
///
/// # Logical and physical representation
///
/// [`SkipIndex`] defines the logical skip index abstraction. A skip index has no identifier
/// or serialized representation of its own. Instead, it is represented in a Vortex file by
/// the serialized [`AggregateFnRef`] it provides.
///
/// # Examples
///
/// Register the skip index implementation with the session before reading or
/// writing:
///
/// ```
/// use vortex_layout::layouts::zoned::skip_index::SkipIndexSessionExt;
/// use vortex_layout::layouts::zoned::skip_index::bloom::BloomSkipIndex;
/// use vortex_session::VortexSession;
///
/// fn register_index(session: &VortexSession) {
///     let index = BloomSkipIndex::default();
///     session.register_skip_index(&index);
/// }
/// ```
///
/// For writes, create a configured index and use [`SkipIndex::aggregate_fn`] to
/// obtain its aggregate. Pass the field path and an aggregate list containing it to
/// `WriteStrategyBuilder::with_field_aggregate_additions`. This retains the default
/// aggregates for that field.
pub trait SkipIndex: Send + Sync + 'static {
    /// The concrete aggregate implementation registered for this index.
    type Aggregate: AggregateFnVTable;

    /// Returns the aggregate implementation, without binding write options.
    fn aggregate_vtable(&self) -> Self::Aggregate;

    /// Returns a custom probe implementation, if the rewrite needs one.
    ///
    /// Indexes whose proofs use only built-in scalar functions can leave this unset.
    fn scalar_plugin(&self) -> Option<ScalarFnPluginRef> {
        None
    }

    /// Returns the options to bind when writing this index.
    fn options(&self) -> <Self::Aggregate as AggregateFnVTable>::Options;

    /// Returns the rules that turn query predicates into proofs over the summary.
    fn rewrite_rules(&self) -> Vec<StatsRewriteRuleRef>;

    /// Binds this index's write options into an aggregate for the zoned writer.
    ///
    /// Binding does not register components or check input dtype compatibility. The zoned writer
    /// rejects explicitly requested aggregates that do not support its input dtype.
    fn aggregate_fn(&self) -> AggregateFnRef {
        self.aggregate_vtable().bind(self.options())
    }
}

/// Extension trait for registering skipping indexes with a Vortex session.
pub trait SkipIndexSessionExt: SessionExt {
    /// Registers the aggregate, probe scalar functions, and rewrite rules
    /// supplied by a skip index.
    ///
    /// Repeated calls replace this implementation's components and rewrite group. Registration
    /// must support every persisted configuration and must not depend on this instance's options.
    ///
    /// For more information about skip indexes, see [`SkipIndex`].
    fn register_skip_index<I: SkipIndex>(&self, index: &I) {
        let session = self.session();
        session.aggregate_fns().register(index.aggregate_vtable());
        if let Some(scalar) = index.scalar_plugin() {
            session.scalar_fns().registry().insert(scalar.id(), scalar);
        }
        session
            .stats()
            .register_rewrite_group::<I>(index.rewrite_rules());
    }
}

impl<S: SessionExt> SkipIndexSessionExt for S {}

#[cfg(test)]
mod tests;
