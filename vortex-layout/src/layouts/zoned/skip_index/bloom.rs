// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Skip index implementation for the Bloom filter.

use std::sync::Arc;

use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::scalar_fn::ScalarFnPluginRef;
use vortex_array::stats::rewrite::StatsRewriteRuleRef;

use crate::layouts::zoned::aggregates::bloom_filter::BloomFilter;
use crate::layouts::zoned::aggregates::bloom_filter::BloomOptions;
use crate::layouts::zoned::aggregates::bloom_filter::scalar_fn::BloomContains;
use crate::layouts::zoned::aggregates::bloom_filter::scalar_fn::BloomEqRewrite;
use crate::layouts::zoned::skip_index::SkipIndex;

/// An implementation of a skip index for the [`BloomFilter`] aggregate.
///
/// Instances carry the options used when writing the index. Register Bloom
/// support with a session using
/// `session.register_skip_index(&index)`, where `index` is a [`BloomSkipIndex`].
///
/// # Writing
///
/// Bloom skip indexes are not currently included in any Vortex edition. When
/// writing a file with this index, disable edition checks using
/// `WriteOptions::disable_editions`.
///
/// For more information about how the index works, see [`BloomFilter`].
#[derive(Clone, Debug, Default)]
pub struct BloomSkipIndex {
    options: BloomOptions,
}

impl BloomSkipIndex {
    /// Create an index with explicit Bloom tuning.
    pub fn new(options: BloomOptions) -> Self {
        Self { options }
    }
}

impl SkipIndex for BloomSkipIndex {
    type Aggregate = BloomFilter;

    fn aggregate_vtable(&self) -> Self::Aggregate {
        BloomFilter
    }

    fn scalar_plugin(&self) -> Option<ScalarFnPluginRef> {
        Some(Arc::new(BloomContains))
    }

    fn options(&self) -> <Self::Aggregate as AggregateFnVTable>::Options {
        self.options.clone()
    }

    fn rewrite_rules(&self) -> Vec<StatsRewriteRuleRef> {
        vec![Arc::new(BloomEqRewrite)]
    }
}
