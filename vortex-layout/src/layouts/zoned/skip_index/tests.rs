// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::num::NonZeroU32;
use std::thread;

use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::expr::bound::eq;
use vortex_array::expr::bound::lit;
use vortex_array::expr::bound::root;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_array::scalar_fn::session::ScalarFnSessionExt;
use vortex_array::stats::StatsSession;
use vortex_array::stats::StatsSessionExt;
use vortex_array::stats::rewrite::StatsRewriteCtx;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::layouts::zoned::aggregates::bloom_filter::BloomFilter;
use crate::layouts::zoned::aggregates::bloom_filter::BloomOptions;
use crate::layouts::zoned::aggregates::bloom_filter::HashFn;
use crate::layouts::zoned::aggregates::bloom_filter::scalar_fn::BloomContains;
use crate::layouts::zoned::aggregates::bloom_filter::scalar_fn::BloomEqRewrite;
use crate::layouts::zoned::skip_index::SkipIndex;
use crate::layouts::zoned::skip_index::SkipIndexSessionExt;
use crate::layouts::zoned::skip_index::bloom::BloomSkipIndex;

fn bloom_probe_options(
    session: &VortexSession,
    index: &BloomSkipIndex,
) -> VortexResult<Vec<BloomOptions>> {
    let aggregate_fns = [index.aggregate_fn()];
    let predicate = eq(
        root(DType::Primitive(PType::I64, Nullability::NonNullable)),
        lit(42i64),
    );
    let proof = StatsRewriteCtx::new(session)
        .with_aggregate_fns(&aggregate_fns)
        .falsify(&predicate)?
        .ok_or_else(|| vortex_err!("expected an equality falsifier"))?;
    let mut pending = vec![&proof];
    let mut options = Vec::new();
    while let Some(expr) = pending.pop() {
        if let Some(probe_options) = expr.as_opt::<BloomContains>() {
            options.push(probe_options.clone());
        }
        pending.extend(expr.children());
    }
    Ok(options)
}

#[test]
fn registration_after_aggregate_registration_installs_components() -> VortexResult<()> {
    let session = vortex_array::array_session();
    let index = BloomSkipIndex::default();
    session.aggregate_fns().register(BloomFilter);
    session.register_skip_index(&index);

    assert!(
        session
            .scalar_fns()
            .registry()
            .get(&BloomContains.id())
            .is_some()
    );
    assert_eq!(bloom_probe_options(&session, &index)?, [index.options()]);
    Ok(())
}

#[test]
#[expect(
    clippy::redundant_clone,
    reason = "Registration must be shared across session clones."
)]
fn repeated_registration_installs_one_bloom_rewrite() -> VortexResult<()> {
    let session = vortex_array::array_session();
    let index = BloomSkipIndex::default();
    session.register_skip_index(&index);
    session.clone().register_skip_index(&index);

    assert_eq!(bloom_probe_options(&session, &index)?, [index.options()]);
    Ok(())
}

#[test]
fn concurrent_registration_installs_one_bloom_rewrite() -> VortexResult<()> {
    let session = vortex_array::array_session();
    let index = BloomSkipIndex::default();
    thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| session.register_skip_index(&index));
        }
    });

    assert_eq!(bloom_probe_options(&session, &index)?, [index.options()]);
    Ok(())
}

#[test]
fn registration_uses_persisted_options() -> VortexResult<()> {
    let session = vortex_array::array_session();
    session.register_skip_index(&BloomSkipIndex::default());
    let index = BloomSkipIndex::new(BloomOptions::new(
        NonZeroU32::new(8).ok_or_else(|| vortex_err!("block count must be nonzero"))?,
        HashFn::XxHash3_64,
    ));

    assert_eq!(bloom_probe_options(&session, &index)?, [index.options()]);
    Ok(())
}

#[test]
fn registration_after_stats_replacement_restores_rewrite() -> VortexResult<()> {
    let session = vortex_array::array_session();
    let index = BloomSkipIndex::default();
    session.register_skip_index(&index);
    session.register(StatsSession::default());
    session.register_skip_index(&index);

    assert_eq!(bloom_probe_options(&session, &index)?, [index.options()]);
    Ok(())
}

#[test]
fn registration_preserves_independently_registered_rule() -> VortexResult<()> {
    let session = vortex_array::array_session();
    let index = BloomSkipIndex::default();
    session.stats().register_rewrite(BloomEqRewrite);
    session.register_skip_index(&index);
    session.register_skip_index(&index);

    assert_eq!(
        bloom_probe_options(&session, &index)?,
        [index.options(), index.options()]
    );
    Ok(())
}
