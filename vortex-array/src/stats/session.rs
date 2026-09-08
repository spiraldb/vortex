// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Session state for stats APIs.

use std::any::Any;
use std::sync::Arc;

use parking_lot::RwLock;
use vortex_session::ArcSwapMap;
use vortex_session::SessionExt;
use vortex_session::SessionGuard;
use vortex_session::SessionVar;
use vortex_utils::aliases::hash_map::HashMap;

use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::AggregateFnRef;
use crate::dtype::DType;
use crate::scalar_fn::ScalarFnId;
use crate::stats::rewrite::StatsRewriteRule;
use crate::stats::rewrite::StatsRewriteRuleRef;
use crate::stats::rewrite::register_builtins;

type StatsRewriteRuleSet = Arc<[StatsRewriteRuleRef]>;

/// Session state for stats APIs.
#[derive(Clone, Debug)]
pub struct StatsSession {
    zone_stat_defaults: ArcSwapMap<AggregateFnId, AggregateFnRef>,
    rewrite_rules: Arc<RwLock<HashMap<ScalarFnId, StatsRewriteRuleSet>>>,
}

impl Default for StatsSession {
    fn default() -> Self {
        let this = Self {
            zone_stat_defaults: ArcSwapMap::default(),
            rewrite_rules: Arc::new(RwLock::new(HashMap::default())),
        };
        register_builtins(&this);
        this
    }
}

impl StatsSession {
    /// Register a bound aggregate as a default zone statistic for its supported input dtypes.
    ///
    /// Replaces any default with the same aggregate ID. Register the aggregate vtable separately
    /// in the aggregate function session so readers can deserialize the stored statistic.
    pub fn register_zone_stat_default(&self, aggregate_fn: AggregateFnRef) {
        self.zone_stat_defaults
            .insert(aggregate_fn.id(), aggregate_fn);
    }

    /// Return registered zone defaults that support `input_dtype`, ordered by aggregate ID.
    ///
    /// The zoned writer adds these to its built-in defaults once per column.
    pub fn zone_stat_defaults(&self, input_dtype: &DType) -> Vec<AggregateFnRef> {
        self.zone_stat_defaults.read(|defaults| {
            let mut fns: Vec<_> = defaults
                .values()
                .filter(|aggregate_fn| aggregate_fn.return_dtype(input_dtype).is_some())
                .cloned()
                .collect();
            fns.sort_by_key(|aggregate_fn| aggregate_fn.id());
            fns
        })
    }

    /// Register a stats rewrite rule.
    pub fn register_rewrite<R: StatsRewriteRule>(&self, rule: R) {
        self.register_rewrite_ref(Arc::new(rule));
    }

    /// Register a shared stats rewrite rule.
    pub fn register_rewrite_ref(&self, rule: StatsRewriteRuleRef) {
        let mut rules = self.rewrite_rules.write();
        let rule_id = rule.scalar_fn_id();
        let mut updated_rules = rules
            .get(&rule_id)
            .map(|rules| rules.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        updated_rules.push(rule);
        rules.insert(rule_id, updated_rules.into());
    }

    /// Return the rewrite rules registered for `scalar_fn_id`.
    pub(crate) fn rewrite_rules_for(
        &self,
        scalar_fn_id: ScalarFnId,
    ) -> Option<StatsRewriteRuleSet> {
        self.rewrite_rules.read().get(&scalar_fn_id).cloned()
    }
}

impl SessionVar for StatsSession {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Extension trait for accessing stats session data.
pub trait StatsSessionExt: SessionExt {
    /// Returns the stats session state.
    fn stats(&self) -> SessionGuard<'_, StatsSession> {
        self.get::<StatsSession>()
    }
}
impl<S: SessionExt> StatsSessionExt for S {}

#[cfg(test)]
mod tests {
    use crate::aggregate_fn::AggregateFnVTableExt;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::min::Min;
    use crate::aggregate_fn::fns::sum::Sum;
    use crate::aggregate_fn::session::AggregateFnSessionExt;
    use crate::dtype::DType;
    use crate::dtype::Nullability::NonNullable;
    use crate::dtype::PType;
    use crate::stats::session::StatsSession;
    use crate::stats::session::StatsSessionExt;

    #[test]
    fn aggregate_registration_does_not_opt_in_to_zone_statistics() {
        let session = crate::array_session();
        session.aggregate_fns().register(Min);
        assert!(
            session
                .stats()
                .zone_stat_defaults(&DType::Primitive(PType::I32, NonNullable))
                .is_empty()
        );
    }

    #[test]
    fn zone_defaults_filter_dtypes_and_replace_options() {
        let stats = StatsSession::default();
        let sum = Sum.bind(NumericalAggregateOpts::skip_nans());
        stats.register_zone_stat_default(sum.clone());
        assert!(
            stats
                .zone_stat_defaults(&DType::Utf8(NonNullable))
                .is_empty()
        );
        assert_eq!(
            stats.zone_stat_defaults(&DType::Primitive(PType::I32, NonNullable)),
            vec![sum]
        );

        stats.register_zone_stat_default(Sum.bind(NumericalAggregateOpts::skip_nans()));
        let replacement = Sum.bind(NumericalAggregateOpts::include_nans());
        stats.register_zone_stat_default(replacement.clone());
        stats.register_zone_stat_default(replacement.clone());
        assert_eq!(
            stats.zone_stat_defaults(&DType::Primitive(PType::F64, NonNullable)),
            vec![replacement]
        );
    }
}
