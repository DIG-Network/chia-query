//! The provider registry and its two views: the fail-closed **custody** view
//! ([`ProviderRegistry::trusted`]) and the low-trust **discovery** view
//! ([`ProviderRegistry::any`]).
//!
//! The registry composes arbitrary [`ChainSourceProvider`]s (a local node, coinset.org, DIG peers,
//! an operator override) under an operator-assigned [`TrustLevel`]. Its custody view answers a read
//! ONLY from an operator-trusted source or a qualifying quorum, and FAILS CLOSED otherwise — a
//! provider's self-declared `trustless` flag is advisory and never grants custody trust (SPEC §5).

use chia_protocol::{Bytes32, CoinSpend};
use dig_chainsource_interface::{
    ChainSource, ChainSourceError, ChainSourceProvider, CoinRecord, ProviderKind, SingletonLineage,
};

/// The trust an OPERATOR assigns a provider for custody (money-routing) reads. This is the
/// authority the custody view relies on — distinct from a provider's advisory `trustless` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustLevel {
    /// The operator vouches for this source for custody reads (their own verified node).
    Trusted,
    /// Not vouched for custody; usable only within a qualifying quorum or for discovery reads.
    Untrusted,
}

impl TrustLevel {
    /// The default trust for a provider kind: a local node the operator runs is `Trusted`; every
    /// public/shared source is `Untrusted` until the operator explicitly vouches for it.
    pub fn default_for(kind: ProviderKind) -> Self {
        match kind {
            ProviderKind::LocalNode => Self::Trusted,
            ProviderKind::PublicOracle | ProviderKind::DigPeers | ProviderKind::Custom => {
                Self::Untrusted
            }
        }
    }
}

/// The number of independent groups that must agree for a pure-public quorum to satisfy custody.
const PUBLIC_QUORUM_THRESHOLD: usize = 2;

/// A boxed, dynamically-dispatched registry participant.
type DynProvider = dyn ChainSourceProvider<Error = ChainSourceError>;

/// One registered provider: the source itself, its operator-assigned trust, and the independence
/// group it belongs to (two providers in the same group are NOT independent).
struct Registration {
    provider: Box<DynProvider>,
    trust: TrustLevel,
    independence_group: String,
}

/// Composes [`ChainSourceProvider`]s under an operator trust model, exposing a fail-closed custody
/// view and a low-trust discovery view.
#[derive(Default)]
pub struct ProviderRegistry {
    providers: Vec<Registration>,
    allow_public_quorum_custody: bool,
}

impl ProviderRegistry {
    /// A new, empty registry (custody fails closed until a trusted or quorum-qualifying source is
    /// registered).
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables (`true`) or disables (`false`, the safe default) pure-public-quorum custody.
    ///
    /// With this OFF, a registry holding only public/untrusted sources cannot satisfy custody —
    /// every custody read fails closed. Turning it ON is a deliberate operator choice that accepts
    /// the reduced assurance of a [`PUBLIC_QUORUM_THRESHOLD`]-of-independent-groups agreement in
    /// place of an operator-trusted source (SPEC §5).
    pub fn allow_public_quorum_custody(mut self, allow: bool) -> Self {
        self.allow_public_quorum_custody = allow;
        if allow {
            log::warn!(
                "chia-query registry: pure-public-quorum custody ENABLED — custody reads may be \
                 satisfied by {PUBLIC_QUORUM_THRESHOLD} independent public sources with NO \
                 operator-trusted source. Reduced assurance vs a trusted local node."
            );
        }
        self
    }

    /// Registers a provider, defaulting its trust from its [`ProviderKind`] unless the operator
    /// overrides it, and placing it in `independence_group` (sources that could fail or lie
    /// together — e.g. two views of the same coinset.org — share a group id).
    pub fn register(
        mut self,
        provider: Box<DynProvider>,
        trust_override: Option<TrustLevel>,
        independence_group: impl Into<String>,
    ) -> Self {
        let trust = trust_override
            .unwrap_or_else(|| TrustLevel::default_for(provider.provider_info().kind));
        self.providers.push(Registration {
            provider,
            trust,
            independence_group: independence_group.into(),
        });
        self
    }

    /// The CUSTODY view — sound for money-routing reads.
    ///
    /// A read is satisfied ONLY by an operator-[`Trusted`](TrustLevel::Trusted) source (preferred
    /// when present) or, when `allow_public_quorum_custody` is set, a quorum of
    /// [`PUBLIC_QUORUM_THRESHOLD`] independent public sources that agree. Otherwise it fails closed.
    pub fn trusted(&self) -> TrustedView<'_> {
        TrustedView { registry: self }
    }

    /// The DISCOVERY view — low-trust, single-provider reads for NON-custody use.
    ///
    /// It returns the first provider that answers, with NO trust or quorum guarantee. Its result
    /// MUST NOT be used to route funds; use [`trusted`](Self::trusted) for anything custody-bearing.
    pub fn any(&self) -> DiscoveryView<'_> {
        DiscoveryView { registry: self }
    }

    /// The registered providers sorted by ascending `priority` (lowest tried first).
    fn by_priority(&self) -> Vec<&Registration> {
        let mut ordered: Vec<&Registration> = self.providers.iter().collect();
        ordered.sort_by_key(|reg| reg.provider.provider_info().priority);
        ordered
    }
}

/// The custody view over a [`ProviderRegistry`] — see [`ProviderRegistry::trusted`].
pub struct TrustedView<'a> {
    registry: &'a ProviderRegistry,
}

impl TrustedView<'_> {
    /// Answers `query` under the custody trust rule, failing closed when no trusted source or
    /// qualifying quorum can answer.
    fn custody_read<T, Q>(&self, query: Q) -> Result<T, ChainSourceError>
    where
        T: QuorumComparable + Clone,
        Q: Fn(&DynProvider) -> Result<T, ChainSourceError>,
    {
        let trusted: Vec<&Registration> = self
            .registry
            .by_priority()
            .into_iter()
            .filter(|reg| reg.trust == TrustLevel::Trusted)
            .collect();

        // (a) An operator-trusted source is the gold standard — always preferred when present.
        if !trusted.is_empty() {
            let mut last_error = ChainSourceError::NoProvider;
            for reg in trusted {
                match query(&*reg.provider) {
                    Ok(value) => return Ok(value),
                    Err(error) => last_error = error,
                }
            }
            // Every trusted source failed to answer — fail closed, never degrade to a public source.
            return Err(last_error);
        }

        // (b) No trusted source. A pure-public quorum only qualifies when explicitly opted in.
        if !self.registry.allow_public_quorum_custody {
            return Err(ChainSourceError::NoProvider);
        }
        quorum_read(&self.registry.providers, PUBLIC_QUORUM_THRESHOLD, query)
    }
}

/// The discovery view over a [`ProviderRegistry`] — see [`ProviderRegistry::any`].
pub struct DiscoveryView<'a> {
    registry: &'a ProviderRegistry,
}

impl DiscoveryView<'_> {
    /// Answers `query` from the first provider that responds — NO trust or quorum guarantee.
    fn discovery_read<T, Q>(&self, query: Q) -> Result<T, ChainSourceError>
    where
        Q: Fn(&DynProvider) -> Result<T, ChainSourceError>,
    {
        let mut last_error = ChainSourceError::NoProvider;
        for reg in self.registry.by_priority() {
            match query(&*reg.provider) {
                Ok(value) => return Ok(value),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }
}

/// Order-insensitive equality for quorum agreement.
///
/// Two honest, independent sources may return the SAME records in a DIFFERENT order — the list
/// reads ([`ChainSource::coin_records_by_puzzle_hash`], [`ChainSource::coin_records_by_parent`])
/// promise a set of matching coins, not an ordering. A byte-for-byte `Vec` comparison would make
/// such sources spuriously "disagree" and fail an otherwise-satisfiable quorum (an availability
/// nit). `quorum_eq` compares answers by VALUE independent of incidental ordering, while still
/// treating genuinely different record SETS as disagreement — the quorum security property (SPEC §5)
/// is preserved; only the false-negative on ordering is removed.
trait QuorumComparable {
    /// Whether two source answers agree for the purpose of a quorum, ignoring incidental ordering.
    fn quorum_eq(&self, other: &Self) -> bool;
}

/// Answers with no meaningful internal ordering agree exactly when they are equal.
macro_rules! quorum_eq_via_partial_eq {
    ($($t:ty),+ $(,)?) => {
        $(impl QuorumComparable for $t {
            fn quorum_eq(&self, other: &Self) -> bool {
                self == other
            }
        })+
    };
}

quorum_eq_via_partial_eq!(
    Option<CoinRecord>,
    Option<CoinSpend>,
    Option<SingletonLineage>,
    Option<u32>,
    Option<u64>,
);

impl QuorumComparable for Vec<CoinRecord> {
    fn quorum_eq(&self, other: &Self) -> bool {
        canonical_order(self) == canonical_order(other)
    }
}

/// Borrows the records in a canonical order (by coin id) so two equal SETS returned in different
/// orders compare equal. A coin id is a SHA-256 commitment to the whole coin, so it totally orders
/// distinct coins; records sharing a coin but differing in metadata still differ after the sort, so
/// genuine disagreement is preserved.
fn canonical_order(records: &[CoinRecord]) -> Vec<&CoinRecord> {
    let mut ordered: Vec<&CoinRecord> = records.iter().collect();
    ordered.sort_by(|a, b| a.coin.coin_id().as_ref().cmp(b.coin.coin_id().as_ref()));
    ordered
}

/// Requires `threshold` DISTINCT independence groups to return the SAME answer for `query`.
///
/// Each group contributes at most one answer (its first provider that responds); the count is over
/// distinct groups, so two providers in the same group can never satisfy a `threshold >= 2` on
/// their own. Agreement is order-insensitive ([`QuorumComparable`]), so two honest sources returning
/// the same record set in a different order still agree. Insufficient agreement — disagreement, too
/// few groups, or all errors — fails closed (SPEC §5).
fn quorum_read<T, Q>(
    providers: &[Registration],
    threshold: usize,
    query: Q,
) -> Result<T, ChainSourceError>
where
    T: QuorumComparable + Clone,
    Q: Fn(&DynProvider) -> Result<T, ChainSourceError>,
{
    // One representative answer per independence group (the first provider in the group to answer).
    let mut per_group: Vec<(&str, T)> = Vec::new();
    for reg in providers {
        let group = reg.independence_group.as_str();
        if per_group.iter().any(|(existing, _)| *existing == group) {
            continue; // this group already contributed a representative answer
        }
        if let Ok(answer) = query(&*reg.provider) {
            per_group.push((group, answer));
        }
    }

    // An answer qualifies when `threshold` distinct groups agree on it (order-insensitively).
    for (_, candidate) in &per_group {
        let agreeing = per_group
            .iter()
            .filter(|(_, a)| a.quorum_eq(candidate))
            .count();
        if agreeing >= threshold {
            return Ok(candidate.clone());
        }
    }
    Err(ChainSourceError::NoProvider)
}

// The two views present the full [`ChainSource`] surface; each method dispatches through the view's
// trust rule via a closure capturing the query's arguments.

impl ChainSource for TrustedView<'_> {
    type Error = ChainSourceError;

    fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
        self.custody_read(move |p| p.coin_record(coin_id))
    }

    fn coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: Bytes32,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        self.custody_read(move |p| p.coin_records_by_puzzle_hash(puzzle_hash, include_spent))
    }

    fn coin_records_by_parent(
        &self,
        parent_coin_id: Bytes32,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        self.custody_read(move |p| p.coin_records_by_parent(parent_coin_id))
    }

    fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
        self.custody_read(move |p| p.coin_spend(coin_id))
    }

    fn resolve_singleton_lineage(
        &self,
        launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        // Cross-source agreement on a lineage requires the FULL lineage to match; consumers still
        // apply the authority MEMBERSHIP test (`SingletonLineage::contains`) to the result — never
        // tip/puzzle-hash equality (SPEC §5).
        self.custody_read(move |p| p.resolve_singleton_lineage(launcher_id))
    }

    fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
        self.custody_read(|p| p.peak_height())
    }

    fn block_timestamp(&self, height: u32) -> Result<Option<u64>, Self::Error> {
        self.custody_read(move |p| p.block_timestamp(height))
    }
}

impl ChainSource for DiscoveryView<'_> {
    type Error = ChainSourceError;

    fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
        self.discovery_read(move |p| p.coin_record(coin_id))
    }

    fn coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: Bytes32,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        self.discovery_read(move |p| p.coin_records_by_puzzle_hash(puzzle_hash, include_spent))
    }

    fn coin_records_by_parent(
        &self,
        parent_coin_id: Bytes32,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        self.discovery_read(move |p| p.coin_records_by_parent(parent_coin_id))
    }

    fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
        self.discovery_read(move |p| p.coin_spend(coin_id))
    }

    fn resolve_singleton_lineage(
        &self,
        launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        self.discovery_read(move |p| p.resolve_singleton_lineage(launcher_id))
    }

    fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
        self.discovery_read(|p| p.peak_height())
    }

    fn block_timestamp(&self, height: u32) -> Result<Option<u64>, Self::Error> {
        self.discovery_read(move |p| p.block_timestamp(height))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chia_protocol::Coin;
    use dig_chainsource_interface::MockChainSource;

    use crate::provider_registry::providers::{CoinsetProvider, CustomProvider, LocalNodeProvider};

    fn coin_id(byte: u8) -> Bytes32 {
        Coin::new(Bytes32::new([byte; 32]), Bytes32::new([byte; 32]), 1).coin_id()
    }

    fn record_for(id: Bytes32) -> CoinRecord {
        CoinRecord {
            coin: Coin::new(id, Bytes32::new([0x22; 32]), 1),
            confirmed_height: Some(100),
            spent_height: None,
            timestamp: Some(1_700_000_000),
            coinbase: false,
        }
    }

    fn mock_with(id: Bytes32) -> MockChainSource {
        MockChainSource::new().with_coin(id, record_for(id))
    }

    // ---- Test #2 (LOAD-BEARING): pure-public quorum WITHOUT opt-in fails closed ----

    #[test]
    fn pure_public_quorum_without_optin_fails_closed_for_custody() {
        let id = coin_id(0x01);
        // Two INDEPENDENT public sources that both KNOW the coin and AGREE — yet, with no
        // operator-trusted source and no opt-in, custody MUST NOT be satisfied.
        let registry = ProviderRegistry::new()
            .register(
                Box::new(CoinsetProvider::new("coinset-a", 10, mock_with(id))),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CustomProvider::new("mirror-b", 20, mock_with(id))),
                None,
                "mirror.example",
            );

        let result = registry.trusted().coin_record(id);
        assert_eq!(
            result,
            Err(ChainSourceError::NoProvider),
            "pure-public custody must fail closed without allow_public_quorum_custody"
        );
    }

    // ---- Test #3: an operator-Trusted LocalNode satisfies custody ----

    #[test]
    fn operator_trusted_local_node_satisfies_custody() {
        let id = coin_id(0x02);
        let registry = ProviderRegistry::new().register(
            Box::new(LocalNodeProvider::new("local", 0, mock_with(id))),
            None, // LocalNode defaults to Trusted
            "local-node",
        );

        let record = registry.trusted().coin_record(id).unwrap();
        assert_eq!(record, Some(record_for(id)));
    }

    #[test]
    fn local_node_defaults_to_trusted() {
        assert_eq!(
            TrustLevel::default_for(ProviderKind::LocalNode),
            TrustLevel::Trusted
        );
        assert_eq!(
            TrustLevel::default_for(ProviderKind::PublicOracle),
            TrustLevel::Untrusted
        );
    }

    // ---- Test #4: a quorum INCLUDING a trusted member satisfies custody ----

    #[test]
    fn quorum_including_trusted_member_satisfies_custody() {
        let id = coin_id(0x03);
        let registry = ProviderRegistry::new()
            .register(
                Box::new(LocalNodeProvider::new("local", 0, mock_with(id))),
                None, // Trusted
                "local-node",
            )
            .register(
                Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
                None, // Untrusted public
                "coinset.org",
            );

        // The trusted member answers; custody is satisfied.
        let record = registry.trusted().coin_record(id).unwrap();
        assert_eq!(record, Some(record_for(id)));
    }

    #[test]
    fn trusted_source_error_fails_closed_not_public_fallback() {
        let id = coin_id(0x04);
        let registry = ProviderRegistry::new()
            .register(
                Box::new(LocalNodeProvider::new(
                    "local",
                    0,
                    MockChainSource::new().fail_with(ChainSourceError::Timeout),
                )),
                None,
                "local-node",
            )
            .register(
                Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
                None,
                "coinset.org",
            );

        // The trusted node errored; custody fails closed rather than reading the public source.
        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::Timeout)
        );
    }

    // ---- Test #5: opt-in pure-public quorum needs T=2 INDEPENDENT groups ----

    #[test]
    fn optin_two_independent_groups_agree_satisfies_custody() {
        let id = coin_id(0x05);
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CustomProvider::new("mirror", 20, mock_with(id))),
                None,
                "mirror.example",
            );

        assert_eq!(
            registry.trusted().coin_record(id).unwrap(),
            Some(record_for(id))
        );
    }

    #[test]
    fn optin_single_group_fails_closed() {
        let id = coin_id(0x06);
        // Only ONE independent group qualifies -> below threshold -> fail closed.
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
                None,
                "coinset.org",
            );

        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
    }

    #[test]
    fn optin_two_providers_same_group_fails_closed() {
        let id = coin_id(0x07);
        // Two providers agreeing but in the SAME independence group count as ONE -> below T=2.
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new("coinset-a", 10, mock_with(id))),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CoinsetProvider::new("coinset-b", 20, mock_with(id))),
                None,
                "coinset.org", // SAME group
            );

        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
    }

    #[test]
    fn optin_two_groups_disagree_fails_closed() {
        let id = coin_id(0x08);
        // Two independent groups that DISAGREE (one knows the coin, one does not) -> no quorum.
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CustomProvider::new("empty", 20, MockChainSource::new())),
                None,
                "mirror.example",
            );

        // coinset says Some(record), mirror says Ok(None) — disagreement -> fail closed.
        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
    }

    // ---- Order-insensitive quorum agreement (#1259 fix 1) ----

    /// A test source returning a fixed, caller-controlled ORDER of records for the list reads, so a
    /// quorum comparison can be exercised across sources that agree on the SET but differ in order.
    struct FixedListSource {
        records: Vec<CoinRecord>,
    }

    impl ChainSource for FixedListSource {
        type Error = ChainSourceError;

        fn coin_record(&self, _coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
            Ok(None)
        }

        fn coin_records_by_puzzle_hash(
            &self,
            _puzzle_hash: Bytes32,
            _include_spent: bool,
        ) -> Result<Vec<CoinRecord>, Self::Error> {
            Ok(self.records.clone())
        }

        fn coin_records_by_parent(
            &self,
            _parent_coin_id: Bytes32,
        ) -> Result<Vec<CoinRecord>, Self::Error> {
            Ok(self.records.clone())
        }

        fn coin_spend(&self, _coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
            Ok(None)
        }

        fn resolve_singleton_lineage(
            &self,
            _launcher_id: Bytes32,
        ) -> Result<Option<SingletonLineage>, Self::Error> {
            Ok(None)
        }

        fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
            Ok(None)
        }

        fn block_timestamp(&self, _height: u32) -> Result<Option<u64>, Self::Error> {
            Ok(None)
        }
    }

    fn list_source(records: Vec<CoinRecord>) -> FixedListSource {
        FixedListSource { records }
    }

    #[test]
    fn quorum_agrees_on_same_record_set_in_different_order() {
        let ph = Bytes32::new([0x22; 32]);
        let a = record_for(coin_id(0x0A));
        let b = record_for(coin_id(0x0B));

        // Two INDEPENDENT public sources return the SAME set of records in REVERSED order. With the
        // opt-in quorum, they must AGREE (order is not a source of disagreement).
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new(
                    "coinset",
                    10,
                    list_source(vec![a.clone(), b.clone()]),
                )),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CustomProvider::new(
                    "mirror",
                    20,
                    list_source(vec![b.clone(), a.clone()]),
                )),
                None,
                "mirror.example",
            );

        let records = registry
            .trusted()
            .coin_records_by_puzzle_hash(ph, false)
            .expect("honest sources agreeing on a set must satisfy the quorum");
        assert_eq!(records.len(), 2);
        assert!(records.contains(&a) && records.contains(&b));
    }

    #[test]
    fn quorum_still_fails_closed_on_genuinely_different_record_sets() {
        let ph = Bytes32::new([0x22; 32]);
        let a = record_for(coin_id(0x0A));
        let b = record_for(coin_id(0x0B));
        let c = record_for(coin_id(0x0C));

        // Different SETS ({a,b} vs {a,c}) must still be treated as disagreement -> fail closed. The
        // order-insensitive comparison must not weaken the quorum into accepting different content.
        let registry = ProviderRegistry::new()
            .allow_public_quorum_custody(true)
            .register(
                Box::new(CoinsetProvider::new(
                    "coinset",
                    10,
                    list_source(vec![a.clone(), b]),
                )),
                None,
                "coinset.org",
            )
            .register(
                Box::new(CustomProvider::new("mirror", 20, list_source(vec![c, a]))),
                None,
                "mirror.example",
            );

        assert_eq!(
            registry.trusted().coin_records_by_puzzle_hash(ph, false),
            Err(ChainSourceError::NoProvider),
            "genuinely different record sets must fail closed"
        );
    }

    // ---- Test #7: discovery view returns a single-provider answer, inert for custody ----

    #[test]
    fn discovery_view_returns_single_provider_answer() {
        let id = coin_id(0x09);
        let registry = ProviderRegistry::new().register(
            Box::new(CoinsetProvider::new("coinset", 10, mock_with(id))),
            None,
            "coinset.org",
        );

        // Discovery answers from the single public provider...
        assert_eq!(
            registry.any().coin_record(id).unwrap(),
            Some(record_for(id))
        );
        // ...but the SAME registry fails closed for custody (no trusted source, no opt-in).
        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
    }

    #[test]
    fn every_read_method_flows_through_both_views() {
        let ph = Bytes32::new([0x22; 32]);
        let parent = Coin::new(Bytes32::new([0x01; 32]), ph, 1);
        let parent_id = parent.coin_id();
        let child = Coin::new(parent_id, ph, 1);
        let launcher = Bytes32::new([0x77; 32]);

        let source = MockChainSource::new()
            .with_coin(parent_id, record_for(parent_id))
            .with_coin(child.coin_id(), {
                let mut r = record_for(child.coin_id());
                r.coin = child;
                r
            })
            .with_spend(
                parent_id,
                chia_protocol::CoinSpend::new(
                    parent,
                    chia_protocol::Program::from(vec![1]),
                    chia_protocol::Program::from(vec![0x80]),
                ),
            )
            .with_lineage(launcher, SingletonLineage::single(launcher))
            .with_timestamp(100, 1_700_000_000)
            .with_peak(555);

        let registry = ProviderRegistry::new().register(
            Box::new(LocalNodeProvider::new("local", 0, source)),
            None, // Trusted
            "local-node",
        );

        // Custody view — every method answers via the trusted source.
        let custody = registry.trusted();
        assert!(!custody
            .coin_records_by_puzzle_hash(ph, true)
            .unwrap()
            .is_empty());
        assert!(!custody
            .coin_records_by_parent(parent_id)
            .unwrap()
            .is_empty());
        assert!(custody.coin_spend(parent_id).unwrap().is_some());
        assert_eq!(custody.peak_height().unwrap(), Some(555));
        assert_eq!(custody.block_timestamp(100).unwrap(), Some(1_700_000_000));
        assert_eq!(
            custody.resolve_singleton_lineage(launcher).unwrap(),
            Some(SingletonLineage::single(launcher))
        );

        // Discovery view — same reads, single-provider, no trust guarantee.
        let discovery = registry.any();
        assert!(discovery.coin_record(parent_id).unwrap().is_some());
        assert!(!discovery
            .coin_records_by_puzzle_hash(ph, false)
            .unwrap()
            .is_empty());
        assert!(!discovery
            .coin_records_by_parent(parent_id)
            .unwrap()
            .is_empty());
        assert!(discovery.coin_spend(parent_id).unwrap().is_some());
        assert_eq!(discovery.peak_height().unwrap(), Some(555));
        assert_eq!(discovery.block_timestamp(100).unwrap(), Some(1_700_000_000));
        assert!(discovery
            .resolve_singleton_lineage(launcher)
            .unwrap()
            .is_some());
    }

    #[test]
    fn discovery_falls_through_failing_providers_to_a_responder() {
        let id = coin_id(0x0B);
        let registry = ProviderRegistry::new()
            .register(
                Box::new(CoinsetProvider::new(
                    "down",
                    0,
                    MockChainSource::new().fail_with(ChainSourceError::Timeout),
                )),
                None,
                "down",
            )
            .register(
                Box::new(CoinsetProvider::new("up", 10, mock_with(id))),
                None,
                "up",
            );
        assert_eq!(
            registry.any().coin_record(id).unwrap(),
            Some(record_for(id))
        );
    }

    #[test]
    fn empty_registry_fails_closed_everywhere() {
        let registry = ProviderRegistry::new();
        let id = coin_id(0x0A);
        assert_eq!(
            registry.trusted().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
        assert_eq!(
            registry.any().coin_record(id),
            Err(ChainSourceError::NoProvider)
        );
    }
}
