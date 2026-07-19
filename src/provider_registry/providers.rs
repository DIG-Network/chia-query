//! The four provider-kind wrappers the registry composes: [`CoinsetProvider`] (public oracle),
//! [`LocalNodeProvider`] (the operator's node), [`DigPeersProvider`] (the DIG peer network), and
//! [`CustomProvider`] (an operator override).
//!
//! Each wraps ANY [`ChainSource`] and labels it with the right [`ProviderKind`], so the same
//! wrapper composes a live [`ChiaQueryProvider`](crate::provider_registry::ChiaQueryProvider) in
//! production and a `MockChainSource` in tests. The wrappers add identity + kind only; they do NOT
//! grant trust — the registry's operator-assigned [`TrustLevel`](super::TrustLevel) does that.

use std::borrow::Cow;

use chia_protocol::{Bytes32, CoinSpend};
use dig_chainsource_interface::{
    ChainSource, ChainSourceProvider, CoinRecord, ProviderId, ProviderInfo, ProviderKind,
    SingletonLineage,
};

/// Defines a provider-kind wrapper over an arbitrary [`ChainSource`], delegating every read to the
/// inner source and reporting a fixed [`ProviderKind`] via [`ChainSourceProvider`].
macro_rules! kinded_provider {
    ($(#[$meta:meta])* $name:ident, $kind:expr) => {
        $(#[$meta])*
        pub struct $name<S> {
            inner: S,
            info: ProviderInfo,
        }

        impl<S> $name<S> {
            /// Wraps `source` with a stable `id`, a try-order `priority` (lower = tried first), and
            /// this wrapper's fixed [`ProviderKind`].
            pub fn new(id: impl Into<Cow<'static, str>>, priority: i32, source: S) -> Self {
                Self {
                    inner: source,
                    info: ProviderInfo {
                        id: ProviderId(id.into()),
                        kind: $kind,
                        priority,
                        // A wrapper never self-declares trustlessness; the registry assigns trust.
                        trustless: false,
                    },
                }
            }
        }

        impl<S> ChainSource for $name<S>
        where
            S: ChainSource,
        {
            type Error = S::Error;

            fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
                self.inner.coin_record(coin_id)
            }

            fn coin_records_by_puzzle_hash(
                &self,
                puzzle_hash: Bytes32,
                include_spent: bool,
            ) -> Result<Vec<CoinRecord>, Self::Error> {
                self.inner.coin_records_by_puzzle_hash(puzzle_hash, include_spent)
            }

            fn coin_records_by_parent(
                &self,
                parent_coin_id: Bytes32,
            ) -> Result<Vec<CoinRecord>, Self::Error> {
                self.inner.coin_records_by_parent(parent_coin_id)
            }

            fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
                self.inner.coin_spend(coin_id)
            }

            fn parent_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
                self.inner.parent_spend(coin_id)
            }

            fn resolve_singleton_lineage(
                &self,
                launcher_id: Bytes32,
            ) -> Result<Option<SingletonLineage>, Self::Error> {
                self.inner.resolve_singleton_lineage(launcher_id)
            }

            fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
                self.inner.peak_height()
            }

            fn block_timestamp(&self, height: u32) -> Result<Option<u64>, Self::Error> {
                self.inner.block_timestamp(height)
            }
        }

        impl<S> ChainSourceProvider for $name<S>
        where
            S: ChainSource,
        {
            fn provider_info(&self) -> ProviderInfo {
                self.info.clone()
            }
        }
    };
}

kinded_provider!(
    /// A public oracle/gateway (e.g. coinset.org): convenient, but `Untrusted` for custody by
    /// default — only ever a quorum member unless the operator explicitly vouches for it.
    CoinsetProvider,
    ProviderKind::PublicOracle
);

kinded_provider!(
    /// The operator's own full/wallet node — the most trustworthy source, `Trusted` for custody by
    /// default. Compose it over the §5.3 `dig.local` -> `localhost` node ladder.
    LocalNodeProvider,
    ProviderKind::LocalNode
);

kinded_provider!(
    /// Chain data served over the DIG peer network: `Untrusted` for custody by default.
    DigPeersProvider,
    ProviderKind::DigPeers
);

kinded_provider!(
    /// An operator-supplied override not covered by the other kinds: `Untrusted` for custody by
    /// default (the operator may raise it to `Trusted` at registration).
    CustomProvider,
    ProviderKind::Custom
);

#[cfg(test)]
mod tests {
    use super::*;
    use dig_chainsource_interface::MockChainSource;

    #[test]
    fn wrappers_report_their_kind_and_identity() {
        let coinset = CoinsetProvider::new("coinset.org", 10, MockChainSource::new());
        assert_eq!(coinset.provider_info().kind, ProviderKind::PublicOracle);
        assert_eq!(coinset.provider_info().priority, 10);
        assert!(!coinset.provider_info().trustless);

        let local = LocalNodeProvider::new("local", 0, MockChainSource::new());
        assert_eq!(local.provider_info().kind, ProviderKind::LocalNode);

        let peers = DigPeersProvider::new("dig-peers", 20, MockChainSource::new());
        assert_eq!(peers.provider_info().kind, ProviderKind::DigPeers);

        let custom = CustomProvider::new("custom", 30, MockChainSource::new());
        assert_eq!(custom.provider_info().kind, ProviderKind::Custom);
    }
}
