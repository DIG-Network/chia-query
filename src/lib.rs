//! # chia-query
//!
//! Query the Chia blockchain through decentralized peer connections with
//! automatic fallback to the [coinset.org](https://api.coinset.org) HTTP API.
//!
//! No Chia installation is required. The peer TLS client identity is generated in
//! memory by default ([`TlsIdentity::Generated`]), so a client works from a service
//! account with no home directory of its own.
//!
//! ```rust,no_run
//! use chia_query::{ChiaQuery, ChiaQueryConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;
//!     let record = client.get_coin_record_by_name("0xabc...").await?;
//!     println!("{:?}", record);
//!     Ok(())
//! }
//! ```

pub mod coinset;
pub mod drift;
pub mod types;

// The `@dignetwork/chia-query-wasm` bindings — only for the wasm coinset build.
#[cfg(all(target_arch = "wasm32", feature = "coinset", not(feature = "native")))]
pub mod wasm_api;

// The peer WebSocket backend + the routing/CLVM layer are native-only: they
// pull in `chia-wallet-sdk` (native-tls), `clvmr`, and tokio networking, none
// of which belong in the wasm coinset-only build.
#[cfg(feature = "native")]
pub mod peer;
#[cfg(feature = "native")]
pub mod provider_registry;
#[cfg(feature = "native")]
pub mod router;

pub use types::*;

// Everything below — the full `ChiaQuery` client that races peers against the
// coinset fallback — is the native surface. A wasm consumer uses
// [`coinset::CoinsetClient`] directly with an injected `fetch` transport.
#[cfg(feature = "native")]
mod native_client {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    use serde_json::Value;

    use crate::types::*;
    use crate::{coinset, peer, router};

    // ---------------------------------------------------------------------------
    // NetworkType
    // ---------------------------------------------------------------------------

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum NetworkType {
        Mainnet,
        Testnet11,
    }

    impl NetworkType {
        pub fn network_id(self) -> &'static str {
            match self {
                Self::Mainnet => "mainnet",
                Self::Testnet11 => "testnet11",
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Configuration
    // ---------------------------------------------------------------------------

    /// Where the peer-protocol TLS client identity comes from.
    ///
    /// Modelled as one choice rather than a pair of optional paths so a half-configured
    /// identity — a certificate without its key — cannot be expressed.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum TlsIdentity {
        /// Generate a fresh self-signed certificate in memory (the default).
        ///
        /// Chia full nodes accept any well-formed client certificate, so nothing is
        /// gained by requiring one on disk and a great deal is lost: a service account
        /// has no populated `~/.chia`, which is what made every balance read fail in
        /// dig_ecosystem#2210. See [`peer::connect::create_generated_tls`] for why the
        /// certificate is not persisted.
        Generated,

        /// Load an existing certificate/key pair, e.g. a real Chia node's wallet cert.
        Files {
            cert_path: PathBuf,
            key_path: PathBuf,
        },
    }

    pub struct ChiaQueryConfig {
        pub network: NetworkType,
        /// How many peer sessions the pool holds.
        ///
        /// Defaults to [`peer::plurality::default_max_peers`], which is DERIVED from the sample a
        /// corroborated read needs to leave standing rather than picked. Lowering it below that
        /// does not make corroboration weaker quietly — the pool refuses rather than degrading —
        /// but it does make it unavailable.
        pub max_peers: usize,
        pub coinset_base_url: String,
        pub coinset_fallback_enabled: bool,
        pub tls_identity: TlsIdentity,
        pub peer_connect_timeout: Duration,
        pub peer_request_timeout: Duration,
        pub coinset_request_timeout: Duration,
    }

    impl Default for ChiaQueryConfig {
        fn default() -> Self {
            Self {
                network: NetworkType::Mainnet,
                max_peers: peer::plurality::default_max_peers(),
                coinset_base_url: "https://api.coinset.org".into(),
                coinset_fallback_enabled: true,
                tls_identity: TlsIdentity::Generated,
                peer_connect_timeout: Duration::from_secs(8),
                peer_request_timeout: Duration::from_secs(30),
                coinset_request_timeout: Duration::from_secs(30),
            }
        }
    }

    // ---------------------------------------------------------------------------
    // ChiaQuery -- the public entry-point
    // ---------------------------------------------------------------------------

    pub struct ChiaQuery {
        router: router::QueryRouter,
    }

    impl ChiaQuery {
        /// Create a new client.  This will:
        /// 1. Establish the peer TLS identity (generated by default — no files needed).
        /// 2. Discover peers via DNS and connect up to `max_peers` concurrently.
        /// 3. Initialise the coinset.org HTTP client.
        ///
        /// Peer discovery failing is fatal ([`ChiaQueryError::PeerDiscoveryFailed`])
        /// ONLY when the coinset fallback is disabled; with the fallback enabled the
        /// client is usable immediately and the peer pool refills in the background.
        pub async fn new(cfg: ChiaQueryConfig) -> Result<Self, ChiaQueryError> {
            let tls = match &cfg.tls_identity {
                TlsIdentity::Generated => peer::connect::create_generated_tls()?,
                TlsIdentity::Files {
                    cert_path,
                    key_path,
                } => peer::connect::create_tls(cert_path, key_path)?,
            };

            // The coinset tier is plain HTTP and needs neither a credential nor a peer,
            // so a peer-tier problem must not deny a reader the fallback that exists
            // for exactly that case (dig_ecosystem#2210).
            let peer_requirement = if cfg.coinset_fallback_enabled {
                peer::PeerRequirement::Optional
            } else {
                peer::PeerRequirement::Required
            };

            let peer_backend = peer::PeerBackend::new(
                cfg.network,
                tls,
                cfg.max_peers,
                peer_requirement,
                cfg.peer_connect_timeout,
                cfg.peer_request_timeout,
            )
            .await?;

            let coinset_client =
                coinset::CoinsetClient::new(&cfg.coinset_base_url, cfg.coinset_request_timeout)?;

            Ok(Self {
                router: router::QueryRouter {
                    peer: std::sync::Arc::new(peer_backend),
                    coinset: coinset_client,
                    coinset_fallback_enabled: cfg.coinset_fallback_enabled,
                },
            })
        }

        /// The independence group this client must be registered under, derived from the fabric it
        /// can actually REACH rather than from what a caller believes it is.
        ///
        /// # Why this is not a caller's choice
        ///
        /// `ProviderRegistry` decides custody by independence group: a pure-public quorum keeps one
        /// representative answer per group and refuses below the threshold. So the group id is a
        /// security-relevant input, and on dig-node#354 it was supplied as a literal that had gone
        /// stale — a `ChiaQuery` was registered as `"chia-peers"` while its router asks
        /// `api.coinset.org` FIRST whenever the fallback is enabled. Both "independent" groups then
        /// answered from one endpoint, and a client configured with `max_peers: 0` — holding no
        /// peers whatsoever — satisfied a two-of-two independent-group custody quorum.
        ///
        /// # The derivation, and why it collapses conservatively
        ///
        /// A client whose coinset fallback is enabled CAN answer from coinset.org, so it is not
        /// independent of any other coinset-backed source and reports
        /// [`COINSET_INDEPENDENCE_GROUP`](crate::provider_registry::COINSET_INDEPENDENCE_GROUP).
        /// Only a client that cannot reach coinset at all is a pure peer fabric and reports
        /// [`CHIA_PEERS_INDEPENDENCE_GROUP`](crate::provider_registry::CHIA_PEERS_INDEPENDENCE_GROUP).
        ///
        /// Collapsing a peers-AND-coinset client into the coinset group is deliberate and
        /// fail-closed: it can lie *together with* coinset, which is precisely what a shared group
        /// records. Claiming the peer group because peers are also reachable would restore the
        /// defect — an attacker controlling coinset would face a quorum it could satisfy alone.
        ///
        /// Register with this, never with a literal:
        ///
        /// ```ignore
        /// let group = query.independence_group();
        /// let registry = ProviderRegistry::new().register(Box::new(provider), None, group);
        /// ```
        pub fn independence_group(&self) -> &'static str {
            crate::provider_registry::independence_group_for(self.router.coinset_fallback_enabled)
        }

        // =======================================================================
        // Blocks
        // =======================================================================

        pub async fn get_additions_and_removals(
            &self,
            header_hash: &str,
        ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
            self.router.get_additions_and_removals(header_hash).await
        }

        pub async fn get_block(&self, header_hash: &str) -> Result<FullBlock, ChiaQueryError> {
            self.router.get_block(header_hash).await
        }

        /// Fetch a full block by height.  Peer-backed via `RequestBlock`.
        pub async fn get_block_by_height(&self, height: u32) -> Result<FullBlock, ChiaQueryError> {
            self.router.get_block_by_height(height).await
        }

        pub async fn get_block_count_metrics(&self) -> Result<BlockCountMetrics, ChiaQueryError> {
            self.router.get_block_count_metrics().await
        }

        pub async fn get_block_record(
            &self,
            header_hash: &str,
        ) -> Result<BlockRecord, ChiaQueryError> {
            self.router.get_block_record(header_hash).await
        }

        pub async fn get_block_record_by_height(
            &self,
            height: u32,
        ) -> Result<BlockRecord, ChiaQueryError> {
            self.router.get_block_record_by_height(height).await
        }

        pub async fn get_block_records(
            &self,
            start: u32,
            end: u32,
        ) -> Result<Vec<BlockRecord>, ChiaQueryError> {
            self.router.get_block_records(start, end).await
        }

        pub async fn get_block_spends(
            &self,
            header_hash: &str,
        ) -> Result<Vec<CoinSpend>, ChiaQueryError> {
            self.router.get_block_spends(header_hash).await
        }

        pub async fn get_block_spends_with_conditions(
            &self,
            header_hash: &str,
        ) -> Result<Vec<CoinSpendWithConditions>, ChiaQueryError> {
            self.router
                .get_block_spends_with_conditions(header_hash)
                .await
        }

        pub async fn get_blocks(
            &self,
            start: u32,
            end: u32,
            exclude_header_hash: bool,
            exclude_reorged: bool,
        ) -> Result<Vec<FullBlock>, ChiaQueryError> {
            self.router
                .get_blocks(start, end, exclude_header_hash, exclude_reorged)
                .await
        }

        pub async fn get_unfinished_block_headers(
            &self,
        ) -> Result<Vec<UnfinishedBlockHeader>, ChiaQueryError> {
            self.router.get_unfinished_block_headers().await
        }

        // =======================================================================
        // Coins
        // =======================================================================

        pub async fn get_coin_record_by_name(
            &self,
            name: &str,
        ) -> Result<CoinRecord, ChiaQueryError> {
            self.router.get_coin_record_by_name(name).await
        }

        /// Absence-aware [`get_coin_record_by_name`](Self::get_coin_record_by_name).
        ///
        /// `Ok(None)` means **corroborated absence**: two independent sources were asked and both
        /// reported no such coin. Absence that only one source will vouch for is
        /// [`UncorroboratedAbsence`](ChiaQueryError::UncorroboratedAbsence), and two sources that
        /// contradict each other are [`SourcesDisagree`](ChiaQueryError::SourcesDisagree) — both
        /// errors, because neither is a fact about the chain (dig_ecosystem#2456).
        ///
        /// `Ok(Some(record))` means **corroborated presence**, and means it for the same reason:
        /// the coin-id binding authenticates the coin's identity only, so `confirmed_block_index`
        /// and `spent_block_index` are put to a second independent source before they are
        /// reported. A record only one source will vouch for is
        /// [`UncorroboratedPresence`](ChiaQueryError::UncorroboratedPresence)
        /// (dig_ecosystem#2462).
        ///
        /// Used by the [`ChainSource`](dig_chainsource_interface::ChainSource) facade to honour the
        /// fail-closed `Ok(None)`-vs-`Err` contract.
        pub async fn get_coin_record_by_name_opt(
            &self,
            name: &str,
        ) -> Result<Option<CoinRecord>, ChiaQueryError> {
            self.router.get_coin_record_by_name_opt(name).await
        }

        /// Absence-aware read of the spend that spent `coin_id`.
        ///
        /// `Ok(None)` when two independent sources agree the coin is unspent or unknown,
        /// `Ok(Some(spend))` when two agree on the spend; `Err` on failure, and on an answer in
        /// either direction that only one source will vouch for — see
        /// [`get_coin_record_by_name_opt`](Self::get_coin_record_by_name_opt).
        pub async fn get_coin_spend_opt(
            &self,
            coin_id: &str,
        ) -> Result<Option<CoinSpend>, ChiaQueryError> {
            self.router.get_coin_spend_opt(coin_id).await
        }

        /// The current peak height (`Ok(None)` when unavailable), `Err` on failure.
        pub async fn peak_height_opt(&self) -> Result<Option<u32>, ChiaQueryError> {
            self.router.peak_height_opt().await
        }

        /// How many Chia full-node peers this client HOLDS right now.
        ///
        /// Exposed because a consumer that presents itself as a light client has to be able to
        /// SAY how many peers it is a client of, and until now the pool's size was observable
        /// only as the boolean [`has_peers`](peer::PeerBackend::has_peers). A count is not
        /// derivable from that, and a consumer with no way to read it is left either silent or
        /// quoting [`ChiaQueryConfig::max_peers`] — an intention presented as a measurement.
        ///
        /// It is the LIVE count, never the target: a filling pool reports the smaller number.
        /// See [`peer::pool::PeerPool::peer_count`] for what "held" means with respect to a peer
        /// that has died without being used since.
        pub async fn peer_count(&self) -> usize {
            self.router.peer.peer_count().await
        }

        /// How many held peers count as INDEPENDENT opinions about the chain.
        ///
        /// Never larger than [`peer_count`](Self::peer_count), and smaller by the peers reached
        /// from a preferred address — an operator's `TRUSTED_FULLNODE`, or a full node on this
        /// machine. Those are the fastest peers to read from and the worst possible witnesses to
        /// each other: a local process is a source a local attacker can supply, so a count of
        /// agreeing sources that includes one is not a count of independent sources.
        ///
        /// Use this, not `peer_count`, for any decision of the form "do enough separate sources
        /// agree" (dig_ecosystem#2648). Use `peer_count` to tell a user how many peers are held.
        pub async fn independent_peer_count(&self) -> usize {
            self.router.peer.independent_peer_count().await
        }

        /// The peak height this client's OWN peers have reported, or `None` when they have
        /// reported none yet.
        ///
        /// Distinct from [`peak_height_opt`](Self::peak_height_opt), which answers "what is the
        /// chain's peak" and consults coinset FIRST — so its figure is a third party's view of
        /// the chain even on a client holding peers. This one answers "what have MY peers told
        /// me", which is the only form of the question a light client can demonstrate, and it
        /// makes no network call at all: the pool tracks it from inbound `NewPeakWallet`
        /// messages.
        ///
        /// `None` is UNKNOWN, never height zero. The pool spells an unobserved peak `0`
        /// internally, and every block is trivially above zero, so returning it would silently
        /// satisfy any "is this buried yet" comparison a caller makes.
        pub async fn peer_peak_height(&self) -> Option<u32> {
            observed_peak(self.router.peer.peak_height())
        }

        /// A subscribing light client over THIS client's peer pool.
        ///
        /// The returned [`ChiaLightClient`](peer::ChiaLightClient) borrows the same held sessions
        /// this client reads through, so a consumer that needs both coin subscriptions and ordinary
        /// queries holds ONE set of peers with ONE notion of the peak. Building a second
        /// `ChiaQuery` to get a light client would recreate exactly the split this replaces.
        ///
        /// `request_timeout` bounds each of the light client's own requests; it does not affect
        /// this client's.
        pub async fn light_client(&self, request_timeout: Duration) -> peer::ChiaLightClient {
            peer::ChiaLightClient::new(self.router.peer.clone(), request_timeout).await
        }

        /// The Unix timestamp of the block at `height` (`Ok(None)` when absent), `Err` on failure.
        pub async fn block_timestamp_opt(
            &self,
            height: u32,
        ) -> Result<Option<u64>, ChiaQueryError> {
            self.router.block_timestamp_opt(height).await
        }

        pub async fn get_coin_records_by_hint(
            &self,
            hint: &str,
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_hint(hint, start_height, end_height, include_spent_coins)
                .await
        }

        pub async fn get_coin_records_by_hints(
            &self,
            hints: &[String],
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_hints(hints, start_height, end_height, include_spent_coins)
                .await
        }

        pub async fn get_coin_records_by_names(
            &self,
            names: &[String],
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_names(names, start_height, end_height, include_spent_coins)
                .await
        }

        pub async fn get_coin_records_by_parent_ids(
            &self,
            parent_ids: &[String],
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_parent_ids(
                    parent_ids,
                    start_height,
                    end_height,
                    include_spent_coins,
                )
                .await
        }

        pub async fn get_coin_records_by_puzzle_hash(
            &self,
            puzzle_hash: &str,
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_puzzle_hash(
                    puzzle_hash,
                    start_height,
                    end_height,
                    include_spent_coins,
                )
                .await
        }

        pub async fn get_coin_records_by_puzzle_hashes(
            &self,
            puzzle_hashes: &[String],
            start_height: Option<u32>,
            end_height: Option<u32>,
            include_spent_coins: bool,
        ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
            self.router
                .get_coin_records_by_puzzle_hashes(
                    puzzle_hashes,
                    start_height,
                    end_height,
                    include_spent_coins,
                )
                .await
        }

        pub async fn get_memos_by_coin_name(&self, name: &str) -> Result<Value, ChiaQueryError> {
            self.router.get_memos_by_coin_name(name).await
        }

        pub async fn get_puzzle_and_solution(
            &self,
            coin_id: &str,
            height: Option<u32>,
        ) -> Result<CoinSpend, ChiaQueryError> {
            self.router.get_puzzle_and_solution(coin_id, height).await
        }

        pub async fn get_puzzle_and_solution_with_conditions(
            &self,
            coin_id: &str,
            height: Option<u32>,
        ) -> Result<CoinSpendWithConditions, ChiaQueryError> {
            self.router
                .get_puzzle_and_solution_with_conditions(coin_id, height)
                .await
        }

        pub async fn push_tx(
            &self,
            spend_bundle: &SpendBundle,
        ) -> Result<TxStatus, ChiaQueryError> {
            self.router.push_tx(spend_bundle).await
        }

        // =======================================================================
        // Fees
        // =======================================================================

        pub async fn get_fee_estimate(
            &self,
            spend_bundle: Option<&SpendBundle>,
            target_times: Option<&[u64]>,
            spend_count: Option<u64>,
        ) -> Result<FeeEstimate, ChiaQueryError> {
            self.router
                .get_fee_estimate(spend_bundle, target_times, spend_count)
                .await
        }

        // =======================================================================
        // Full node / network
        // =======================================================================

        pub async fn get_aggsig_additional_data(&self) -> Result<String, ChiaQueryError> {
            self.router.get_aggsig_additional_data().await
        }

        pub async fn get_network_info(&self) -> Result<NetworkInfo, ChiaQueryError> {
            self.router.get_network_info().await
        }

        pub async fn get_blockchain_state(&self) -> Result<BlockchainState, ChiaQueryError> {
            self.router.get_blockchain_state().await
        }

        pub async fn get_network_space(
            &self,
            newer_block_header_hash: &str,
            older_block_header_hash: &str,
        ) -> Result<u64, ChiaQueryError> {
            self.router
                .get_network_space(newer_block_header_hash, older_block_header_hash)
                .await
        }

        // =======================================================================
        // Mempool
        // =======================================================================

        pub async fn get_all_mempool_items(
            &self,
        ) -> Result<HashMap<String, MempoolItem>, ChiaQueryError> {
            self.router.get_all_mempool_items().await
        }

        pub async fn get_all_mempool_tx_ids(&self) -> Result<Vec<String>, ChiaQueryError> {
            self.router.get_all_mempool_tx_ids().await
        }

        pub async fn get_mempool_item_by_tx_id(
            &self,
            tx_id: &str,
        ) -> Result<MempoolItem, ChiaQueryError> {
            self.router.get_mempool_item_by_tx_id(tx_id).await
        }

        pub async fn get_mempool_items_by_coin_name(
            &self,
            coin_name: &str,
            include_spent_coins: Option<bool>,
        ) -> Result<Vec<MempoolItem>, ChiaQueryError> {
            self.router
                .get_mempool_items_by_coin_name(coin_name, include_spent_coins)
                .await
        }

        // =======================================================================
        // Convenience helpers
        // =======================================================================

        /// Poll the blockchain until a coin appears on-chain (confirmed) or the
        /// timeout elapses.
        ///
        /// Returns the [`CoinRecord`] once the coin is found with a non-zero
        /// `confirmed_block_index`.  Returns an error if the timeout expires
        /// before the coin is confirmed.
        ///
        /// ```rust,no_run
        /// # use chia_query::{ChiaQuery, ChiaQueryConfig};
        /// # use std::time::Duration;
        /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
        /// let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;
        /// let record = client.wait_for_confirmation(
        ///     "0xabc...",
        ///     Duration::from_secs(5),   // poll every 5 seconds
        ///     Duration::from_secs(300), // give up after 5 minutes
        /// ).await?;
        /// println!("confirmed at height {}", record.confirmed_block_index);
        /// # Ok(())
        /// # }
        /// ```
        pub async fn wait_for_confirmation(
            &self,
            coin_id: &str,
            poll_interval: Duration,
            timeout: Duration,
        ) -> Result<CoinRecord, ChiaQueryError> {
            let deadline = tokio::time::Instant::now() + timeout;

            loop {
                match self.get_coin_record_by_name(coin_id).await {
                    Ok(record) if record.confirmed_block_index > 0 => {
                        return Ok(record);
                    }
                    Ok(_) => {
                        // Coin exists but confirmed_block_index is 0 -- not
                        // confirmed yet, keep polling.
                    }
                    Err(ChiaQueryError::PeerRejection(_))
                    | Err(ChiaQueryError::CoinsetApiError(_)) => {
                        // Coin not found yet -- keep polling.
                    }
                    Err(e) => {
                        // Transient connection errors -- log and keep trying.
                        log::debug!("wait_for_confirmation poll error: {e}");
                    }
                }

                if tokio::time::Instant::now() + poll_interval > deadline {
                    return Err(ChiaQueryError::PeerConnection(format!(
                        "coin {coin_id} not confirmed within {timeout:?}"
                    )));
                }

                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    /// The pool's peak sentinel as an honest optional height.
    ///
    /// Kept as a named pure function rather than inlined, because the rule it encodes — an
    /// unobserved peak is UNKNOWN and not height zero — is the whole reason
    /// [`ChiaQuery::peer_peak_height`] returns an `Option`, and inline it is unreachable from a
    /// test on a machine with no peers.
    fn observed_peak(raw: u32) -> Option<u32> {
        (raw != 0).then_some(raw)
    }

    #[cfg(test)]
    mod tests {
        use super::observed_peak;

        /// **An unobserved peak is unknown, never zero.** The pool spells "no peer has told me a
        /// peak" as `0`, and every block is trivially above zero — so a caller asking "is this
        /// coin buried yet" against a leaked `0` gets a confident yes about a chain nobody has
        /// looked at.
        #[test]
        fn an_unobserved_peak_is_unknown_and_a_real_height_survives() {
            assert_eq!(observed_peak(0), None);
            assert_eq!(observed_peak(1), Some(1));
            assert_eq!(observed_peak(9_139_211), Some(9_139_211));
        }
    }

    #[cfg(test)]
    mod independence_group_tests {
        use super::*;
        use crate::coinset::CoinsetClient;
        use crate::peer::PeerBackend;
        use crate::provider_registry::{CHIA_PEERS_INDEPENDENCE_GROUP, COINSET_INDEPENDENCE_GROUP};

        /// A client whose router dials NOTHING, built directly so both fallback settings are
        /// reachable offline.
        ///
        /// It cannot go through [`ChiaQuery::new`], and that is the whole reason this fixture
        /// exists: `new` sets [`peer::PeerRequirement::Required`] whenever the coinset fallback is
        /// DISABLED, so the `false` branch can only be constructed by dialling a real peer. Built
        /// this way, the branch that a constant-returning method would misclassify becomes
        /// testable on an isolated runner. Mirrors the router fixture in
        /// `router::presence_tests`; neither tier is ever consulted, so the URL is unroutable.
        fn client(coinset_fallback_enabled: bool) -> ChiaQuery {
            ChiaQuery {
                router: router::QueryRouter {
                    peer: std::sync::Arc::new(PeerBackend::for_tests()),
                    coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(1))
                        .expect("build a client that is never called"),
                    coinset_fallback_enabled,
                },
            }
        }

        /// **The METHOD reads the routing predicate; it does not return a constant.**
        ///
        /// The mutation this catches is
        /// `independence_group_for(self.router.coinset_fallback_enabled)` becoming
        /// `independence_group_for(true)`. Every other test in the crate stays green under it: the
        /// free-function tests call `independence_group_for` directly and never reach this method,
        /// and the integration suite can only build a fallback-ENABLED client, whose expected
        /// answer a coinset-hard-wired method produces identically.
        ///
        /// The surviving direction is fail-closed and still harmful. A genuine pure-peer fabric
        /// reported as coinset-backed COLLAPSES two real independence groups into one, so a
        /// legitimate two-group quorum can never be satisfied - a denial of exactly the property
        /// NC-12 needs, rather than a bypass of it.
        ///
        /// The `false` case is asserted FIRST because it is the only assertion the mutation can
        /// fail; putting the passing case first would make the panic message ambiguous about which
        /// property broke.
        #[test]
        fn the_group_is_read_from_the_routers_own_fallback_setting() {
            assert_eq!(
                client(false).independence_group(),
                CHIA_PEERS_INDEPENDENCE_GROUP,
                "a router that CANNOT reach coinset is a pure peer fabric; reporting the coinset                  group here collapses two independence groups into one and makes a legitimate                  two-group quorum permanently unsatisfiable",
            );
            assert_eq!(
                client(true).independence_group(),
                COINSET_INDEPENDENCE_GROUP,
                "a router that falls back to coinset.org shares the coinset group",
            );
            assert_ne!(
                client(true).independence_group(),
                client(false).independence_group(),
                "the method must distinguish the two fabrics, or the classification it feeds                  register() carries no information at all",
            );
        }
    }
} // mod native_client

#[cfg(feature = "native")]
pub use native_client::{ChiaQuery, ChiaQueryConfig, NetworkType, TlsIdentity};
