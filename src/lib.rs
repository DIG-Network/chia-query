//! # chia-query
//!
//! Query the Chia blockchain through decentralized peer connections with
//! automatic fallback to the [coinset.org](https://api.coinset.org) HTTP API.
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
pub mod peer;
pub mod router;
pub mod types;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

pub use types::*;

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

    fn default_cert_path(self) -> PathBuf {
        let base = dirs_home().join(".chia");
        match self {
            Self::Mainnet => base.join("mainnet/config/ssl/wallet/wallet_node.crt"),
            Self::Testnet11 => base.join("testnet11/config/ssl/wallet/wallet_node.crt"),
        }
    }

    fn default_key_path(self) -> PathBuf {
        let base = dirs_home().join(".chia");
        match self {
            Self::Mainnet => base.join("mainnet/config/ssl/wallet/wallet_node.key"),
            Self::Testnet11 => base.join("testnet11/config/ssl/wallet/wallet_node.key"),
        }
    }
}

fn dirs_home() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("C:\\"))
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/"))
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub struct ChiaQueryConfig {
    pub network: NetworkType,
    pub max_peers: usize,
    pub coinset_base_url: String,
    pub coinset_fallback_enabled: bool,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub peer_connect_timeout: Duration,
    pub peer_request_timeout: Duration,
    pub coinset_request_timeout: Duration,
}

impl Default for ChiaQueryConfig {
    fn default() -> Self {
        let network = NetworkType::Mainnet;
        Self {
            network,
            max_peers: 5,
            coinset_base_url: "https://api.coinset.org".into(),
            coinset_fallback_enabled: true,
            cert_path: network.default_cert_path(),
            key_path: network.default_key_path(),
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
    /// 1. Load TLS certificates from the configured paths.
    /// 2. Discover peers via DNS and connect up to `max_peers` concurrently.
    /// 3. Initialise the coinset.org HTTP client.
    ///
    /// At least one peer must connect successfully, otherwise this returns
    /// [`ChiaQueryError::PeerDiscoveryFailed`].
    pub async fn new(cfg: ChiaQueryConfig) -> Result<Self, ChiaQueryError> {
        let tls = peer::connect::create_tls(&cfg.cert_path, &cfg.key_path)?;

        let peer_backend = peer::PeerBackend::new(
            cfg.network,
            tls,
            cfg.max_peers,
            cfg.peer_connect_timeout,
            cfg.peer_request_timeout,
        )
        .await?;

        let coinset_client =
            coinset::CoinsetClient::new(&cfg.coinset_base_url, cfg.coinset_request_timeout)?;

        Ok(Self {
            router: router::QueryRouter {
                peer: peer_backend,
                coinset: coinset_client,
                coinset_fallback_enabled: cfg.coinset_fallback_enabled,
            },
        })
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

    pub async fn get_block_record(&self, header_hash: &str) -> Result<BlockRecord, ChiaQueryError> {
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

    pub async fn get_coin_record_by_name(&self, name: &str) -> Result<CoinRecord, ChiaQueryError> {
        self.router.get_coin_record_by_name(name).await
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

    pub async fn push_tx(&self, spend_bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
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
                Err(ChiaQueryError::PeerRejection(_)) | Err(ChiaQueryError::CoinsetApiError(_)) => {
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
