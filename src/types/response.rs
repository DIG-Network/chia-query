use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Core coin types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Coin {
    pub parent_coin_info: String,
    pub puzzle_hash: String,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinRecord {
    pub coin: Coin,
    pub confirmed_block_index: u32,
    pub spent_block_index: u32,
    pub spent: bool,
    pub coinbase: bool,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinSpend {
    pub coin: Coin,
    pub puzzle_reveal: String,
    pub solution: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub opcode: serde_json::Value,
    pub vars: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinSpendWithConditions {
    pub coin_spend: CoinSpend,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendBundle {
    pub coin_spends: Vec<CoinSpend>,
    pub aggregated_signature: String,
}

// ---------------------------------------------------------------------------
// Transaction status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxStatus {
    pub status: String,
    pub success: bool,
}

// ---------------------------------------------------------------------------
// Block types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdditionsAndRemovals {
    pub additions: Vec<CoinRecord>,
    pub removals: Vec<CoinRecord>,
}

/// Block record with common fields typed and the rest captured in `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockRecord {
    #[serde(default)]
    pub header_hash: String,
    #[serde(default)]
    pub height: u32,
    #[serde(default)]
    pub weight: u64,
    #[serde(default)]
    pub prev_hash: String,
    #[serde(default)]
    pub total_iters: u64,
    #[serde(default)]
    pub signage_point_index: u8,
    #[serde(default)]
    pub farmer_puzzle_hash: String,
    #[serde(default)]
    pub pool_puzzle_hash: String,
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub fees: Option<u64>,
    /// All remaining fields the API may return.
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

/// Full block -- the JSON shape is very deep; we expose it as opaque JSON so
/// callers can drill in as needed without us maintaining dozens of sub-structs.
pub type FullBlock = serde_json::Value;

/// Unfinished block header -- same rationale as FullBlock.
pub type UnfinishedBlockHeader = serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCountMetrics {
    pub compact_blocks: u64,
    pub uncompact_blocks: u64,
    pub hint_count: u64,
}

// ---------------------------------------------------------------------------
// Fee estimate
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeEstimate {
    pub estimates: Vec<f64>,
    pub target_times: Vec<u64>,
    pub current_fee_rate: f64,
    #[serde(default)]
    pub mempool_size: u64,
    #[serde(default)]
    pub mempool_fees: u64,
    #[serde(default)]
    pub mempool_max_size: u64,
    #[serde(default)]
    pub num_spends: u64,
    #[serde(default)]
    pub full_node_synced: bool,
    #[serde(default)]
    pub peak_height: u32,
    #[serde(default)]
    pub last_peak_timestamp: u64,
    #[serde(default)]
    pub last_block_cost: u64,
    #[serde(default)]
    pub fees_last_block: u64,
    #[serde(default)]
    pub fee_rate_last_block: f64,
    #[serde(default)]
    pub last_tx_block_height: u32,
    #[serde(default)]
    pub node_time_utc: u64,
}

// ---------------------------------------------------------------------------
// Full-node / network info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInfo {
    pub network_name: String,
    pub network_prefix: String,
    #[serde(default)]
    pub genesis_challenge: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub sync_mode: bool,
    pub sync_progress_height: u32,
    pub sync_tip_height: u32,
    pub synced: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockchainState {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub difficulty: u64,
    #[serde(default)]
    pub genesis_challenge_initialized: bool,
    #[serde(default)]
    pub mempool_size: u64,
    #[serde(default)]
    pub mempool_cost: u64,
    #[serde(default)]
    pub mempool_fees: u64,
    #[serde(default)]
    pub mempool_min_fees: serde_json::Value,
    #[serde(default)]
    pub mempool_max_total_cost: u64,
    #[serde(default)]
    pub block_max_cost: u64,
    #[serde(default)]
    pub peak: Option<BlockRecord>,
    #[serde(default)]
    pub space: u64,
    #[serde(default)]
    pub sub_slot_iters: u64,
    #[serde(default)]
    pub average_block_time: Option<f64>,
    #[serde(default)]
    pub sync: Option<SyncState>,
}

// ---------------------------------------------------------------------------
// Mempool
// ---------------------------------------------------------------------------

/// Mempool items have a complex, version-dependent shape -- exposed as JSON.
pub type MempoolItem = serde_json::Value;

// ---------------------------------------------------------------------------
// Convenience conversions between our types and chia protocol types
// ---------------------------------------------------------------------------

impl Coin {
    /// Build from a `chia::protocol::Coin`, encoding hashes as 0x-prefixed hex.
    pub fn from_protocol(c: &chia::protocol::Coin) -> Self {
        Self {
            parent_coin_info: format!("0x{}", hex::encode(c.parent_coin_info)),
            puzzle_hash: format!("0x{}", hex::encode(c.puzzle_hash)),
            amount: c.amount,
        }
    }
}
