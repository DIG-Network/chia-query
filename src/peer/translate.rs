//! Conversions between `chia_protocol` peer-protocol types and our public
//! response types.

use chia::protocol::{Bytes32, CoinState, HeaderBlock, Program, RespondAdditions, RespondRemovals};

use crate::types::{
    AdditionsAndRemovals, BlockRecord, ChiaQueryError, Coin, CoinRecord, CoinSpend, FeeEstimate,
    TxStatus,
};

// ---------------------------------------------------------------------------
// Hex utilities
// ---------------------------------------------------------------------------

pub fn parse_hex(s: &str) -> Result<Vec<u8>, ChiaQueryError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s).map_err(|e| ChiaQueryError::InvalidRequest(format!("bad hex: {e}")))
}

pub fn parse_bytes32(s: &str) -> Result<Bytes32, ChiaQueryError> {
    let bytes = parse_hex(s)?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ChiaQueryError::InvalidRequest("expected 32 bytes".into()))?;
    Ok(Bytes32::new(arr))
}

pub fn hex32(b: &Bytes32) -> String {
    format!("0x{}", hex::encode(b.as_ref()))
}

pub fn hex_bytes(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

// ---------------------------------------------------------------------------
// CoinState -> CoinRecord
// ---------------------------------------------------------------------------

pub fn coin_state_to_record(cs: &CoinState) -> CoinRecord {
    CoinRecord {
        coin: Coin::from_protocol(&cs.coin),
        confirmed_block_index: cs.created_height.unwrap_or(0),
        spent_block_index: cs.spent_height.unwrap_or(0),
        spent: cs.spent_height.is_some(),
        // These fields are not available via the peer protocol.
        coinbase: false,
        timestamp: 0,
    }
}

pub fn coin_states_to_records(states: &[CoinState]) -> Vec<CoinRecord> {
    states.iter().map(coin_state_to_record).collect()
}

// ---------------------------------------------------------------------------
// Peer puzzle-and-solution -> our CoinSpend
// ---------------------------------------------------------------------------

pub fn make_coin_spend(
    coin: &chia::protocol::Coin,
    puzzle: &Program,
    solution: &Program,
) -> CoinSpend {
    CoinSpend {
        coin: Coin::from_protocol(coin),
        puzzle_reveal: hex_bytes(puzzle.as_ref()),
        solution: hex_bytes(solution.as_ref()),
    }
}

// ---------------------------------------------------------------------------
// Fee estimates (peer response is minimal; fill in defaults for fields only
// available through the full-node RPC).
// ---------------------------------------------------------------------------

pub fn make_fee_estimate(estimates: Vec<f64>, target_times: Vec<u64>) -> FeeEstimate {
    FeeEstimate {
        estimates,
        target_times,
        current_fee_rate: 0.0,
        mempool_size: 0,
        mempool_fees: 0,
        mempool_max_size: 0,
        num_spends: 0,
        full_node_synced: false,
        peak_height: 0,
        last_peak_timestamp: 0,
        last_block_cost: 0,
        fees_last_block: 0,
        fee_rate_last_block: 0.0,
        last_tx_block_height: 0,
        node_time_utc: 0,
    }
}

// ---------------------------------------------------------------------------
// TransactionAck -> TxStatus
// ---------------------------------------------------------------------------

pub fn ack_to_tx_status(status: u8) -> TxStatus {
    let label = match status {
        1 => "SUCCESS",
        2 => "PENDING",
        3 => "FAILED",
        _ => "UNKNOWN",
    };
    TxStatus {
        status: label.to_string(),
        success: status == 1 || status == 2,
    }
}

// ---------------------------------------------------------------------------
// HeaderBlock -> BlockRecord
// ---------------------------------------------------------------------------

pub fn header_block_to_block_record(hb: &HeaderBlock) -> BlockRecord {
    let rcb = &hb.reward_chain_block;
    let foliage = &hb.foliage;

    let timestamp = hb.foliage_transaction_block.as_ref().map(|ft| ft.timestamp);

    BlockRecord {
        header_hash: hex32(&foliage.reward_block_hash),
        height: rcb.height,
        weight: rcb.weight as u64,
        prev_hash: hex32(&foliage.prev_block_hash),
        total_iters: rcb.total_iters as u64,
        signage_point_index: rcb.signage_point_index,
        farmer_puzzle_hash: hex32(&foliage.foliage_block_data.farmer_reward_puzzle_hash),
        pool_puzzle_hash: String::new(), // pool target is in PoolTarget, not a plain hash
        timestamp,
        fees: None, // not available from the header alone
        extra: serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// RequestAdditions / RequestRemovals -> AdditionsAndRemovals
// ---------------------------------------------------------------------------

pub fn additions_removals_to_response(
    additions_resp: &RespondAdditions,
    removals_resp: &RespondRemovals,
    height: u32,
) -> AdditionsAndRemovals {
    let additions: Vec<CoinRecord> = additions_resp
        .coins
        .iter()
        .flat_map(|(_ph, coins)| {
            coins.iter().map(|c| CoinRecord {
                coin: Coin::from_protocol(c),
                confirmed_block_index: height,
                spent_block_index: 0,
                spent: false,
                coinbase: false,
                timestamp: 0,
            })
        })
        .collect();

    let removals: Vec<CoinRecord> = removals_resp
        .coins
        .iter()
        .filter_map(|(_name, maybe_coin)| {
            maybe_coin.as_ref().map(|c| CoinRecord {
                coin: Coin::from_protocol(c),
                confirmed_block_index: 0,
                spent_block_index: height,
                spent: true,
                coinbase: false,
                timestamp: 0,
            })
        })
        .collect();

    AdditionsAndRemovals {
        additions,
        removals,
    }
}
