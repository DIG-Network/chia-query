//! Conversions between `chia_protocol` peer-protocol types and our public
//! response types.

use chia_protocol::{Bytes32, CoinState, HeaderBlock, Program, RespondAdditions, RespondRemovals};

use crate::types::{
    AdditionsAndRemovals, BlockRecord, ChiaQueryError, Coin, CoinRecord, CoinSpend, FeeEstimate,
    MempoolInclusion, TxStatus,
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
    coin: &chia_protocol::Coin,
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

/// Translate a `TransactionAck` into a [`TxStatus`], preserving BOTH the verdict and the node's
/// own reason for it.
///
/// `status` is Chia's `MempoolInclusionStatus` byte and `error` is the ack's `error` field --
/// the mempool's words for a refusal, which the caller cannot reconstruct from anything else.
///
/// Status 2 is `PENDING`, which is the node declining to admit the bundle, so it maps to
/// [`MempoolInclusion::NotAdmitted`] and `success: false` (#48).
pub fn ack_to_tx_status(status: u8, error: Option<String>) -> TxStatus {
    let (label, inclusion) = match status {
        1 => ("SUCCESS", MempoolInclusion::Admitted),
        2 => ("PENDING", MempoolInclusion::NotAdmitted),
        3 => ("FAILED", MempoolInclusion::Failed),
        _ => ("UNKNOWN", MempoolInclusion::Unknown),
    };
    TxStatus {
        status: label.to_string(),
        success: inclusion.is_admitted(),
        inclusion,
        error,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chia_protocol::{Coin as ProtoCoin, Program};

    /// #1258 regression: the peer path must build a `CoinSpend` from the GENUINE spent coin, not a
    /// name-only placeholder. This proves `make_coin_spend` carries every genuine coin field through
    /// verbatim (so the downstream lineage coin-id binding can authenticate the hop), and that the
    /// pre-fix placeholder coin (default puzzle hash, zero amount) does NOT match — which is exactly
    /// why peer-sourced lineage failed closed before the fix.
    #[test]
    fn make_coin_spend_preserves_the_genuine_coin() {
        let genuine = ProtoCoin {
            parent_coin_info: Bytes32::new([0x11; 32]),
            puzzle_hash: Bytes32::new([0x22; 32]),
            amount: 7,
        };
        let puzzle = Program::from(vec![0x80]);
        let solution = Program::from(vec![0x80]);

        let spend = make_coin_spend(&genuine, &puzzle, &solution);
        assert_eq!(
            spend.coin.parent_coin_info,
            hex32(&genuine.parent_coin_info)
        );
        assert_eq!(spend.coin.puzzle_hash, hex32(&genuine.puzzle_hash));
        assert_eq!(spend.coin.amount, genuine.amount);

        // The old placeholder shared only the coin name (as parent); its puzzle hash and amount
        // differ, so it hashes to a different coin id and would fail the lineage binding closed.
        let placeholder = ProtoCoin {
            parent_coin_info: genuine.parent_coin_info,
            puzzle_hash: Bytes32::default(),
            amount: 0,
        };
        let placeholder_spend = make_coin_spend(&placeholder, &puzzle, &solution);
        assert_ne!(placeholder_spend.coin.puzzle_hash, spend.coin.puzzle_hash);
        assert_ne!(placeholder_spend.coin.amount, spend.coin.amount);
    }

    /// #48 regression, defect 1. Chia status 2 is `MempoolInclusionStatus::PENDING`, which means
    /// the full node did NOT admit the bundle: it is holding it for an unknown parent, or refusing
    /// it below the fee floor, and it may never be admitted at all. `success` is the field a caller
    /// branches on to answer "is my transaction in a mempool", so reporting `true` here is a
    /// money-shaped wrong answer.
    ///
    /// Status 2 is the ONLY byte on which the pre-fix reading (`status == 1 || status == 2`) and
    /// the honest one differ, so a suite without this case passes against the defect. The
    /// SUCCESS and FAILED assertions beside it are the control: they hold under both readings, and
    /// their presence is what proves the fix narrowed `success` rather than inverting it.
    #[test]
    fn pending_ack_is_not_admitted_to_the_mempool() {
        assert!(
            ack_to_tx_status(1, None).success,
            "status 1 (SUCCESS) IS admission -- the fix must not invert the flag"
        );
        assert!(
            !ack_to_tx_status(2, None).success,
            "status 2 (PENDING) is a REFUSAL to admit; success must not claim otherwise"
        );
        assert!(!ack_to_tx_status(3, None).success);
        assert!(!ack_to_tx_status(9, None).success);
    }

    /// #48 regression, defect 2. The node names the reason it refused, and that string is the most
    /// useful thing an operator can be shown -- during a live incident the cause was
    /// `BAD_AGGREGATE_SIGNATURE`, which the node said plainly and which reached no log or caller.
    ///
    /// The fixture deliberately pushes TWO DIFFERENT reasons through the same status byte and
    /// requires two different observable results. A test asserting only that `error` is `Some` or
    /// non-empty would pass against an implementation that hard-codes one string, or that reports
    /// the label a second time; requiring the two to differ, and each to equal its own input
    /// verbatim, can only be satisfied by carrying the node's actual words.
    #[test]
    fn two_different_ack_reasons_produce_two_different_results() {
        let sig = ack_to_tx_status(3, Some("BAD_AGGREGATE_SIGNATURE".into()));
        let dust = ack_to_tx_status(2, Some("INVALID_FEE_TOO_CLOSE_TO_ZERO".into()));

        assert_eq!(sig.error.as_deref(), Some("BAD_AGGREGATE_SIGNATURE"));
        assert_eq!(dust.error.as_deref(), Some("INVALID_FEE_TOO_CLOSE_TO_ZERO"));
        assert_ne!(sig.error, dust.error, "two refusals must not read alike");

        // Structured, not formatted: the reason lives in its own field, so a caller reads it
        // without parsing it back out of a prose label.
        assert_eq!(sig.status, "FAILED");
        assert_eq!(dust.status, "PENDING");
        assert!(!sig.status.contains("SIGNATURE"));

        // A node that gave no reason must not gain an invented one.
        assert_eq!(ack_to_tx_status(1, None).error, None);
    }

    /// #48. The three-state must actually distinguish the three verdicts. `success` alone cannot:
    /// it is false for both a held bundle and a rejected one, which are different problems, and an
    /// implementation that narrowed the boolean while collapsing the states would satisfy the
    /// admission test above and still leave a caller unable to tell them apart.
    #[test]
    fn each_ack_byte_maps_to_its_own_inclusion_state() {
        assert_eq!(ack_to_tx_status(1, None).inclusion, MempoolInclusion::Admitted);
        assert_eq!(
            ack_to_tx_status(2, None).inclusion,
            MempoolInclusion::NotAdmitted
        );
        assert_eq!(ack_to_tx_status(3, None).inclusion, MempoolInclusion::Failed);
        assert_eq!(ack_to_tx_status(7, None).inclusion, MempoolInclusion::Unknown);

        // An unrecognised byte fails CLOSED -- it is not admission.
        assert!(!MempoolInclusion::Unknown.is_admitted());
        assert!(!MempoolInclusion::NotAdmitted.is_admitted());
    }
}
