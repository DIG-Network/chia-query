//! Conversions between `chia_protocol` peer-protocol types and our public
//! response types.

use chia_protocol::{Bytes32, CoinState, HeaderBlock, Program, RespondAdditions, RespondRemovals};

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

/// The header hash `block` proves for `height`, or `None` when the block is not at that height.
///
/// The hash is COMPUTED from the foliage, so a peer cannot name a hash for a block it did not
/// serve. That alone is not enough: a peer asked for height H may answer with a real block at
/// H', and the hash of THAT block is equally genuine while answering a different question. Two
/// peers each serving one frozen header would then compare EQUAL at every height — false
/// agreement, which is the corroboration failure this guard exists to prevent.
///
/// `None` means abstention, never agreement.
pub fn header_hash_at_height(block: &HeaderBlock, height: u32) -> Option<Bytes32> {
    if block.height() != height {
        return None;
    }
    Some(block.header_hash())
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
    use chia_protocol::{
        ClassgroupElement, Coin as ProtoCoin, Foliage, FoliageBlockData, PoolTarget, Program,
        ProofOfSpace, RewardChainBlock, VDFInfo, VDFProof,
    };

    /// A structurally-valid `HeaderBlock` at `height`, whose foliage — and therefore whose
    /// header hash — is keyed on `marker`.
    ///
    /// The height and the hashed content are varied INDEPENDENTLY on purpose. A fixture whose
    /// hash were a function of its height could not tell "the guard compared heights" from
    /// "the two blocks simply hashed differently", which is the property under test.
    fn header_block_at(height: u32, marker: u8) -> HeaderBlock {
        let vdf_info = VDFInfo {
            challenge: Bytes32::new([0; 32]),
            number_of_iterations: 0,
            output: ClassgroupElement::default(),
        };
        let vdf_proof = || VDFProof {
            witness_type: 0,
            witness: Vec::new().into(),
            normalized_to_identity: false,
        };
        HeaderBlock {
            finished_sub_slots: Vec::new(),
            reward_chain_block: RewardChainBlock {
                weight: 0,
                height,
                total_iters: 0,
                signage_point_index: 0,
                pos_ss_cc_challenge_hash: Bytes32::new([0; 32]),
                proof_of_space: ProofOfSpace {
                    challenge: Bytes32::new([0; 32]),
                    pool_public_key: None,
                    pool_contract_puzzle_hash: None,
                    plot_public_key: chia_bls::PublicKey::default(),
                    version_and_size: 32,
                    proof: Vec::new().into(),
                },
                challenge_chain_sp_vdf: None,
                challenge_chain_sp_signature: chia_bls::Signature::default(),
                challenge_chain_ip_vdf: vdf_info.clone(),
                reward_chain_sp_vdf: None,
                reward_chain_sp_signature: chia_bls::Signature::default(),
                reward_chain_ip_vdf: vdf_info.clone(),
                infused_challenge_chain_ip_vdf: None,
                header_mmr_root: None,
                is_transaction_block: false,
            },
            challenge_chain_sp_proof: None,
            challenge_chain_ip_proof: vdf_proof(),
            reward_chain_sp_proof: None,
            reward_chain_ip_proof: vdf_proof(),
            infused_challenge_chain_ip_proof: None,
            foliage: Foliage {
                prev_block_hash: Bytes32::new([marker; 32]),
                reward_block_hash: Bytes32::new([marker; 32]),
                foliage_block_data: FoliageBlockData {
                    unfinished_reward_block_hash: Bytes32::new([marker; 32]),
                    pool_target: PoolTarget {
                        puzzle_hash: Bytes32::new([0; 32]),
                        max_height: 0,
                    },
                    pool_signature: None,
                    farmer_reward_puzzle_hash: Bytes32::new([0; 32]),
                    extension_data: Bytes32::new([0; 32]),
                },
                foliage_block_data_signature: chia_bls::Signature::default(),
                foliage_transaction_block_hash: None,
                foliage_transaction_block_signature: None,
            },
            foliage_transaction_block: None,
            transactions_filter: Vec::new().into(),
            transactions_info: None,
        }
    }

    /// A peer asked for height H may answer with a real block at H'. The hash it yields is
    /// genuine and verifiable and answers a DIFFERENT question, so it must be refused.
    ///
    /// The control is the same block accepted at its own height — without it this test would
    /// pass against a helper that returns `None` unconditionally.
    #[test]
    fn a_header_hash_is_returned_only_for_the_height_that_was_asked_for() {
        let block = header_block_at(100, 0xAB);

        assert_eq!(
            header_hash_at_height(&block, 100),
            Some(block.header_hash()),
            "the control: a block AT the requested height answers with its computed hash"
        );
        assert_eq!(
            header_hash_at_height(&block, 101),
            None,
            "a block at another height must abstain, not answer for the height asked"
        );
    }

    /// The false-agreement shape this guard exists to stop (dig_ecosystem#2666): two members
    /// both serving one frozen header compare EQUAL at any height, so a caller corroborating
    /// across members reads unanimous agreement about a height neither peer answered for.
    #[test]
    fn two_peers_serving_one_stale_header_cannot_agree_at_a_height_neither_answered() {
        let stale = header_block_at(100, 0xAB);

        // Unguarded, both members would yield this identical hash at height 900 000.
        assert_eq!(
            stale.header_hash(),
            header_block_at(100, 0xAB).header_hash()
        );

        assert_eq!(header_hash_at_height(&stale, 900_000), None);
    }

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
}
