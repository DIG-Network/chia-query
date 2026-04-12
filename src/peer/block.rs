//! Block parsing utilities.
//!
//! Uses `chia_consensus` to extract additions, removals, and coin spends from
//! a `FullBlock`'s CLVM generator.  Follows the same patterns as the
//! chia-block-listener / chia-generator-parser crates.

use chia::bls::Signature;
use chia::consensus::additions_and_removals::additions_and_removals;
use chia::consensus::consensus_constants::ConsensusConstants;
use chia::consensus::flags::DONT_VALIDATE_SIGNATURE;
use chia::consensus::get_puzzle_and_solution::get_puzzle_and_solution_for_coin;
use chia::consensus::run_block_generator::run_block_generator2;
use chia::consensus::{allocator::make_allocator, validation_error};
use chia::protocol::{Bytes32, FullBlock};

use clvmr::serde::{node_from_bytes_backrefs_record, node_to_bytes};

use crate::types::*;

// ---------------------------------------------------------------------------
// Additions & removals from a FullBlock
// ---------------------------------------------------------------------------

/// Extract coin additions and removals from a full block's generator.
pub fn block_additions_and_removals(
    block: &FullBlock,
    height: u32,
    constants: &ConsensusConstants,
) -> Result<AdditionsAndRemovals, ChiaQueryError> {
    let generator = match &block.transactions_generator {
        Some(g) => g,
        None => {
            return Ok(AdditionsAndRemovals {
                additions: reward_coins(block, height),
                removals: Vec::new(),
            });
        }
    };

    let timestamp = block_timestamp(block);
    let block_refs: Vec<&[u8]> = Vec::new();
    let flags = DONT_VALIDATE_SIGNATURE;

    let (raw_additions, raw_removals) =
        additions_and_removals(generator.as_ref(), block_refs, flags, constants)
            .map_err(|e| ChiaQueryError::PeerConnection(format!("CLVM execution failed: {e:?}")))?;

    let mut additions: Vec<CoinRecord> = raw_additions
        .iter()
        .map(|(coin, _hint)| CoinRecord {
            coin: Coin::from_protocol(coin),
            confirmed_block_index: height,
            spent_block_index: 0,
            spent: false,
            coinbase: false,
            timestamp,
        })
        .collect();

    additions.extend(reward_coins(block, height));

    let removals: Vec<CoinRecord> = raw_removals
        .iter()
        .map(|coin| CoinRecord {
            coin: Coin::from_protocol(coin),
            confirmed_block_index: 0,
            spent_block_index: height,
            spent: true,
            coinbase: false,
            timestamp: 0,
        })
        .collect();

    Ok(AdditionsAndRemovals {
        additions,
        removals,
    })
}

// ---------------------------------------------------------------------------
// Block spends (puzzle_reveal + solution for every spent coin)
// ---------------------------------------------------------------------------

/// Run the block generator and extract all coin spends with their
/// puzzle_reveal and solution.
pub fn block_spends(
    block: &FullBlock,
    constants: &ConsensusConstants,
) -> Result<Vec<CoinSpend>, ChiaQueryError> {
    let generator = match &block.transactions_generator {
        Some(g) => g,
        None => return Ok(Vec::new()),
    };

    let flags = DONT_VALIDATE_SIGNATURE;
    let block_refs: Vec<&[u8]> = Vec::new();
    let mut allocator = make_allocator(flags);

    let conds = run_block_generator2(
        &mut allocator,
        generator.as_ref(),
        &block_refs,
        constants.max_block_cost_clvm,
        flags,
        &Signature::default(),
        None,
        constants,
    )
    .map_err(|e| ChiaQueryError::PeerConnection(format!("run_block_generator2 failed: {e:?}")))?;

    let (program, backrefs) =
        node_from_bytes_backrefs_record(&mut allocator, generator.as_ref())
            .map_err(|e| ChiaQueryError::PeerConnection(format!("parse generator: {e:?}")))?;

    let args =
        chia::consensus::run_block_generator::setup_generator_args(&mut allocator, &block_refs)
            .map_err(|e| ChiaQueryError::PeerConnection(format!("setup args: {e:?}")))?;

    let dialect = clvmr::chia_dialect::ChiaDialect::new(flags);
    let reduction = clvmr::run_program::run_program(
        &mut allocator,
        &dialect,
        program,
        args,
        constants.max_block_cost_clvm,
    )
    .map_err(|e| ChiaQueryError::PeerConnection(format!("run_program: {e:?}")))?;

    let generator_output = reduction.1;

    let mut spends = Vec::new();
    for sc in &conds.spends {
        let parent_id: Bytes32 = allocator.atom(sc.parent_id).as_ref().try_into().unwrap();
        let puzzle_hash: Bytes32 = allocator.atom(sc.puzzle_hash).as_ref().try_into().unwrap();
        let removal = chia::protocol::Coin {
            parent_coin_info: parent_id,
            puzzle_hash,
            amount: sc.coin_amount,
        };

        if let Ok((puzzle_node, solution_node)) =
            get_puzzle_and_solution_for_coin(&allocator, generator_output, &backrefs, &removal)
        {
            let puzzle_bytes = node_to_bytes(&allocator, puzzle_node).unwrap_or_default();
            let solution_bytes = node_to_bytes(&allocator, solution_node).unwrap_or_default();

            spends.push(CoinSpend {
                coin: Coin::from_protocol(&removal),
                puzzle_reveal: format!("0x{}", hex::encode(&puzzle_bytes)),
                solution: format!("0x{}", hex::encode(&solution_bytes)),
            });
        }
    }

    Ok(spends)
}

// ---------------------------------------------------------------------------
// Block spends WITH parsed conditions
// ---------------------------------------------------------------------------

/// Same as `block_spends` but also runs each puzzle against its solution to
/// extract the CLVM output conditions.
pub fn block_spends_with_conditions(
    block: &FullBlock,
    constants: &ConsensusConstants,
) -> Result<Vec<CoinSpendWithConditions>, ChiaQueryError> {
    let generator = match &block.transactions_generator {
        Some(g) => g,
        None => return Ok(Vec::new()),
    };

    let flags = DONT_VALIDATE_SIGNATURE;
    let block_refs: Vec<&[u8]> = Vec::new();
    let mut allocator = make_allocator(flags);

    let conds = run_block_generator2(
        &mut allocator,
        generator.as_ref(),
        &block_refs,
        constants.max_block_cost_clvm,
        flags,
        &Signature::default(),
        None,
        constants,
    )
    .map_err(|e| ChiaQueryError::PeerConnection(format!("run_block_generator2 failed: {e:?}")))?;

    let (program, backrefs) =
        node_from_bytes_backrefs_record(&mut allocator, generator.as_ref())
            .map_err(|e| ChiaQueryError::PeerConnection(format!("parse generator: {e:?}")))?;

    let args =
        chia::consensus::run_block_generator::setup_generator_args(&mut allocator, &block_refs)
            .map_err(|e| ChiaQueryError::PeerConnection(format!("setup args: {e:?}")))?;

    let dialect = clvmr::chia_dialect::ChiaDialect::new(flags);
    let reduction = clvmr::run_program::run_program(
        &mut allocator,
        &dialect,
        program,
        args,
        constants.max_block_cost_clvm,
    )
    .map_err(|e| ChiaQueryError::PeerConnection(format!("run_program: {e:?}")))?;

    let generator_output = reduction.1;

    let mut result = Vec::new();
    for sc in &conds.spends {
        let parent_id: Bytes32 = allocator.atom(sc.parent_id).as_ref().try_into().unwrap();
        let puzzle_hash: Bytes32 = allocator.atom(sc.puzzle_hash).as_ref().try_into().unwrap();
        let removal = chia::protocol::Coin {
            parent_coin_info: parent_id,
            puzzle_hash,
            amount: sc.coin_amount,
        };

        if let Ok((puzzle_node, solution_node)) =
            get_puzzle_and_solution_for_coin(&allocator, generator_output, &backrefs, &removal)
        {
            let puzzle_bytes = node_to_bytes(&allocator, puzzle_node).unwrap_or_default();
            let solution_bytes = node_to_bytes(&allocator, solution_node).unwrap_or_default();

            // Run puzzle(solution) to extract conditions.
            let conditions = match clvmr::run_program::run_program(
                &mut allocator,
                &dialect,
                puzzle_node,
                solution_node,
                constants.max_block_cost_clvm,
            ) {
                Ok(clvmr::reduction::Reduction(_, output)) => parse_conditions(&allocator, output),
                Err(_) => Vec::new(),
            };

            result.push(CoinSpendWithConditions {
                coin_spend: CoinSpend {
                    coin: Coin::from_protocol(&removal),
                    puzzle_reveal: format!("0x{}", hex::encode(&puzzle_bytes)),
                    solution: format!("0x{}", hex::encode(&solution_bytes)),
                },
                conditions,
            });
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Parse raw CLVM conditions output into our Condition type
// ---------------------------------------------------------------------------

/// The output of `run_program(puzzle, solution)` is a list of conditions.
/// Each condition is `(opcode . (arg1 arg2 ...))`.
fn parse_conditions(allocator: &clvmr::Allocator, output: clvmr::NodePtr) -> Vec<Condition> {
    parse_conditions_public(allocator, output)
}

/// Public version of condition parsing for use from `router.rs`.
pub fn parse_conditions_public(
    allocator: &clvmr::Allocator,
    mut output: clvmr::NodePtr,
) -> Vec<Condition> {
    let mut conditions = Vec::new();

    while let Ok(Some((cond, rest))) = validation_error::next(allocator, output) {
        output = rest;

        // cond = (opcode . args_list)
        let Ok(opcode_node) = validation_error::first(allocator, cond) else {
            continue;
        };

        let opcode_bytes = match allocator.sexp(opcode_node) {
            clvmr::allocator::SExp::Atom => allocator.atom(opcode_node).as_ref().to_vec(),
            clvmr::allocator::SExp::Pair(_, _) => continue,
        };
        let opcode = serde_json::Value::String(format!("0x{}", hex::encode(&opcode_bytes)));

        // Collect args
        let mut vars = Vec::new();
        let Ok(mut args_iter) = validation_error::rest(allocator, cond) else {
            continue;
        };

        while let Ok(Some((arg, rest))) = validation_error::next(allocator, args_iter) {
            args_iter = rest;
            // Args can be atoms or pairs (e.g., hint lists).
            // Serialize pairs as CLVM bytes for fidelity.
            let arg_hex = match allocator.sexp(arg) {
                clvmr::allocator::SExp::Atom => {
                    format!("0x{}", hex::encode(allocator.atom(arg).as_ref()))
                }
                clvmr::allocator::SExp::Pair(_, _) => {
                    match clvmr::serde::node_to_bytes(allocator, arg) {
                        Ok(bytes) => format!("0x{}", hex::encode(&bytes)),
                        Err(_) => continue,
                    }
                }
            };
            vars.push(arg_hex);
        }

        conditions.push(Condition { opcode, vars });
    }

    conditions
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reward_coins(block: &FullBlock, height: u32) -> Vec<CoinRecord> {
    let Some(ref ti) = block.transactions_info else {
        return Vec::new();
    };
    let timestamp = block_timestamp(block);
    ti.reward_claims_incorporated
        .iter()
        .map(|c| CoinRecord {
            coin: Coin::from_protocol(c),
            confirmed_block_index: height,
            spent_block_index: 0,
            spent: false,
            coinbase: true,
            timestamp,
        })
        .collect()
}

fn block_timestamp(block: &FullBlock) -> u64 {
    block
        .foliage_transaction_block
        .as_ref()
        .map(|ft| ft.timestamp)
        .unwrap_or(0)
}
