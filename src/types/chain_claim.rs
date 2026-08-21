//! What part of an answer is a claim about the CHAIN, and therefore has to be agreed on.
//!
//! A positive answer is not self-verifying. The coin-id binding — `SHA256(parent_coin_info ‖
//! puzzle_hash ‖ amount)` — authenticates the coin's *identity*, and nothing else on the record:
//! `created_height`, `spent_height` and `spent` are copied verbatim from whatever an anonymous
//! peer sent, and those are the fields a consumer reads to decide whether money is settled
//! (dig_ecosystem#2462).
//!
//! So corroboration compares the claim, not the struct. Two sources describing the same chain
//! state must compare EQUAL even though they filled in different amounts of local colour: the peer
//! protocol cannot supply `timestamp` or `coinbase` at all and leaves them zeroed, while the
//! coinset API returns both. Comparing whole records would make every peer/coinset pair
//! "disagree" and turn the corroboration into a permanent error.

use crate::types::{CoinRecord, CoinSpend};

/// The chain-state claim inside an answer, rendered as a value two sources can be compared on.
///
/// Implement this for anything that travels through a corroborated read. The implementation MUST
/// include every field a consumer could read as evidence about the chain, and MUST exclude fields
/// that merely vary by which tier answered — a claim that includes tier-local colour cannot be
/// corroborated across tiers, and one that omits a height cannot catch the fabrication this trait
/// exists to catch.
pub trait ChainClaim {
    /// The claim, canonically rendered. Equal strings mean two sources asserted the same thing
    /// about the chain.
    fn chain_claim(&self) -> String;
}

impl ChainClaim for CoinRecord {
    /// The coin's identity plus the three fields that say where it sits on the chain.
    ///
    /// `timestamp` and `coinbase` are deliberately absent: the peer protocol carries neither, so
    /// including them would make a peer answer and a coinset answer about the same coin
    /// unconditionally unequal.
    fn chain_claim(&self) -> String {
        format!(
            "coin({},{},{}) created={} spent={}",
            self.coin.parent_coin_info,
            self.coin.puzzle_hash,
            self.coin.amount,
            self.confirmed_block_index,
            if self.spent {
                self.spent_block_index.to_string()
            } else {
                "no".to_string()
            },
        )
    }
}

impl ChainClaim for CoinSpend {
    /// A spend is claimed by the coin it spent and the program bytes that spent it.
    fn chain_claim(&self) -> String {
        format!(
            "spend({},{},{}) puzzle={} solution={}",
            self.coin.parent_coin_info,
            self.coin.puzzle_hash,
            self.coin.amount,
            self.puzzle_reveal,
            self.solution,
        )
    }
}
