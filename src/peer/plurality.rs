//! How many independent voices the pool must hold, and how long it may hold the same ones.
//!
//! These are the sizing half of NC-12: peers that are UNTRUSTED, plural enough that agreement
//! between them means something, and CYCLED so that a set an attacker captured once does not
//! decide this node's view of the chain for the life of the process.
//!
//! They live beside the pool because the pool is what has to satisfy them. A consumer counting
//! agreeing answers cannot make a pool large enough after the fact.

use std::time::Duration;

/// The sample size an agreement ratio is expressed against: four distinct voices.
///
/// Fewer than three cannot express "a majority with one dissenter" at all — with two peers every
/// disagreement is a bare split that says nothing about which side is anomalous. Three leaves no
/// margin: one peer mid-reorg drops the round to a 2-1 that only just clears a supermajority, and
/// a second stalls it entirely. Four costs one more handshake and tolerates one dissenter.
///
/// It is deliberately not larger: every extra member widens the window in which an attacker
/// holding a slice of the discoverable set lands two members in one sample.
pub const QUORUM_SAMPLE: usize = 4;

/// The fewest INDEPENDENT peers, besides the one that answered, that must be available before a
/// corroborated read may be attempted at all.
///
/// Two, because one corroborator is a single second opinion — enough to catch a peer that is
/// simply wrong, not enough to survive one that is lying while another is unreachable.
///
/// The gate this feeds REFUSES rather than degrading (see
/// [`PeerPool::corroboration_readiness`](super::pool::PeerPool::corroboration_readiness)). A pool
/// that quietly corroborates against whoever happens to be present converts a four-voice quorum
/// into a three- or two-voice one while still reporting the answer as corroborated, and nothing
/// downstream can tell the difference.
pub const CORROBORATION_FLOOR: usize = 2;

/// How long a peer may stay in the pool before it is rotated out.
///
/// Five minutes: long enough that a read and the walk behind it complete over one set of
/// connections, short enough that a captured set does not decide this node's view of the chain for
/// the life of the process. This is NC-12's cycling half; the corroboration floor above is its
/// plurality half, and neither substitutes for the other.
pub const PEER_LIFETIME: Duration = Duration::from_secs(300);

/// The pool size that leaves [`QUORUM_SAMPLE`] independent voices standing in the normal case.
///
/// Derived, not chosen. Every term below is a slot that is occupied and is NOT an independent
/// corroborating voice:
///
/// 1. **One priority slot.** The dialler tries `TRUSTED_FULLNODE` and the loopback ahead of
///    discovery, so a priority peer is the ordinary case rather than the exception — and a
///    co-resident node is precisely the source a local attacker can supply, so it is counted as a
///    peer to ASK and never as an independent voice (dig_ecosystem#2648).
/// 2. **One slot for the session a subscriber is following.** The wallet replica holds a session
///    for its own frames; the peer it is reading from cannot corroborate itself.
/// 3. **[`QUORUM_SAMPLE`] independent voices** — the sample an agreement ratio is expressed
///    against.
/// 4. **One slot of slack**, so that losing a single peer to attrition, or having one out of the
///    pool mid-rotation, does not immediately drop the sample below its floor. Without it, cycling
///    — which necessarily removes a peer before its replacement connects — would itself be enough
///    to disarm corroboration.
///
/// The previous default of 5 left, in the normal case of one priority entry, **three** usable
/// corroborators — below [`QUORUM_SAMPLE`]. That is the exact shape of a silent regression: a
/// four-voice quorum becomes a three-voice one that still reports itself corroborated.
pub const fn default_max_peers() -> usize {
    1 + 1 + QUORUM_SAMPLE + 1
}

const _: () = assert!(
    default_max_peers() >= QUORUM_SAMPLE + CORROBORATION_FLOOR,
    "the pool must be able to hold a full sample and still clear the corroboration floor"
);

#[cfg(test)]
mod tests {
    use super::*;

    /// The derivation, pinned from BOTH sides: the number is 7, and it is 7 *because* of the terms
    /// above rather than by coincidence.
    ///
    /// Pinning only the literal would let a change to `QUORUM_SAMPLE` silently stop being covered;
    /// pinning only the formula would let the formula be rewritten to whatever the code says.
    #[test]
    fn the_default_pool_size_is_derived_from_the_sample_it_must_leave_standing() {
        assert_eq!(default_max_peers(), 7);
        assert_eq!(
            default_max_peers(),
            1 + 1 + QUORUM_SAMPLE + 1,
            "one priority slot, one followed session, the sample, and one of slack"
        );
    }

    /// The property the derivation exists for: with a priority peer and a followed session taken
    /// out, a full pool still leaves a whole sample.
    #[test]
    fn a_full_pool_leaves_a_whole_sample_after_the_priority_and_followed_slots() {
        let independent_after_priority = default_max_peers() - 1;
        let corroborating = independent_after_priority - 1;
        assert!(
            corroborating >= QUORUM_SAMPLE,
            "{corroborating} corroborators is below the sample of {QUORUM_SAMPLE}"
        );
    }

    /// **The bound from the other side: 6 would not have been enough.**
    ///
    /// Judged mid-rotation, which is the state the slack slot exists for — cycling necessarily
    /// removes a peer before its replacement connects, so one slot is vacant. A pool of 6 leaves
    /// three corroborators there, below the sample; a pool of 7 leaves four. Without this the
    /// slack term would be unfalsifiable padding.
    #[test]
    fn one_smaller_would_not_survive_a_rotation() {
        let corroborators_mid_rotation = |size: usize| size - 1 /* vacant */ - 1 /* priority */ - 1 /* followed */;

        assert!(
            corroborators_mid_rotation(default_max_peers()) >= QUORUM_SAMPLE,
            "the shipped size must hold a whole sample even mid-rotation"
        );
        assert!(
            corroborators_mid_rotation(default_max_peers() - 1) < QUORUM_SAMPLE,
            "if one smaller sufficed, the slack slot would be unjustified"
        );
    }
}
