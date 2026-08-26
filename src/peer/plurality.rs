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
/// **This counts AGREEING ANSWERS, and it is also the number of independent peers that must be
/// HELD before the read is attempted.** The distinction matters across the crate boundary:
/// `dig-node`'s `sage::quorum` uses the same name and the same value for answers received in one
/// round only. Held is the weaker of the two, because a held connection may have died silently
/// since it was last used, so a pool that clears this floor has not yet shown that this many peers
/// will ANSWER. Anything adopting both must not let one stand in for the other — see
/// [`PeerPool::corroboration_readiness`](super::pool::PeerPool::corroboration_readiness), which
/// answers the held question, and `corroborate_presence`, which answers the agreeing one.
///
/// The gate this feeds REFUSES rather than degrading (see
/// [`PeerPool::corroboration_readiness`](super::pool::PeerPool::corroboration_readiness)). A pool
/// that quietly corroborates against whoever happens to be present converts a four-voice quorum
/// into a three- or two-voice one while still reporting the answer as corroborated, and nothing
/// downstream can tell the difference.
///
/// Both denominators are recorded ecosystem-wide in the superproject `canonical` skill, under
/// `CORROBORATION_FLOOR`, which names the HELD and the ANSWERED counts separately and forbids
/// re-exporting either as the other. Read it before adopting this constant in another repo.
pub const CORROBORATION_FLOOR: usize = 2;

/// How long a peer may stay in the pool before it is rotated out.
///
/// Five minutes: long enough that a read and the walk behind it complete over one set of
/// connections, short enough that a captured set does not decide this node's view of the chain for
/// the life of the process. This is NC-12's cycling half; the corroboration floor above is its
/// plurality half, and neither substitutes for the other.
pub const PEER_LIFETIME: Duration = Duration::from_secs(300);

/// How far a peer's announced peak may trail the pool's reference peak and still be CURRENT.
///
/// Three blocks, matching the bar Sage applies when it decides whether two peers are talking about
/// the same chain. This is the canonical home for that number: dig-node applies the same tolerance
/// inside a corroboration round, and two crates each carrying their own `3` is a drift waiting to
/// happen, so a consumer adopts THIS rather than restating it.
pub const PEAK_LAG_TOLERANCE: u32 = 3;

/// How far a peer must trail the reference peak before the pool stops HOLDING it.
///
/// Twice [`PEAK_LAG_TOLERANCE`], and derived from it rather than chosen: a peer just outside the
/// round tolerance may simply be one propagation hop late, and evicting on that would spend a
/// handshake on ordinary jitter. At twice the tolerance the peer has missed roughly six blocks —
/// about two minutes of mainnet — which is not jitter; it is a peer that is no longer following.
///
/// Membership and round tolerance are deliberately different bars for the same reason they are
/// derived from one constant: a lagging peer's ANSWER must be excluded immediately, while its
/// CONNECTION is worth keeping a little longer in case it catches up.
pub const PEAK_LAG_EVICTION: u32 = 2 * PEAK_LAG_TOLERANCE;

/// How many pool slots the PRIORITY path can occupy.
///
/// Two, because the dialler tries two priority addresses before discovery — an operator's
/// `TRUSTED_FULLNODE` and the loopback — and they are distinct `SocketAddr`s, so on a host running
/// both, BOTH are admitted, and both are admitted as
/// [`PeerOrigin::Priority`](super::connect::PeerOrigin::Priority).
///
/// A priority peer is an excellent peer to ASK and is never an independent voice: a co-resident
/// node is precisely the source a local attacker can supply (dig_ecosystem#2648). So each of these
/// slots is occupied by a connection that cannot corroborate anything, which is why the pool is
/// sized around them.
///
/// It is measured against the dialler rather than asserted — see
/// [`connect::priority_addresses_from`](super::connect::priority_addresses_from) and the test
/// below. Sizing the pool for ONE priority slot while the dialler offered two is what left a full
/// pool three corroborators mid-rotation, below [`QUORUM_SAMPLE`].
pub const PRIORITY_SLOTS: usize = 2;

/// The pool size that leaves [`QUORUM_SAMPLE`] independent voices standing in the normal case.
///
/// Derived, not chosen. Every term below is a slot that is occupied and is NOT an independent
/// corroborating voice:
///
/// 1. **[`PRIORITY_SLOTS`] priority slots**, the addresses tried ahead of discovery.
/// 2. **One slot for the session a subscriber is following.** The wallet replica holds a session
///    for its own frames; the peer it is reading from cannot corroborate itself.
/// 3. **[`QUORUM_SAMPLE`] independent voices** — the sample an agreement ratio is expressed
///    against.
/// 4. **One slot of slack**, so that losing a single peer to attrition, or having one out of the
///    pool mid-rotation, does not immediately drop the sample below its floor. Without it, cycling
///    — which necessarily removes a peer before its replacement connects — would itself be enough
///    to disarm corroboration.
///
/// The previous default of 5 left, in the normal case, **three** usable corroborators — below
/// [`QUORUM_SAMPLE`]. That is the exact shape of a silent regression: a four-voice quorum becomes
/// a three-voice one that still reports itself corroborated. The default of 7 that replaced it
/// counted only ONE priority slot and had the same defect one host short of the worst case.
pub const fn default_max_peers() -> usize {
    PRIORITY_SLOTS + 1 + QUORUM_SAMPLE + 1
}

const _: () = assert!(
    default_max_peers() >= QUORUM_SAMPLE + CORROBORATION_FLOOR,
    "the pool must be able to hold a full sample and still clear the corroboration floor"
);

#[cfg(test)]
mod tests {
    use super::*;

    /// **`PRIORITY_SLOTS` is MEASURED against the dialler, not restated.**
    ///
    /// The number of slots the priority path can occupy is a property of
    /// [`connect::priority_addresses_from`](super::super::connect::priority_addresses_from), so it
    /// is obtained from that function on the worst case it actually produces: an operator who has
    /// configured `TRUSTED_FULLNODE` and is also running a node on this machine. Both are returned,
    /// both are distinct socket addresses, and the pool admits both as `Priority`.
    ///
    /// A test that wrote the number down instead — as the version this replaces did, with a
    /// `- 1 /* priority */` inside a closure — cannot notice the dialler gaining an address, which
    /// is the drift that produced the wrong default.
    #[test]
    fn the_priority_slot_count_is_measured_against_the_dialler() {
        use crate::peer::connect::{priority_addresses_from, MAINNET_PORT};

        let worst_case = priority_addresses_from(Some("203.0.113.5"), MAINNET_PORT, &[]);

        assert_eq!(
            worst_case.len(),
            PRIORITY_SLOTS,
            "the pool is sized for {PRIORITY_SLOTS} priority slots but the dialler offers \
             {}: {worst_case:?}",
            worst_case.len()
        );
        assert_eq!(
            worst_case
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            worst_case.len(),
            "distinct addresses, so each one occupies a slot of its own"
        );
    }

    /// The control: with no `TRUSTED_FULLNODE` configured the priority path still occupies a slot.
    ///
    /// Without it, a `priority_addresses_from` that returned an empty list for the ordinary case
    /// would leave the measurement above satisfied by a function nobody could use.
    #[test]
    fn the_loopback_occupies_a_priority_slot_even_with_nothing_configured() {
        use crate::peer::connect::{priority_addresses_from, MAINNET_PORT};

        assert_eq!(priority_addresses_from(None, MAINNET_PORT, &[]).len(), 1);
    }

    /// The derivation, pinned from BOTH sides: the number is 8, and it is 8 *because* of the terms
    /// above rather than by coincidence.
    ///
    /// Pinning only the literal would let a change to `QUORUM_SAMPLE` silently stop being covered;
    /// pinning only the formula would let the formula be rewritten to whatever the code says.
    #[test]
    fn the_default_pool_size_is_derived_from_the_sample_it_must_leave_standing() {
        assert_eq!(default_max_peers(), 8);
        assert_eq!(
            default_max_peers(),
            PRIORITY_SLOTS + 1 + QUORUM_SAMPLE + 1,
            "the priority slots, one followed session, the sample, and one of slack"
        );
    }

    /// The property the derivation exists for: with the priority slots and a followed session
    /// taken out, a full pool still leaves a whole sample.
    #[test]
    fn a_full_pool_leaves_a_whole_sample_after_the_priority_and_followed_slots() {
        let corroborating = default_max_peers() - PRIORITY_SLOTS - 1;
        assert!(
            corroborating >= QUORUM_SAMPLE,
            "{corroborating} corroborators is below the sample of {QUORUM_SAMPLE}"
        );
    }

    /// **The bound from the other side: one smaller would not have been enough.**
    ///
    /// Judged mid-rotation, which is the state the slack slot exists for — cycling necessarily
    /// removes a peer before its replacement connects, so one slot is vacant. Without this the
    /// slack term would be unfalsifiable padding.
    ///
    /// The subtraction reads `PRIORITY_SLOTS` rather than a literal, so a dialler that grows a
    /// third priority address moves this test rather than leaving it agreeing with itself.
    #[test]
    fn one_smaller_would_not_survive_a_rotation() {
        let corroborators_mid_rotation =
            |size: usize| size - 1 /* vacant */ - PRIORITY_SLOTS - 1 /* followed */;

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
