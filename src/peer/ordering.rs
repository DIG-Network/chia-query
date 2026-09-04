//! Dial ordering: IPv6 first, IPv4 only as a fallback (CLAUDE.md §5.2).
//!
//! Ordering is a pure function of the candidate set, so the policy is testable without a socket.
//! It is separated from [`super::connect`] for that reason alone — the dialler decides *whether* a
//! candidate is worth trying, this decides *in what order* the survivors are tried.

use std::collections::HashSet;
use std::net::SocketAddr;

use rand::seq::SliceRandom;

use super::plurality::default_max_peers;
use super::pool::DIAL_OVERSUBSCRIPTION;

/// The most discovered candidates a single fill round will consider.
///
/// **Derived, not chosen** — the same way [`default_max_peers`] is derived from `QUORUM_SAMPLE`
/// rather than pinned. A round can admit at most [`default_max_peers`] peers, and it deliberately
/// oversubscribes each slot by [`DIAL_OVERSUBSCRIPTION`] so attrition does not leave it short. A
/// candidate beyond that product cannot be dialled by the round that discovered it, so considering
/// it buys nothing and costs a timeout-bounded batch.
///
/// # Why an uncapped set is an attacker-controlled term in the round's duration
///
/// Discovery proceeds in sequential chunks of `connect::BATCH_SIZE`, each bounded separately by
/// `connect_timeout`, so a round costs
/// `(PRIORITY_SLOTS + 1 + ceil(N / BATCH_SIZE)) * connect_timeout` — and **`N` is decided by the
/// DNS introducer**, not by this host. A hostile or compromised introducer returning ten thousand
/// addresses lengthens every fill round without ever being dialled successfully, which is exactly
/// the trust model NC-12 assumes: the introducer is a stranger and may be lying.
///
/// Mainnet's introducer set is small (4 addresses, one chunk), so this is latent today. The cap
/// makes the term constant rather than attacker-supplied.
///
/// # Capping does not permanently exclude anyone
///
/// The truncation is applied AFTER the shuffle, so each round draws a fresh uniform sample, and a
/// fill makes `FILL_ROUNDS` of them. An address dropped by one round is as likely as any other to
/// appear in the next. Capping BEFORE the shuffle would instead let the introducer choose the
/// survivors by position, which hands it the very influence this bound removes.
pub const MAX_DISCOVERED_CANDIDATES: usize = default_max_peers() * DIAL_OVERSUBSCRIPTION;

/// Order candidates IPv6-first (§5.2).
///
/// A stable partition, so whatever order the caller established between two addresses of the same
/// family is preserved. That is what lets [`candidate_order`] shuffle for load spreading and then
/// order for §5.2 without the ordering undoing the shuffle.
///
/// **Locality is not a preference here.** An earlier version sorted loopback ahead of everything
/// else within each family, which put a DISCOVERED local address in front of every public peer —
/// and a discovered local address is precisely the one a local attacker can supply. Reaching a
/// co-resident node is the priority path's job
/// ([`connect::priority_addresses`](super::connect::priority_addresses)), where the peer is
/// recorded as `Priority` and is not counted as an independent voice; the discovered set has no
/// business preferring it (dig_ecosystem#2648).
pub fn order_candidates(candidates: &[SocketAddr]) -> Vec<SocketAddr> {
    let (v6, v4): (Vec<SocketAddr>, Vec<SocketAddr>) =
        candidates.iter().partition(|addr| addr.is_ipv6());

    let mut ordered = Vec::with_capacity(candidates.len());
    ordered.extend(v6);
    ordered.extend(v4);
    ordered
}

/// The full dial order for a discovered candidate set: distinct, spread, then IPv6-first.
///
/// The three steps run in this order for one reason each, and the FIRST is a fix:
///
/// 1. **Distinct.** Both rival diallers shuffled and then called `Vec::dedup`, which removes only
///    ADJACENT duplicates — so shuffling immediately beforehand made the deduplication very nearly
///    a no-op, and a repeated introducer result could occupy two dial slots. Distinctness is
///    decided by a set, which does not care what order the input arrived in.
/// 2. **Shuffle**, so this node does not hammer whichever peer DNS happened to return first.
/// 3. **Cap** at [`MAX_DISCOVERED_CANDIDATES`], so the number of addresses a DNS introducer
///    returns is not a term in how long a fill round takes. Deliberately AFTER the shuffle: the
///    survivors are then a uniform sample rather than whichever addresses the introducer chose to
///    list first.
/// 4. **Order**, IPv6 before IPv4 (§5.2). Stable, so the shuffle survives inside each class.
pub fn candidate_order(discovered: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut seen = HashSet::with_capacity(discovered.len());
    let mut distinct: Vec<SocketAddr> = discovered
        .iter()
        .copied()
        .filter(|addr| seen.insert(*addr))
        .collect();

    distinct.shuffle(&mut rand::thread_rng());
    distinct.truncate(MAX_DISCOVERED_CANDIDATES);
    order_candidates(&distinct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(last: u8) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::new(203, 0, 113, last).into(), 8444)
    }

    fn v6(seg: u16) -> SocketAddr {
        SocketAddr::new(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, seg).into(),
            8444,
        )
    }

    /// §5.2: every IPv6 candidate is dialled before any IPv4 one.
    ///
    /// The fixture MIXES the two families and interleaves them, so an implementation that merely
    /// returned its input — the behaviour being replaced — fails on the very first position.
    #[test]
    fn every_ipv6_candidate_is_dialled_before_any_ipv4_one() {
        let mixed = vec![v4(1), v6(1), v4(2), v6(2), v4(3)];
        let ordered = candidate_order(&mixed);

        let first_v4 = ordered
            .iter()
            .position(|a| a.is_ipv4())
            .expect("the fixture holds IPv4 candidates");
        let last_v6 = ordered
            .iter()
            .rposition(|a| a.is_ipv6())
            .expect("the fixture holds IPv6 candidates");
        assert!(
            last_v6 < first_v4,
            "IPv6 must be preferred over IPv4 (§5.2): {ordered:?}"
        );
    }

    /// **The bug both rival diallers shared: `shuffle()` then `dedup()`.**
    ///
    /// `Vec::dedup` collapses only ADJACENT equal elements, so a set shuffled first keeps its
    /// duplicates with high probability. The fixture therefore places the duplicates NON-ADJACENT
    /// with distinct addresses between them — against `shuffle(); dedup()` this test can actually
    /// fail, which a fixture like `[a, a, b]` could not, because there the duplicate pair is
    /// adjacent before the shuffle and often still adjacent after it.
    ///
    /// Both families are duplicated so the ordering step cannot be what removes them.
    ///
    /// **Repeated, because the input to `dedup` is RANDOM and a proof must not be.** The property
    /// asserted is exact — a set cannot hold a duplicate — but the proof that it is load-bearing
    /// rests on `shuffle(); dedup()` actually failing, and that behaviour fails only when the
    /// shuffle leaves some duplicate pair non-adjacent. A single call therefore lets the replaced
    /// implementation through roughly one run in twenty-five, which is a flaky red waiting to
    /// happen and, worse, a revert-proof that can report a fix as unnecessary. Each call shuffles
    /// independently, so `ROUNDS` calls drive that below 1e-40 without seeding production code.
    #[test]
    fn a_duplicate_address_is_removed_even_when_the_duplicates_are_not_adjacent() {
        const ROUNDS: usize = 32;
        let with_repeats = vec![v6(1), v4(1), v6(2), v6(1), v4(2), v4(1), v6(3), v6(1)];

        for round in 0..ROUNDS {
            let ordered = candidate_order(&with_repeats);

            let distinct: HashSet<SocketAddr> = ordered.iter().copied().collect();
            assert_eq!(
                ordered.len(),
                distinct.len(),
                "round {round}: a candidate must be offered at most once, however the duplicates                  were spread: {ordered:?}"
            );
            assert_eq!(
                distinct.len(),
                5,
                "round {round}: the five distinct addresses must all survive: {ordered:?}"
            );
        }
    }

    /// The control: deduplication must not COST a candidate. Without it, returning a single
    /// address — or an empty vector — would satisfy the test above.
    #[test]
    fn distinct_candidates_all_survive_ordering() {
        let all_distinct = vec![v6(1), v6(2), v4(1), v4(2)];
        let ordered = candidate_order(&all_distinct);
        assert_eq!(ordered.len(), 4);
        assert_eq!(
            ordered.iter().copied().collect::<HashSet<_>>(),
            all_distinct.iter().copied().collect::<HashSet<_>>()
        );
    }

    /// The ordering is by FAMILY alone: a local address gets no head start over a public one.
    ///
    /// The fixture puts the loopback address LAST within its family, so the behaviour being
    /// replaced — loopback first within each family — moves it and fails here. Both families carry
    /// one, so a fix applied to only one of them is visible too.
    #[test]
    fn a_local_address_is_not_preferred_over_a_public_one_of_the_same_family() {
        let v6_lo = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let v4_lo = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8444);

        assert_eq!(
            order_candidates(&[v6(1), v6_lo, v4(1), v4_lo]),
            vec![v6(1), v6_lo, v4(1), v4_lo],
            "family orders the candidates; locality must not reorder them"
        );
    }

    /// IPv6 still precedes IPv4 when both are loopback (§5.2 applies to every pair).
    #[test]
    fn ipv6_precedes_ipv4_for_loopback_too() {
        let v6_lo = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let v4_lo = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8444);
        assert_eq!(order_candidates(&[v4_lo, v6_lo]), vec![v6_lo, v4_lo]);
    }

    /// **Proves (#45):** an oversized discovered set is capped, so a hostile introducer cannot
    /// lengthen a fill round by returning more addresses.
    ///
    /// Goes RED without the truncate: the round would consider all 500 and walk
    /// `ceil(500 / BATCH_SIZE)` timeout-bounded chunks instead of a constant number.
    #[test]
    fn an_oversized_discovered_set_is_capped() {
        let flood: Vec<SocketAddr> = (0..500u16).map(v6).collect();
        let ordered = candidate_order(&flood);

        assert_eq!(
            ordered.len(),
            MAX_DISCOVERED_CANDIDATES,
            "a round must consider a bounded number of candidates, not whatever DNS returned"
        );
    }

    /// **Control:** a set at or under the cap is passed through untouched.
    ///
    /// Without this, a cap that returned a constant — or truncated everything to zero — would
    /// still satisfy the test above while destroying ordinary discovery. Mainnet's introducer set
    /// is 4 addresses, so the normal case must be provably unaffected.
    #[test]
    fn a_set_within_the_cap_is_not_truncated() {
        for n in [1usize, 4, MAX_DISCOVERED_CANDIDATES] {
            let modest: Vec<SocketAddr> = (0..n as u16).map(v6).collect();
            assert_eq!(
                candidate_order(&modest).len(),
                n,
                "a set of {n} is within the cap and must survive whole"
            );
        }
    }

    /// **Proves (#45):** the cap is applied AFTER the shuffle, so the introducer cannot choose the
    /// survivors by listing them first.
    ///
    /// This is the half that makes the bound worth having. A truncate applied BEFORE the shuffle
    /// passes the size assertion above just as well, while handing a hostile introducer complete
    /// control over which addresses this host ever dials — a strictly worse position than the
    /// uncapped set it replaced, because the attacker no longer has to out-number the honest
    /// entries, only out-rank them.
    ///
    /// **Repeated, because the shuffle is RANDOM and a proof must not be.** One draw could place a
    /// tail address in the survivors by luck even under a cap-first implementation. Over many
    /// draws, cap-first can NEVER surface a tail address, so the assertion separates the two
    /// implementations rather than merely observing one run.
    #[test]
    fn the_cap_samples_uniformly_rather_than_taking_the_head_of_the_list() {
        // Deliberately one family, so the IPv6-first ordering cannot be what moves anything.
        let listed: Vec<SocketAddr> = (0..200u16).map(v6).collect();
        let tail = &listed[MAX_DISCOVERED_CANDIDATES..];

        let surfaced_a_tail_address = (0..200).any(|_| {
            let ordered = candidate_order(&listed);
            ordered.iter().any(|addr| tail.contains(addr))
        });

        assert!(
            surfaced_a_tail_address,
            "no address beyond the first {MAX_DISCOVERED_CANDIDATES} was EVER dialled across 200              draws, so the cap is taking the head of the introducer's list rather than a sample"
        );
    }

    /// **Proves (#45):** §5.2 still holds after the cap.
    ///
    /// The truncate runs before [`order_candidates`], so IPv6-first must survive it. A cap applied
    /// after the ordering would instead be able to discard every IPv4 candidate whenever enough
    /// IPv6 ones existed — a different policy, silently.
    #[test]
    fn the_cap_preserves_the_ipv6_first_ordering() {
        let mut mixed: Vec<SocketAddr> = (0..100u16).map(v6).collect();
        mixed.extend((0..100u8).map(v4));

        let ordered = candidate_order(&mixed);
        assert_eq!(ordered.len(), MAX_DISCOVERED_CANDIDATES);

        if let (Some(first_v4), Some(last_v6)) = (
            ordered.iter().position(|a| a.is_ipv4()),
            ordered.iter().rposition(|a| a.is_ipv6()),
        ) {
            assert!(
                last_v6 < first_v4,
                "IPv6 must still precede IPv4 after the cap (§5.2): {ordered:?}"
            );
        }
    }
}
