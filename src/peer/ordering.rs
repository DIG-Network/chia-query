//! Dial ordering: IPv6 first, IPv4 only as a fallback (CLAUDE.md §5.2).
//!
//! Ordering is a pure function of the candidate set, so the policy is testable without a socket.
//! It is separated from [`super::connect`] for that reason alone — the dialler decides *whether* a
//! candidate is worth trying, this decides *in what order* the survivors are tried.

use std::collections::HashSet;
use std::net::SocketAddr;

use rand::seq::SliceRandom;

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
/// 3. **Order**, IPv6 before IPv4 (§5.2). Stable, so the shuffle survives inside each class.
pub fn candidate_order(discovered: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut seen = HashSet::with_capacity(discovered.len());
    let mut distinct: Vec<SocketAddr> = discovered
        .iter()
        .copied()
        .filter(|addr| seen.insert(*addr))
        .collect();

    distinct.shuffle(&mut rand::thread_rng());
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
}
