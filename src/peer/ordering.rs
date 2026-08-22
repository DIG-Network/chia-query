//! Dial ordering: IPv6 first, IPv4 only as a fallback (CLAUDE.md §5.2).
//!
//! Ordering is a pure function of the candidate set, so the policy is testable without a socket.
//! It is separated from [`super::connect`] for that reason alone — the dialler decides *whether* a
//! candidate is worth trying, this decides *in what order* the survivors are tried.

use std::collections::HashSet;
use std::net::SocketAddr;

use rand::seq::SliceRandom;

/// Order candidates IPv6-first, loopback-first within each family.
///
/// A stable partition, so whatever order the caller established between two addresses of the same
/// class is preserved. That is what lets [`candidate_order`] shuffle for load spreading and then
/// order for §5.2 without the ordering undoing the shuffle.
pub fn order_candidates(candidates: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut v6_loopback = Vec::new();
    let mut v6_rest = Vec::new();
    let mut v4_loopback = Vec::new();
    let mut v4_rest = Vec::new();

    for &addr in candidates {
        match addr {
            SocketAddr::V6(_) if addr.ip().is_loopback() => v6_loopback.push(addr),
            SocketAddr::V6(_) => v6_rest.push(addr),
            SocketAddr::V4(_) if addr.ip().is_loopback() => v4_loopback.push(addr),
            SocketAddr::V4(_) => v4_rest.push(addr),
        }
    }

    let mut ordered = Vec::with_capacity(candidates.len());
    ordered.extend(v6_loopback);
    ordered.extend(v6_rest);
    ordered.extend(v4_loopback);
    ordered.extend(v4_rest);
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
        SocketAddr::new(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, seg).into(), 8444)
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
    #[test]
    fn a_duplicate_address_is_removed_even_when_the_duplicates_are_not_adjacent() {
        let with_repeats = vec![v6(1), v4(1), v6(2), v6(1), v4(2), v4(1), v6(3), v6(1)];

        let ordered = candidate_order(&with_repeats);

        let distinct: HashSet<SocketAddr> = ordered.iter().copied().collect();
        assert_eq!(
            ordered.len(),
            distinct.len(),
            "a candidate must be offered at most once, however the duplicates were spread: {ordered:?}"
        );
        assert_eq!(
            distinct.len(),
            5,
            "the five distinct addresses must all survive: {ordered:?}"
        );
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

    /// IPv6 loopback precedes IPv4 loopback, so a co-resident node is reached over v6 first.
    #[test]
    fn ipv6_loopback_precedes_ipv4_loopback() {
        let v6_lo = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let v4_lo = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8444);
        assert_eq!(order_candidates(&[v4_lo, v6_lo]), vec![v6_lo, v4_lo]);
    }
}
