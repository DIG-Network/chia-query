//! Admission tests for [`PeerPoolInner`] (dig_ecosystem#2648).
//!
//! Every assertion is on the pool's REALIZED contents — which addresses it ended up holding —
//! never on "no error was returned". The collapsed pool this fixes returned no error at all; it
//! reported itself full and healthy while holding five sockets to one process.
//!
//! The peer stand-in is what makes these tests possible offline, and it is honest here because
//! the admission rule keys on `SocketAddr` alone and never touches the peer type.

use super::*;

use std::sync::Mutex;

use chia_protocol::Bytes32;
use chia_traits::Streamable;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A peer stand-in. Carries its address so a test can prove WHICH connection was admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FakePeer {
    addr: SocketAddr,
}

/// A dialer that answers only for addresses in its book, and records every connection it
/// handed out — including the ones the pool went on to reject.
struct FakeDialer {
    /// Addresses that answer. Anything else fails to connect.
    reachable: HashSet<SocketAddr>,
    /// Every dial that succeeded, in order, with the sender for that connection.
    handed_out: Mutex<Vec<(SocketAddr, mpsc::Sender<Message>)>>,
    /// Holds dials to this address until two of them have arrived.
    ///
    /// The pool skips an address it already holds BEFORE dialling, so the only way a duplicate
    /// connection is ever opened is two fills dialling concurrently and one losing the
    /// write-lock re-check. That race is what this makes deterministic.
    contended: Option<(SocketAddr, Arc<tokio::sync::Barrier>)>,
}

impl FakeDialer {
    fn reaching(addrs: &[SocketAddr]) -> Arc<Self> {
        Arc::new(Self {
            reachable: addrs.iter().copied().collect(),
            handed_out: Mutex::new(Vec::new()),
            contended: None,
        })
    }

    /// As [`reaching`](Self::reaching), but dials to `addr` block until two have arrived.
    fn reaching_with_contention(addrs: &[SocketAddr], addr: SocketAddr) -> Arc<Self> {
        Arc::new(Self {
            reachable: addrs.iter().copied().collect(),
            handed_out: Mutex::new(Vec::new()),
            contended: Some((addr, Arc::new(tokio::sync::Barrier::new(2)))),
        })
    }

    /// The senders handed out for `addr`, in dial order.
    fn senders_for(&self, addr: SocketAddr) -> Vec<mpsc::Sender<Message>> {
        self.handed_out
            .lock()
            .expect("dial log is not poisoned")
            .iter()
            .filter(|(a, _)| *a == addr)
            .map(|(_, s)| s.clone())
            .collect()
    }

    fn dial_count(&self) -> usize {
        self.handed_out
            .lock()
            .expect("dial log is not poisoned")
            .len()
    }
}

impl PeerDialer<FakePeer> for FakeDialer {
    fn dial(&self, addr: SocketAddr) -> DialFuture<FakePeer> {
        if !self.reachable.contains(&addr) {
            return Box::pin(async move {
                Err(ChiaQueryError::PeerConnection(format!(
                    "{addr} unreachable"
                )))
            });
        }
        let (tx, rx) = mpsc::channel(8);
        self.handed_out
            .lock()
            .expect("dial log is not poisoned")
            .push((addr, tx));
        let peer = FakePeer { addr };
        let barrier = self
            .contended
            .as_ref()
            .filter(|(contended, _)| *contended == addr)
            .map(|(_, barrier)| Arc::clone(barrier));
        Box::pin(async move {
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
            Ok((peer, rx))
        })
    }
}

fn addr(n: u8) -> SocketAddr {
    format!("10.0.0.{n}:8444")
        .parse()
        .expect("a valid socket address")
}

fn book(count: u8) -> Vec<SocketAddr> {
    (1..=count).map(addr).collect()
}

/// A pool over `candidates`, reaching everything in `reachable`.
fn pool_over(
    candidates: Vec<SocketAddr>,
    reachable: &[SocketAddr],
    target: usize,
) -> (PeerPoolInner<FakePeer>, Arc<FakeDialer>) {
    let dialer = FakeDialer::reaching(reachable);
    let pool = PeerPoolInner::with_dialer(
        Arc::clone(&dialer) as Arc<dyn PeerDialer<FakePeer>>,
        target,
        Vec::new(),
        NetworkType::Mainnet,
        Duration::from_millis(1),
        Some(candidates),
    );
    (pool, dialer)
}

async fn held_addresses(pool: &PeerPoolInner<FakePeer>) -> Vec<SocketAddr> {
    pool.peer_members().await.iter().map(|m| m.addr).collect()
}

/// Encode a real `NewPeakWallet` onto the wire and hand it to a member's channel.
///
/// Deliberately goes through `to_bytes`/`from_bytes` rather than a hand-built struct: the
/// handler's job includes decoding, and a fixture that skips the Streamable round-trip cannot
/// see a decode that never happens.
async fn announce_peak(sender: &mpsc::Sender<Message>, height: u32, weight: u128) -> Bytes32 {
    let header_hash = Bytes32::new([height as u8; 32]);
    let peak = NewPeakWallet {
        header_hash,
        height,
        weight,
        fork_point_with_previous_peak: 0,
    };
    let message = Message {
        msg_type: ProtocolMessageTypes::NewPeakWallet,
        id: None,
        data: peak.to_bytes().expect("NewPeakWallet encodes").into(),
    };
    sender.send(message).await.expect("member channel is open");
    header_hash
}

/// Await a condition the background handler satisfies, failing at the deadline rather than
/// sleeping blind for a fixed interval.
async fn wait_until<F, Fut>(what: &str, mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if predicate().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

/// #2648, the core defect: one address offered for every slot fills exactly ONE.
///
/// Before the fix, five independent `connect_random_peer` calls each returned the one address
/// that answered and the pool pushed all five unconditionally.
#[tokio::test]
async fn one_address_offered_repeatedly_yields_one_member() {
    let squatter = addr(1);
    let (pool, _dialer) = pool_over(vec![squatter; 5], &[squatter], 5);

    assert_eq!(pool.fill().await, 1);
    assert_eq!(held_addresses(&pool).await, vec![squatter]);
}

/// The same collapse in its realistic shape: the candidate list is full of real peers, but only
/// the co-resident process answers.
#[tokio::test]
async fn a_single_reachable_address_cannot_occupy_more_than_one_slot() {
    let squatter = addr(1);
    let (pool, dialer) = pool_over(book(8), &[squatter], 5);

    assert_eq!(pool.fill().await, 1);
    assert_eq!(held_addresses(&pool).await, vec![squatter]);
    // Every other candidate was genuinely attempted, so the shortfall is the network's, not a
    // premature stop.
    assert_eq!(
        dialer.dial_count(),
        1,
        "only the reachable address connects"
    );
}

#[tokio::test]
async fn distinct_reachable_addresses_fill_to_target() {
    let reachable = book(8);
    let (pool, _dialer) = pool_over(reachable.clone(), &reachable, 5);

    assert_eq!(pool.fill().await, 5);
    let held = held_addresses(&pool).await;
    assert_eq!(held.len(), 5);
    assert_eq!(held.iter().collect::<HashSet<_>>().len(), 5);
}

/// A refill after an eviction must reach a genuinely NEW address, not re-admit the one just
/// ejected and not double up on a survivor.
#[tokio::test]
async fn pool_refills_to_target_after_a_member_is_evicted() {
    let reachable = book(8);
    let (pool, _dialer) = pool_over(reachable.clone(), &reachable, 5);
    assert_eq!(pool.fill().await, 5);

    let evicted = held_addresses(&pool).await[2];
    pool.eject_peer(evicted).await;
    assert_eq!(pool.len().await, 4);

    assert_eq!(pool.fill().await, 5);
    let held = held_addresses(&pool).await;
    assert_eq!(held.len(), 5);
    assert_eq!(held.iter().collect::<HashSet<_>>().len(), 5, "all distinct");
    assert!(
        !held.contains(&evicted),
        "the freed slot took a new address, not the evicted one back: {held:?}"
    );
}

/// Two fills racing over the same book: the write-lock re-check is what keeps the realized set
/// distinct.
///
/// The contention gate is essential rather than decorative. Without it the first fill runs to
/// completion before the second is ever polled — every dial resolves immediately — so the two
/// fills never overlap and the test cannot see a duplicate even with every guard removed. It
/// was written that way first, and passed against deliberately broken code.
#[tokio::test]
async fn concurrent_fills_cannot_admit_the_same_address_twice() {
    let reachable = book(6);
    let dialer = FakeDialer::reaching_with_contention(&reachable, reachable[0]);
    let pool = PeerPoolInner::with_dialer(
        Arc::clone(&dialer) as Arc<dyn PeerDialer<FakePeer>>,
        5,
        Vec::new(),
        NetworkType::Mainnet,
        Duration::from_millis(1),
        Some(reachable.clone()),
    );

    let (a, b) = tokio::join!(pool.fill(), pool.fill());

    let held = held_addresses(&pool).await;
    assert_eq!(
        held.iter().collect::<HashSet<_>>().len(),
        held.len(),
        "concurrent fills admitted a duplicate: {held:?}"
    );
    assert!(held.len() <= 5, "pool overshot its target: {held:?}");
    assert!(a <= 5 && b <= 5);
}

/// The drop-on-rejection, and the only test that can see it.
///
/// A rejected duplicate whose receiver handler still ran would keep folding that peer's claim
/// into the shared peak — so the collapsed pool would go on reporting the squatter's number even
/// though the duplicate connection was refused.
#[tokio::test]
async fn a_rejected_duplicate_never_feeds_the_shared_peak() {
    let squatter = addr(1);
    let honest = addr(2);
    // Two concurrent fills both dial the squatter before either can admit it, so the second
    // connection is genuinely opened and then REFUSED at the write-lock re-check. The honest
    // peer is the control: the pool is not simply inert.
    let dialer = FakeDialer::reaching_with_contention(&[squatter, honest], squatter);
    let pool = PeerPoolInner::with_dialer(
        Arc::clone(&dialer) as Arc<dyn PeerDialer<FakePeer>>,
        3,
        Vec::new(),
        NetworkType::Mainnet,
        Duration::from_millis(1),
        Some(vec![squatter, honest]),
    );

    tokio::join!(pool.fill(), pool.fill());
    assert_eq!(held_addresses(&pool).await.len(), 2, "squatter + control");

    let senders = dialer.senders_for(squatter);
    assert_eq!(
        senders.len(),
        2,
        "the fixture must actually produce a second connection to the same address"
    );
    // Exactly one of the two connections survived admission; which one won the write lock is a
    // scheduling detail, so identify them by whether their receiver still exists.
    let (open, closed): (Vec<_>, Vec<_>) = senders.iter().partition(|s| !s.is_closed());
    assert_eq!(open.len(), 1, "exactly one connection was admitted");
    assert_eq!(closed.len(), 1, "the duplicate's receiver was dropped");
    let (admitted, rejected) = (open[0], closed[0]);

    // The admitted connection's handler is alive and its claim reaches the shared peak.
    announce_peak(admitted, 100, 100).await;
    let peak = Arc::clone(&pool.peak_height);
    wait_until("the admitted peer's claim", || {
        let peak = Arc::clone(&peak);
        async move { peak.load(Ordering::Relaxed) == 100 }
    })
    .await;

    // The rejected connection was dropped: no receiver, so no handler, so no send.
    let rejected_send = rejected
        .send(Message {
            msg_type: ProtocolMessageTypes::NewPeakWallet,
            id: None,
            data: NewPeakWallet {
                header_hash: Bytes32::new([9u8; 32]),
                height: 9_999_999,
                weight: 1,
                fork_point_with_previous_peak: 0,
            }
            .to_bytes()
            .expect("NewPeakWallet encodes")
            .into(),
        })
        .await;
    assert!(
        rejected_send.is_err(),
        "the refused duplicate's receiver must have been dropped, not handed to a handler"
    );

    // And the shared peak still reflects only the admitted peer.
    assert_eq!(pool.peak_height(), 100);
}

// ---------------------------------------------------------------------------
// Peak claims
// ---------------------------------------------------------------------------

/// Per-member claims stay each peer's own; the shared height is their maximum. This is what
/// lets a caller tell "two peers agree" from "one peer said it twice".
#[tokio::test]
async fn per_peer_peaks_stay_distinct_while_peak_height_is_the_max() {
    let (low, high) = (addr(1), addr(2));
    let (pool, dialer) = pool_over(vec![low, high], &[low, high], 2);
    assert_eq!(pool.fill().await, 2);

    let low_hash = announce_peak(&dialer.senders_for(low)[0], 100, 1_000).await;
    let high_hash = announce_peak(&dialer.senders_for(high)[0], 200, 2_000).await;

    let members = pool.peer_members().await;
    for member in &members {
        let m = member.clone();
        wait_until("both peers to record a claim", move || {
            let m = m.clone();
            async move { m.peak().await.is_some() }
        })
        .await;
    }

    let claim = |a: SocketAddr| {
        let member = members
            .iter()
            .find(|m| m.addr == a)
            .expect("member is held")
            .clone();
        async move { member.peak().await.expect("a recorded claim") }
    };

    assert_eq!(
        claim(low).await,
        PeakClaim {
            height: 100,
            header_hash: low_hash,
            weight: 1_000
        }
    );
    assert_eq!(
        claim(high).await,
        PeakClaim {
            height: 200,
            header_hash: high_hash,
            weight: 2_000
        }
    );
    assert_eq!(pool.peak_height(), 200, "the shared height is the maximum");
}

/// A member has no claim until its peer makes one — an unknown peak must never read as zero
/// agreement.
#[tokio::test]
async fn a_member_has_no_peak_claim_before_its_peer_announces_one() {
    let only = addr(1);
    let (pool, _dialer) = pool_over(vec![only], &[only], 1);
    assert_eq!(pool.fill().await, 1);

    assert_eq!(pool.peer_members().await[0].peak().await, None);
    assert_eq!(pool.peak_height(), 0);
}

// ---------------------------------------------------------------------------
// Selection + construction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_peer_round_robins_across_the_distinct_members() {
    let reachable = book(3);
    let (pool, _dialer) = pool_over(reachable.clone(), &reachable, 3);
    assert_eq!(pool.fill().await, 3);

    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(pool.select_peer().await.expect("a peer").1);
    }
    assert_eq!(seen.iter().collect::<HashSet<_>>().len(), 3);
}

#[tokio::test]
async fn an_empty_candidate_list_yields_an_empty_pool_without_erroring() {
    let (pool, _dialer) = pool_over(Vec::new(), &[], 5);
    assert_eq!(pool.fill().await, 0);
    assert!(!pool.has_peers().await);
}

/// `max_peers: 0` attempts no connection at all, so the pool is deterministically
/// empty offline — an exact, network-free fixture for the empty-pool branch.
async fn pool_with_no_connection_attempts(
    requirement: PeerRequirement,
) -> Result<PeerPool, ChiaQueryError> {
    PeerPool::new(
        NetworkType::Mainnet,
        crate::peer::connect::create_generated_tls().expect("generate a TLS identity"),
        0,
        Vec::new(),
        requirement,
        Duration::from_millis(1),
    )
    .await
}

/// The control: an empty pool is still fatal when nothing can serve in its place.
#[tokio::test]
async fn empty_pool_is_fatal_when_peers_are_required() {
    assert!(matches!(
        pool_with_no_connection_attempts(PeerRequirement::Required).await,
        Err(ChiaQueryError::PeerDiscoveryFailed)
    ));
}

/// #2210: with a fallback able to serve, an empty pool must not deny the client.
#[tokio::test]
async fn empty_pool_is_tolerated_when_peers_are_optional() {
    let pool = pool_with_no_connection_attempts(PeerRequirement::Optional)
        .await
        .expect("an optional peer pool must construct with zero peers");
    assert!(!pool.has_peers().await);
}
