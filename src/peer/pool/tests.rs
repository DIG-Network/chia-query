//! Admission tests for [`PeerPoolInner`] (dig_ecosystem#2648).
//!
//! Every assertion is on the pool's REALIZED contents — which addresses it ended up holding —
//! never on "no error was returned". The collapsed pool this fixes returned no error at all; it
//! reported itself full and healthy while holding five sockets to one process.
//!
//! The peer stand-in is what makes these tests possible offline, and it is honest here because
//! the admission rule keys on `SocketAddr` alone and never touches the peer type.

use super::*;

use std::sync::atomic::AtomicUsize;
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

/// A candidate source that answers with a fixed list, counting how often it was asked.
///
/// The count is what makes the resolve-once guarantee observable: "the list did not change"
/// is satisfied by a pool that re-resolves and happens to get the same answer.
struct FixedAddresses {
    /// One answer per call, the last repeating thereafter. `None` is a discovery FAILURE, which
    /// the pool must treat differently from an empty success.
    answers: Mutex<Vec<Option<Vec<SocketAddr>>>>,
    calls: AtomicUsize,
}

impl FixedAddresses {
    /// Answers `addrs` every time it is asked.
    fn always(addrs: &[SocketAddr]) -> Arc<Self> {
        Self::in_turn(vec![Some(addrs.to_vec())])
    }

    /// Answers each of `answers` in turn, repeating the last one thereafter.
    fn in_turn(answers: Vec<Option<Vec<SocketAddr>>>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers),
            calls: AtomicUsize::new(0),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AddressSource for FixedAddresses {
    fn discover(&self) -> DiscoverFuture {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let mut answers = self.answers.lock().expect("answers are not poisoned");
        let answer = if answers.len() > 1 {
            answers.remove(0)
        } else {
            answers[0].clone()
        };
        Box::pin(async move { answer.ok_or(ChiaQueryError::PeerDiscoveryFailed) })
    }
}

/// A dialer that answers only for addresses in its book, and records every connection it
/// handed out — including the ones the pool went on to reject.
struct FakeDialer {
    /// Addresses that answer. Anything else fails to connect.
    reachable: HashSet<SocketAddr>,
    /// EVERY dial the pool asked for, in order, reachable or not.
    ///
    /// Attempts rather than successes, because what needs bounding is the work a fill does:
    /// a count of successes cannot tell a fill that swept a hundred unreachable addresses from
    /// one that stopped after the first.
    attempted: Mutex<Vec<SocketAddr>>,
    /// Every dial that succeeded, in order, with the sender for that connection.
    handed_out: Mutex<Vec<(SocketAddr, mpsc::Sender<Message>)>>,
    /// Holds dials to this address until two of them have arrived.
    ///
    /// The pool skips an address it already holds BEFORE dialling, so the only way a duplicate
    /// connection is ever opened is two fills dialling concurrently and one losing the
    /// write-lock re-check. That race is what this makes deterministic.
    contended: Option<(SocketAddr, Arc<tokio::sync::Barrier>)>,
    /// Whether every dial yields before answering.
    ///
    /// A dialer that answers on its first poll lets one whole fill run to completion inside a
    /// single poll of a joined future, so concurrent fills never actually overlap and a test
    /// meant to observe overlap observes none. Yielding is the smallest thing that makes the
    /// interleaving real without introducing a timing dependency.
    stalls: bool,
    /// Dials currently in flight, and the most that were ever in flight at once.
    ///
    /// The high-water mark is the only thing that can tell a CONCURRENT batch from a serial
    /// sweep: both dial the same addresses in the same order and both record the same attempt
    /// count. Only overlap distinguishes them, and overlap is what the fill's cost bound is
    /// argued from.
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl FakeDialer {
    fn new(addrs: &[SocketAddr], stalls: bool, contended: Option<SocketAddr>) -> Arc<Self> {
        Arc::new(Self {
            reachable: addrs.iter().copied().collect(),
            attempted: Mutex::new(Vec::new()),
            handed_out: Mutex::new(Vec::new()),
            contended: contended.map(|a| (a, Arc::new(tokio::sync::Barrier::new(2)))),
            stalls,
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn reaching(addrs: &[SocketAddr]) -> Arc<Self> {
        Self::new(addrs, false, None)
    }

    /// As [`reaching`](Self::reaching), but every dial yields before answering, so concurrent
    /// fills genuinely interleave.
    fn reaching_slowly(addrs: &[SocketAddr]) -> Arc<Self> {
        Self::new(addrs, true, None)
    }

    /// As [`reaching`](Self::reaching), but dials to `addr` block until two have arrived.
    fn reaching_with_contention(addrs: &[SocketAddr], addr: SocketAddr) -> Arc<Self> {
        Self::new(addrs, false, Some(addr))
    }

    /// The most dials that were ever open simultaneously.
    fn max_concurrent_dials(&self) -> usize {
        self.max_in_flight.load(Ordering::Relaxed)
    }

    /// Every address the pool asked for, in dial order.
    fn attempts(&self) -> Vec<SocketAddr> {
        self.attempted
            .lock()
            .expect("dial log is not poisoned")
            .clone()
    }

    fn attempt_count(&self) -> usize {
        self.attempted
            .lock()
            .expect("dial log is not poisoned")
            .len()
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

    /// Dials that CONNECTED. See [`attempt_count`](Self::attempt_count) for dials made.
    fn connected_count(&self) -> usize {
        self.handed_out
            .lock()
            .expect("dial log is not poisoned")
            .len()
    }
}

impl PeerDialer<FakePeer> for FakeDialer {
    fn dial(&self, addr: SocketAddr) -> DialFuture<FakePeer> {
        self.attempted
            .lock()
            .expect("dial log is not poisoned")
            .push(addr);
        let stalls = self.stalls;
        let (in_flight, max_in_flight) =
            (Arc::clone(&self.in_flight), Arc::clone(&self.max_in_flight));
        // A guard so the in-flight count falls however the dial future ends, including if it is
        // dropped part-way — a leaked count would make a later serial sweep look concurrent.
        let enter = move || {
            let open = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            max_in_flight.fetch_max(open, Ordering::Relaxed);
            InFlight(Arc::clone(&in_flight))
        };
        if !self.reachable.contains(&addr) {
            return Box::pin(async move {
                let _open = enter();
                if stalls {
                    tokio::task::yield_now().await;
                }
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
            let _open = enter();
            if stalls {
                tokio::task::yield_now().await;
            }
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
            Ok((peer, rx))
        })
    }
}

/// Decrements the dialer's in-flight count when the dial future ends, however it ends.
struct InFlight(Arc<AtomicUsize>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The nth fixture address, in GLOBALLY ROUTABLE space.
///
/// `1.2.3.0/24` and not `10.0.0.0/24`, and the difference is load-bearing. The pool filters
/// discovered addresses to routable ones, so a fixture book in RFC1918 space is dropped before
/// any admission guard is reached and every test over it would assert an empty pool. The cheapest
/// way to make such a book work again is to weaken the filter, which is exactly the defect
/// (dig_ecosystem#2648). [`a_private_address_from_discovery_is_never_dialled`] keeps one private
/// address around to prove the filter still refuses it, through the pool.
fn addr(n: u8) -> SocketAddr {
    format!("1.2.3.{n}:8444")
        .parse()
        .expect("a valid socket address")
}

fn private_addr(n: u8) -> SocketAddr {
    format!("10.0.0.{n}:8444")
        .parse()
        .expect("a valid socket address")
}

fn book(count: u8) -> Vec<SocketAddr> {
    (1..=count).map(addr).collect()
}

/// A pool discovering `candidates`, reaching everything in `reachable`.
fn pool_over(
    candidates: Vec<SocketAddr>,
    reachable: &[SocketAddr],
    target: usize,
) -> (PeerPoolInner<FakePeer>, Arc<FakeDialer>) {
    let dialer = FakeDialer::reaching(reachable);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&candidates),
        target,
        Vec::new(),
    );
    (pool, dialer)
}

/// A pool over an explicit dialer, address source and trusted list — the full production
/// seeding path, with only the network at its edges replaced.
fn pool_with(
    dialer: Arc<FakeDialer>,
    discovery: Arc<FixedAddresses>,
    target: usize,
    trusted: Vec<SocketAddr>,
) -> PeerPoolInner<FakePeer> {
    PeerPoolInner::with_dialer(
        dialer as Arc<dyn PeerDialer<FakePeer>>,
        discovery as Arc<dyn AddressSource>,
        target,
        trusted,
        NetworkType::Mainnet,
    )
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

/// #2648, the core defect: one address offered for every slot fills exactly ONE, and is dialled
/// exactly once.
///
/// Before the fix, five independent `connect_random_peer` calls each returned the one address
/// that answered and the pool pushed all five unconditionally.
#[tokio::test]
async fn one_address_offered_repeatedly_yields_one_member() {
    let squatter = addr(1);
    let (pool, dialer) = pool_over(vec![squatter; 5], &[squatter], 5);

    assert_eq!(pool.fill().await, 1);
    assert_eq!(held_addresses(&pool).await, vec![squatter]);
    // The dial count is the discriminating half. Holding one member is also what a pool that
    // dialled the squatter five times and refused four would report, and those differ in the
    // work done and in how a real full node sees this client.
    assert_eq!(dialer.attempts(), vec![squatter]);
}

/// The same collapse in its realistic shape: the candidate list is full of real peers, but only
/// the co-resident process answers.
#[tokio::test]
async fn a_single_reachable_address_cannot_occupy_more_than_one_slot() {
    let squatter = addr(1);
    let (pool, dialer) = pool_over(book(8), &[squatter], 5);

    assert_eq!(pool.fill().await, 1);
    assert_eq!(held_addresses(&pool).await, vec![squatter]);
    assert_eq!(dialer.connected_count(), 1, "only the squatter connects");
    // The shortfall is the network's and not a premature stop: every one of the eight
    // candidates was genuinely attempted. `connected_count` alone cannot say this — a fill that
    // broke out after its first success produces exactly the same 1.
    assert_eq!(dialer.attempt_count(), 8, "every candidate was attempted");
}

/// A held address is skipped BEFORE it is dialled, not dialled and then refused.
///
/// Anchors the pre-dial skip specifically. The write-lock re-check would keep the member set
/// distinct without it, so the realized membership cannot tell the two apart — what can is the
/// wasted connection: to a real full node an unnecessary redial is an unexplained second
/// handshake from a client that already holds an open session.
#[tokio::test]
async fn an_address_the_pool_already_holds_is_never_dialled_again() {
    let reachable = book(3);
    let (pool, dialer) = pool_over(reachable.clone(), &reachable[..1], 3);

    assert_eq!(pool.fill().await, 1, "only the first address answers");
    assert_eq!(
        dialer.attempts(),
        reachable,
        "the first fill tried all three"
    );

    // A second fill over the same list: the pool is still under target, so it sweeps again —
    // and must leave the address it already holds alone.
    assert_eq!(pool.fill().await, 1);
    let second_sweep = &dialer.attempts()[reachable.len()..];
    assert!(
        !second_sweep.contains(&reachable[0]),
        "the held address was re-dialled: {second_sweep:?}"
    );
    assert_eq!(
        second_sweep,
        &reachable[1..],
        "the untried ones were retried"
    );
}

/// A fill stops at `target`: it admits no more members than that, and it stops DIALLING once it
/// has them.
///
/// Two guards, and the fixture has to see both. Distinctness is satisfied by an overshooting
/// pool too — six distinct members is still six distinct addresses — so only the member count
/// can see the ceiling. And the write-lock ceiling alone would produce the same member count
/// while the loop went on dialling every remaining candidate, so only the dial count can see the
/// loop's own break. The candidate list is deliberately three batches long, and everything in it
/// answers, so a fill that failed to stop would be plainly visible.
#[tokio::test]
async fn a_fill_admits_no_more_members_than_the_target() {
    let reachable = book(30);
    let (pool, dialer) = pool_over(reachable.clone(), &reachable, 3);

    assert_eq!(pool.fill().await, 3);
    assert_eq!(held_addresses(&pool).await.len(), 3);
    // Dialling happens a batch at a time, so the first batch opens more connections than there
    // is room for; what must not happen is admitting them, or dialling a batch beyond it.
    assert!(
        dialer.connected_count() > 3,
        "the fixture must produce surplus connections for the ceiling to refuse"
    );
    assert_eq!(
        dialer.attempt_count(),
        DIAL_BATCH,
        "the fill dialled past the batch that already reached target"
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

/// Two fills racing over the same book: the ADDRESS half of the write-lock re-check is what
/// keeps the realized set distinct.
///
/// Two fixture choices are load-bearing, and each was arrived at by watching this test pass
/// against deliberately broken code.
///
/// The contention gate is the first. Without it the winning fill runs to completion before the
/// loser is ever polled — every dial resolves immediately — so the two never overlap and no
/// duplicate can arise however many guards are removed.
///
/// The TARGET EXCEEDING THE BOOK is the second, and it is the subtler one. With `target = 5`
/// over six addresses the loser is refused by `members.len() >= target` — the COUNT ceiling —
/// before the address comparison is ever reached, so the nearest wrong implementation (a
/// target-only re-check) satisfies the assertion identically. `target = 9` over six reachable
/// addresses makes the ceiling unreachable, leaving the address check as the only guard under
/// test. Do NOT tidy the target back down to the size of the book.
///
/// Both conjuncts of that re-check are separately necessary and both must survive: the count
/// ceiling is what stops a racing fill overshooting, and is anchored by
/// [`a_fill_admits_no_more_members_than_the_target`].
#[tokio::test]
async fn concurrent_fills_cannot_admit_the_same_address_twice() {
    let reachable = book(6);
    let dialer = FakeDialer::reaching_with_contention(&reachable, reachable[0]);
    let pool = pool_with(dialer, FixedAddresses::always(&reachable), 9, Vec::new());

    let (a, b) = tokio::join!(pool.fill(), pool.fill());

    let held = held_addresses(&pool).await;
    assert_eq!(
        held.iter().collect::<HashSet<_>>().len(),
        held.len(),
        "concurrent fills admitted a duplicate: {held:?}"
    );
    assert!(
        held.len() <= reachable.len(),
        "the pool holds more members than there are reachable addresses: {held:?}"
    );
    assert!(a <= reachable.len() && b <= reachable.len());
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
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&[squatter, honest]),
        3,
        Vec::new(),
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
// What may be dialled at all
// ---------------------------------------------------------------------------

/// #2648 in its full shape, through the PUBLIC seam: a poisoned address source cannot put
/// loopback in the pool.
///
/// `with_dialer` and `AddressSource` are both public, so an address list reaching the pool need
/// never have passed through DNS discovery — a consumer building its own pool over this crate's
/// resolver seam supplies one directly. A filter that lived only in `discover_addresses` left
/// that entire path unguarded, and this fixture is the exploit: five loopback addresses, five
/// slots, and on a Linux host all of `127.0.0.0/8` is one process answering as five distinct
/// `SocketAddr`s. Every admission guard in the pool passes them, because they ARE distinct.
///
/// The routable control is what makes this a filter test and not a "the pool dialled nothing"
/// test: the honest address must still be admitted.
#[tokio::test]
async fn a_poisoned_address_source_cannot_put_a_local_address_in_the_pool() {
    let squatter: Vec<SocketAddr> = (1..=5)
        .map(|n| format!("127.0.0.{n}:8444").parse().expect("valid"))
        .collect();
    let honest = addr(7);
    let offered = [squatter.clone(), vec![honest]].concat();

    let dialer = FakeDialer::reaching(&offered);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&offered),
        5,
        Vec::new(),
    );

    assert_eq!(pool.fill().await, 1);
    assert_eq!(
        held_addresses(&pool).await,
        vec![honest],
        "an address nobody configured must never be dialled merely because a source named it"
    );
    assert_eq!(
        dialer.attempts(),
        vec![honest],
        "and it must be refused BEFORE the handshake, not admitted and then evicted"
    );
}

/// The private half of the same rule, and the one fixture in this file still in RFC1918 space.
///
/// Every other fixture address was moved to routable space when the filter reached the pool. The
/// cheap way to have kept them working was to weaken the filter; this is what stands in the way
/// of that, and it asserts refusal through the pool rather than through the predicate.
#[tokio::test]
async fn a_private_address_from_discovery_is_never_dialled() {
    let private = private_addr(1);
    let routable = addr(2);
    let dialer = FakeDialer::reaching(&[private, routable]);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&[private, routable]),
        5,
        Vec::new(),
    );

    assert_eq!(pool.fill().await, 1);
    assert_eq!(dialer.attempts(), vec![routable]);
}

/// The operator's exemption survives the filter being where it now is.
///
/// A local full node is the legitimate loopback case, and it reaches the pool because somebody
/// CONFIGURED it — not because something answered. Without this the filter would be indis-
/// tinguishable from a blanket refusal, which would break every operator running their own node.
#[tokio::test]
async fn an_operator_named_local_address_is_still_dialled_and_admitted() {
    let local: SocketAddr = "127.0.0.1:8444".parse().expect("valid");
    let discovered = book(3);
    let dialer = FakeDialer::reaching(&[&discovered[..], &[local][..]].concat());
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&discovered),
        4,
        vec![local],
    );

    assert_eq!(pool.fill().await, 4);
    assert_eq!(
        dialer.attempts().first(),
        Some(&local),
        "the operator's own node is dialled first"
    );
    assert!(held_addresses(&pool).await.contains(&local));
}

/// A discovery that resolves nothing DIALABLE is not a resolution, and is not cached.
///
/// A resolver answering only private or loopback addresses succeeds at the transport level and
/// contributes nothing. Caching that would fix the candidate list on the operator's own addresses
/// for the process's life — the same permanent downgrade a cached FAILURE would cause, arriving
/// by a route the failure check cannot see.
#[tokio::test]
async fn a_discovery_that_resolves_nothing_routable_is_not_cached() {
    let junk = vec![private_addr(1), private_addr(2)];
    let good = book(3);
    let dialer = FakeDialer::reaching(&good);
    let discovery = FixedAddresses::in_turn(vec![Some(junk), Some(good.clone())]);
    let pool = pool_with(Arc::clone(&dialer), Arc::clone(&discovery), 3, Vec::new());

    assert_eq!(pool.fill().await, 0, "nothing usable resolved");
    assert_eq!(dialer.attempt_count(), 0, "and nothing was dialled");

    assert_eq!(pool.fill().await, 3, "the next fill resolves and fills");
    assert_eq!(discovery.call_count(), 2, "discovery was asked again");
}

// ---------------------------------------------------------------------------
// Seeding: discovery, trusted addresses, and what is cached
// ---------------------------------------------------------------------------

/// A FAILED discovery is never cached, so a dropped DNS exchange costs one fill and not the
/// process's life.
///
/// The failure is deliberately transient and the source then answers normally — an
/// always-failing source could not tell "not cached" from "cached and empty", because both look
/// like an empty pool forever. The retry succeeding is the whole observation.
#[tokio::test]
async fn a_failed_discovery_is_retried_rather_than_cached() {
    let reachable = book(3);
    let dialer = FakeDialer::reaching(&reachable);
    let discovery = FixedAddresses::in_turn(vec![None, Some(reachable.clone())]);
    let pool = pool_with(Arc::clone(&dialer), Arc::clone(&discovery), 3, Vec::new());

    assert_eq!(pool.fill().await, 0, "nothing resolved, so nothing is held");
    assert_eq!(dialer.attempt_count(), 0, "and nothing was dialled");

    assert_eq!(pool.fill().await, 3, "the next fill resolves and fills");
    assert_eq!(
        discovery.call_count(),
        2,
        "discovery was asked a second time"
    );
}

/// A SUCCESSFUL resolution is fixed for the pool's life.
///
/// Re-resolving per fill would let a resolver that is fast or always up reappear at the head of
/// every subsequent list. The call count is the discriminating assertion: a pool that
/// re-resolved and got the same answer would hold the same members.
#[tokio::test]
async fn a_successful_resolution_is_not_repeated_on_a_later_fill() {
    let reachable = book(3);
    let dialer = FakeDialer::reaching(&reachable[..1]);
    let discovery = FixedAddresses::always(&reachable);
    let pool = pool_with(Arc::clone(&dialer), Arc::clone(&discovery), 3, Vec::new());

    assert_eq!(pool.fill().await, 1);
    assert_eq!(
        pool.fill().await,
        1,
        "still under target, so it fills again"
    );
    assert_eq!(
        discovery.call_count(),
        1,
        "the candidate list is resolved once and then fixed"
    );
}

/// Operator-named addresses reach the pool through the REAL seeding path, ahead of discovery,
/// and occupy at most one slot even when discovery names them too.
///
/// This is the claim the crate makes about a configured local node, and until now no test
/// reached the composition that has to hold it up: an operator who configures a node and
/// silently gets zero peers would not have been caught.
#[tokio::test]
async fn a_trusted_address_is_dialled_first_and_occupies_one_slot() {
    // The configured address is deliberately ABSENT from what discovery returns. A pool that
    // discarded its trusted list entirely would still dial a configured address that discovery
    // also happened to name, and would still name it first — so a fixture where the two overlap
    // cannot tell "honoured" from "ignored".
    let configured = addr(9);
    let discovered = book(3);
    let dialer = FakeDialer::reaching(&[&discovered[..], &[configured][..]].concat());
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&discovered),
        3,
        vec![configured],
    );

    assert_eq!(pool.fill().await, 3);
    assert_eq!(
        dialer.attempts().first(),
        Some(&configured),
        "the operator's address is dialled before anything discovered"
    );
    assert!(
        held_addresses(&pool).await.contains(&configured),
        "an operator who configures a node must not silently get only discovered peers"
    );
}

/// An address named by BOTH configuration and discovery occupies at most one slot — the claim
/// this crate makes about a configured local node crowding out the pool.
#[tokio::test]
async fn an_address_both_configured_and_discovered_is_dialled_once() {
    let configured = addr(1);
    let discovered = book(3);
    let dialer = FakeDialer::reaching(&discovered);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&discovered),
        3,
        vec![configured],
    );

    assert_eq!(pool.fill().await, 3);
    assert_eq!(
        dialer
            .attempts()
            .iter()
            .filter(|a| **a == configured)
            .count(),
        1,
        "offered twice - by configuration and by discovery - and still dialled once"
    );
    assert_eq!(
        held_addresses(&pool)
            .await
            .iter()
            .filter(|a| **a == configured)
            .count(),
        1,
        "and occupying exactly one slot"
    );
}

/// The pool's own seeding path is the one production uses: nothing pre-seeds the candidate
/// list, so discovery is genuinely consulted.
///
/// Without this, a pool that never asked its address source would look identical to one that
/// did — which is how the trusted/discovered composition went untested.
#[tokio::test]
async fn a_fill_consults_the_address_source() {
    let reachable = book(2);
    let discovery = FixedAddresses::always(&reachable);
    let pool = pool_with(
        FakeDialer::reaching(&reachable),
        Arc::clone(&discovery),
        2,
        Vec::new(),
    );

    assert_eq!(discovery.call_count(), 0, "not before it is needed");
    assert_eq!(pool.fill().await, 2);
    assert_eq!(discovery.call_count(), 1);
}

// ---------------------------------------------------------------------------
// What a fill costs
// ---------------------------------------------------------------------------

/// One fill dials a BOUNDED number of addresses, however long the candidate list is.
///
/// The candidate list is 60 here because the mainnet introducers currently answer with over a
/// hundred addresses; a serial unbounded sweep at the 8-second default connect timeout is over
/// sixteen minutes of dials, paid on construction and again on every read that finds the pool
/// short. Address-distinct admission is what makes that permanent: a host that cannot reach
/// `target` DISTINCT peers is under target forever, where the duplicate-admitting pool this PR
/// replaces was satisfied by one reachable peer and stopped sweeping.
///
/// Nothing is reachable on purpose — that is the case where the cap has to hold, and the one
/// where an early success cannot hide the sweep's real length.
#[tokio::test]
async fn one_fill_dials_a_bounded_number_of_addresses() {
    let candidates = book(60);
    let (pool, dialer) = pool_over(candidates.clone(), &[], 5);

    assert_eq!(pool.fill().await, 0);
    assert!(
        dialer.attempt_count() <= MAX_DIALS_PER_FILL,
        "one fill dialled {} of {} candidates, past the {MAX_DIALS_PER_FILL} cap",
        dialer.attempt_count(),
        candidates.len()
    );
    assert_eq!(
        dialer.attempt_count(),
        MAX_DIALS_PER_FILL,
        "and it does spend its whole budget rather than stopping early"
    );
}

/// Concurrent requests over a short pool cost ONE sweep, not one per request.
///
/// Without single-flight, K concurrent reads run K independent sweeps — up to K times the dial
/// cap in outbound handshakes from one client, which real full nodes rate-limit and ban source
/// IPs for. Nothing is reachable, so the pool stays short and every caller qualifies to sweep,
/// which is the condition under which this goes wrong.
///
/// The dialer must STALL. With a dialer that answers on its first poll, the first refill runs
/// start to finish inside one poll and arms the cooldown before any sibling is polled at all —
/// so the COOLDOWN carries the test and it passes with single-flight removed entirely. Yielding
/// gets all eight past the cooldown check before any of them finishes, which is what leaves
/// single-flight as the only thing that can hold the count down.
#[tokio::test]
async fn concurrent_refills_over_a_short_pool_sweep_once() {
    let dialer = FakeDialer::reaching_slowly(&[]);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&book(30)),
        5,
        Vec::new(),
    );

    let refills = (0..8).map(|_| pool.try_refill());
    futures_util::future::join_all(refills).await;

    assert_eq!(
        dialer.attempt_count(),
        MAX_DIALS_PER_FILL,
        "eight concurrent refills must cost one sweep, not eight"
    );
}

/// Successive fills reach DIFFERENT addresses, so the budget bounds one fill's cost and never
/// which candidates are dialable at all.
///
/// The cap alone makes the pool consider a fixed prefix of a fixed candidate list. Nothing else
/// rotates it: a FAILED dial records nothing, so with no admissions the second fill's list is
/// byte-identical to the first's. Two consequences, and the second is the one a full node sees.
/// On a host that cannot reach any of the first twenty, the other hundred are never dialled again
/// for the process's life and every read is served by the one centralized HTTP endpoint. And
/// because the pool is then permanently under target, each refill re-dials the identical twenty
/// that just failed — repeatedly, from one source IP, which is what full nodes ban for.
///
/// Nothing is reachable on purpose. An admission would let a shrinking shortfall explain the
/// second window's contents, where what is under test is that the window MOVES.
#[tokio::test]
async fn successive_fills_dial_different_addresses() {
    let candidates = book(60);
    let (pool, dialer) = pool_over(candidates.clone(), &[], 5);

    assert_eq!(pool.fill().await, 0);
    let first: Vec<SocketAddr> = dialer.attempts();
    assert_eq!(pool.fill().await, 0);
    let second: Vec<SocketAddr> = dialer.attempts()[first.len()..].to_vec();

    assert_eq!(first.len(), MAX_DIALS_PER_FILL);
    assert_eq!(second.len(), MAX_DIALS_PER_FILL);
    let overlap: Vec<&SocketAddr> = second.iter().filter(|a| first.contains(a)).collect();
    assert!(
        overlap.is_empty(),
        "the second fill re-dialled addresses the first had just tried: {overlap:?}"
    );
}

/// And the window WRAPS, so every candidate is eventually dialable rather than the list being
/// walked once and then abandoned.
///
/// Disjointness alone is satisfied by a cursor that runs off the end and stops dialling alto-
/// gether, which is a worse failure than the one it replaced.
///
/// The book is FIFTY, deliberately not a whole number of windows. A candidate list whose length
/// divides evenly by the budget hides the whole failure: every window then starts at an exact
/// multiple, so a cursor that merely clamps at the end of the list lands on the head anyway and
/// looks like it wrapped. At fifty the third window starts at 40 with ten addresses left, so a
/// window that does not wrap silently shrinks to half a budget — the pool quietly dialling less
/// than it is allowed to, once per lap, forever. Do NOT round this book up to sixty.
#[tokio::test]
async fn the_dial_window_wraps_and_covers_every_candidate() {
    let candidates = book(50);
    let (pool, dialer) = pool_over(candidates.clone(), &[], 5);

    let mut windows = Vec::new();
    let mut seen = 0;
    for _ in 0..3 {
        pool.fill().await;
        windows.push(dialer.attempts()[seen..].to_vec());
        seen = dialer.attempt_count();
    }

    for (n, window) in windows.iter().enumerate() {
        assert_eq!(
            window.len(),
            MAX_DIALS_PER_FILL,
            "fill {n} spent {} of its {MAX_DIALS_PER_FILL}-dial budget",
            window.len()
        );
    }
    let swept: HashSet<SocketAddr> = dialer.attempts().into_iter().collect();
    assert_eq!(
        swept.len(),
        candidates.len(),
        "three full windows over a fifty-address book must cover all of it"
    );
    assert_eq!(
        windows[2][10..],
        candidates[..10],
        "the window that runs off the end continues at the head rather than shrinking"
    );
}

/// A batch is dialled CONCURRENTLY, not one address after another.
///
/// This is what both of the fill's cost bounds are argued from: ten unreachable addresses at the
/// default eight-second connect timeout is eight seconds concurrently and eighty serially, on the
/// request path. A regression to serial produces the identical attempt list in the identical
/// order and the identical member set — only overlap can see it, so only overlap is asserted.
///
/// The dialer must stall; a dial that answers on its first poll never overlaps with anything.
#[tokio::test]
async fn a_batch_is_dialled_concurrently() {
    let dialer = FakeDialer::reaching_slowly(&[]);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&book(30)),
        5,
        Vec::new(),
    );

    assert_eq!(pool.fill().await, 0);
    assert_eq!(
        dialer.max_concurrent_dials(),
        DIAL_BATCH,
        "a whole batch must be in flight at once; {} were",
        dialer.max_concurrent_dials()
    );
}

/// Two concurrent fills cost ONE fill's dial budget between them, not one each.
///
/// A fill that measured occupancy from its OWN admissions cannot see a sibling's progress. Every
/// one of the loser's ten connections is refused by the at-target ceiling, refusal marks nothing,
/// so its private count stays at zero and it dials a SECOND full batch at real full nodes while
/// the pool is already at target — fifty percent over the cap, in exactly the handshake budget
/// the cap exists to defend. Reading occupancy from the member set is what makes the sibling
/// visible.
///
/// `target` is deliberately far below the batch width so the pool reaches it inside batch one,
/// and the dialer must stall so the two fills genuinely interleave.
#[tokio::test]
async fn two_concurrent_fills_do_not_exceed_one_fills_dial_budget() {
    let reachable = book(30);
    let dialer = FakeDialer::reaching_slowly(&reachable);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&reachable),
        3,
        Vec::new(),
    );

    tokio::join!(pool.fill(), pool.fill());

    assert_eq!(pool.len().await, 3);
    assert!(
        dialer.attempt_count() <= MAX_DIALS_PER_FILL,
        "two fills dialled {} addresses; a fill blind to its sibling dials {} here",
        dialer.attempt_count(),
        3 * DIAL_BATCH
    );
}

/// A refill that fell short is not repeated on the very next request.
///
/// A pool that cannot reach `target` has just demonstrated the network cannot currently supply
/// it. Retrying per request puts the dial cap in front of every single read.
///
/// The pool holds ONE member on purpose. That is the whole distinction the cooldown is allowed to
/// draw: it may throttle a SHORT pool, which has something to serve with, and never an EMPTY one
/// — see [`an_empty_pool_may_always_retry`].
#[tokio::test]
async fn a_refill_that_fell_short_is_not_immediately_repeated() {
    let reachable = book(1);
    let (pool, dialer) = pool_over(book(30), &reachable, 5);

    pool.try_refill().await;
    let after_first = dialer.attempt_count();
    assert!(after_first > 0, "the first refill did sweep");
    assert_eq!(pool.len().await, 1, "and fell short of the target");

    pool.try_refill().await;
    assert_eq!(
        dialer.attempt_count(),
        after_first,
        "the second refill swept again inside the cooldown"
    );
}

/// An EMPTY pool may always retry. The cooldown throttles a short pool; it must never black out
/// a pool that has nothing to serve with.
///
/// A pool that fell short at construction — a boot before the network is up, a DHCP or VPN race —
/// holds zero members and arms the cooldown, and an undifferentiated cooldown then refuses to
/// dial for a full minute while every read is answered by the one centralized HTTP endpoint the
/// peer tier exists to avoid depending on. Waiting cannot improve an empty pool; only dialling
/// can. `select_peer` already draws this distinction, and this is the fill side of it.
#[tokio::test]
async fn an_empty_pool_may_always_retry() {
    let dialer = FakeDialer::reaching(&[]);
    let pool = pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&book(60)),
        5,
        Vec::new(),
    );

    pool.try_refill().await;
    let after_first = dialer.attempt_count();
    assert_eq!(after_first, MAX_DIALS_PER_FILL, "the first refill swept");
    assert!(
        pool.is_empty().await,
        "and the pool is empty, not merely short"
    );

    pool.try_refill().await;
    assert_eq!(
        dialer.attempt_count(),
        2 * MAX_DIALS_PER_FILL,
        "an empty pool was blacked out by the cooldown"
    );
}

/// Draining a pool to zero re-enables refilling immediately, without waiting out a cooldown armed
/// while it still had a member.
///
/// This is the same invariant reached by the other route: ejections, not a short first fill. A
/// pool whose last member fails a request is empty and gated by a clock started when it was not.
#[tokio::test]
async fn ejecting_the_last_member_re_enables_refilling_at_once() {
    let only = addr(1);
    let (pool, dialer) = pool_over(book(30), &[only], 5);

    pool.try_refill().await;
    assert_eq!(pool.len().await, 1, "short, so the cooldown is armed");
    let while_short = dialer.attempt_count();

    pool.try_refill().await;
    assert_eq!(dialer.attempt_count(), while_short, "the cooldown holds");

    pool.eject_peer(only).await;
    pool.try_refill().await;
    assert!(
        dialer.attempt_count() > while_short,
        "a pool drained to zero must dial rather than wait out a clock started when it was short"
    );
}

// ---------------------------------------------------------------------------
// Placement: where a refill sits relative to the answer
// ---------------------------------------------------------------------------

/// SPEC "Placement", the WITH-a-member half: the caller gets its peer without waiting for a
/// sweep.
///
/// With something usable in hand a fill buys diversity, which the request waiting on it does not
/// need. Run in front, it puts a bounded-but-real sweep of outbound TLS dials in front of every
/// read on a short pool. The dialer stalls, so a refill placed in front could not have finished:
/// zero attempts at the moment of return is the only observation that separates the two
/// placements, since both eventually dial the same addresses.
#[tokio::test]
async fn a_refill_runs_behind_the_answer_when_a_member_can_serve_it() {
    let served = addr(1);
    let dialer = FakeDialer::reaching_slowly(&[served]);
    let pool = Arc::new(pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&book(30)),
        5,
        Vec::new(),
    ));
    pool.fill().await;
    assert_eq!(pool.len().await, 1, "one member, and still short of five");

    let before = dialer.attempt_count();
    let picked = pool.select_refilling().await;
    assert_eq!(picked.map(|(_, a)| a), Some(served));
    assert_eq!(
        dialer.attempt_count(),
        before,
        "the caller waited on a refill it did not need"
    );

    // And the refill is detached rather than skipped: it still happens, just not in the way.
    wait_until("the detached refill to dial", || {
        let dialer = Arc::clone(&dialer);
        async move { dialer.attempt_count() > before }
    })
    .await;
}

/// SPEC "Placement", the EMPTY half: with nothing to serve, the refill runs in front and the
/// caller gets a peer rather than an error.
///
/// Detaching it here would return "no peers available" while a perfectly good peer was one dial
/// away — and the next request would do the same, since the sweep it spawned had not landed yet.
#[tokio::test]
async fn a_refill_runs_in_front_of_the_answer_when_the_pool_is_empty() {
    let reachable = book(3);
    let dialer = FakeDialer::reaching_slowly(&reachable);
    let pool = Arc::new(pool_with(
        Arc::clone(&dialer),
        FixedAddresses::always(&reachable),
        3,
        Vec::new(),
    ));
    assert!(pool.is_empty().await);

    let picked = pool.select_refilling().await;
    assert!(
        picked.is_some_and(|(_, a)| reachable.contains(&a)),
        "an empty pool must dial before answering, not answer with nothing"
    );
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
