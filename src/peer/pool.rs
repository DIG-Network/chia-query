use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chia_protocol::{CoinStateUpdate, Message, NewPeakWallet, ProtocolMessageTypes};
use chia_traits::Streamable;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, RwLock};

use chia_wallet_sdk::client::Peer;
use tokio_tungstenite::Connector;

use crate::types::ChiaQueryError;
use crate::NetworkType;

use super::connect;
use super::frames::{FrameFanout, FrameSubscription, Generation, PoolFrame};
use super::plurality::{CORROBORATION_FLOOR, PEER_LIFETIME};

// ---------------------------------------------------------------------------
// Pool entry
// ---------------------------------------------------------------------------

struct PeerEntry {
    peer: Peer,
    address: SocketAddr,
    /// How this peer was reached. Held so a caller counting independent opinions can tell a
    /// preferred local node from a discovered one — see [`connect::PeerOrigin`].
    origin: connect::PeerOrigin,
    /// When this connection entered the pool, so it can be rotated out on a TIMER.
    ///
    /// The pool's other eviction is failure-driven, and failure is not the risk this guards: a set
    /// of peers that all keep answering is exactly the set an attacker only has to capture once
    /// (NC-12). Age is the only signal that separates the two.
    admitted_at: Instant,
}

// ---------------------------------------------------------------------------
// PeerRequirement
// ---------------------------------------------------------------------------

/// Whether at least one peer must connect for the pool to be considered usable.
///
/// A client that can fall back to the coinset HTTP tier is still useful with zero
/// peers, so failing construction on peer discovery would deny a keyless reader over a
/// peer-tier problem it does not need (dig_ecosystem#2210).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRequirement {
    /// Peer discovery failing is fatal.
    Required,
    /// An empty pool is acceptable; it refills in the background.
    Optional,
}

// ---------------------------------------------------------------------------
// PeerPool
// ---------------------------------------------------------------------------

/// Whether the pool holds enough independent voices for a corroborated read.
///
/// A two-variant answer rather than a bare count, so a caller cannot accidentally proceed with
/// "some" corroboration: the insufficient case names what it has AND what it needed, which is the
/// information a log line or a user-facing message actually requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorroborationReadiness {
    /// Enough independent peers, besides the one answering, to corroborate.
    Armed { corroborators: usize },
    /// Too few. The read must be REFUSED, never attempted with fewer voices.
    Insufficient {
        corroborators: usize,
        required: usize,
    },
}

pub struct PeerPool {
    entries: RwLock<Vec<PeerEntry>>,
    next_idx: AtomicUsize,
    max_peers: usize,
    tls: Connector,
    network: NetworkType,
    connect_timeout: Duration,
    /// Latest peak height observed from any connected peer's NewPeakWallet
    /// messages.  Updated in the background by receiver handler tasks.
    peak_height: Arc<AtomicU32>,
    /// Fans every inbound frame out to the pool's subscribers.
    ///
    /// The atomic above answers "how high is the chain"; this carries the frames THEMSELVES, which
    /// is what a consumer following coin states needs and what its absence forced into a second
    /// dialled session (dig_ecosystem#2761).
    fanout: Arc<FrameFanout>,
}

impl PeerPool {
    /// Spin up the pool by connecting to `max_peers` random full-node peers
    /// concurrently.  Under [`PeerRequirement::Required`] at least one peer must
    /// succeed, otherwise we return [`ChiaQueryError::PeerDiscoveryFailed`]; under
    /// [`PeerRequirement::Optional`] an empty pool is returned and refills later.
    pub async fn new(
        network: NetworkType,
        tls: Connector,
        max_peers: usize,
        requirement: PeerRequirement,
        connect_timeout: Duration,
    ) -> Result<Self, ChiaQueryError> {
        let peak_height = Arc::new(AtomicU32::new(0));

        // Connect to peers concurrently.
        let mut futures = FuturesUnordered::new();
        for _ in 0..max_peers {
            let t = tls.clone();
            futures.push(async move {
                connect::connect_random_peer_excluding(network, &t, connect_timeout, &[]).await
            });
        }

        let mut connected = Vec::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok(connection) => connected.push(connection),
                Err(e) => log::debug!("initial peer connect failed: {e}"),
            }
        }

        let pool = Self {
            entries: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls,
            network,
            connect_timeout,
            peak_height,
            fanout: Arc::new(FrameFanout::new()),
        };

        // Every connection enters through `admit`, including these, so the distinctness invariant
        // has exactly ONE enforcement site. The initial fill is where duplicates were most likely:
        // `max_peers` dials race concurrently with no knowledge of each other, so each one may
        // return the same priority address. A receiver handler is spawned only for a connection
        // that was actually admitted — spawning one for a discarded duplicate would keep feeding
        // peak heights from a connection nothing else can see, and this must happen after pool
        // construction so the `peak_height` Arc exists.
        for (peer, addr, receiver, origin) in connected {
            if pool.admit(peer, addr, origin).await {
                pool.spawn_receiver_handler(pool.fanout.generation(), receiver);
            }
        }

        if !pool.has_peers().await {
            if requirement == PeerRequirement::Required {
                return Err(ChiaQueryError::PeerDiscoveryFailed);
            }
            log::warn!("no peers connected; serving from the coinset fallback until one does");
        }

        Ok(pool)
    }

    /// Latest peak height observed across all connected peers.
    /// Returns 0 if no peak has been received yet.
    pub fn peak_height(&self) -> u32 {
        self.peak_height.load(Ordering::Relaxed)
    }

    /// Round-robin select a peer from the pool.
    /// Returns `None` when the pool is empty.
    pub async fn select_peer(&self) -> Option<(Peer, SocketAddr)> {
        let entries = self.entries.read().await;
        if entries.is_empty() {
            return None;
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % entries.len();
        let entry = &entries[idx];
        Some((entry.peer.clone(), entry.address))
    }

    /// Select a peer that could CORROBORATE an answer already given by the peer at `asked`.
    ///
    /// A corroborating peer must be two things at once, and neither alone is enough:
    ///
    /// - **A different address than `asked`.** Asking the same connection twice returns the same
    ///   opinion twice, which reads as agreement while being one voice.
    /// - **[`PeerOrigin::Discovered`](connect::PeerOrigin).** A peer reached from a preferred
    ///   address — an operator's node, or one on this machine — is an excellent peer to READ from
    ///   and is not evidence about the chain independent of this host, exactly as
    ///   [`independent_peer_count`](Self::independent_peer_count) records.
    ///
    /// Returns `None` when the pool holds no such peer, which is the honest answer that there is
    /// nobody to corroborate with — never a substitute peer that would manufacture agreement.
    pub async fn select_corroborating_peer(&self, asked: SocketAddr) -> Option<(Peer, SocketAddr)> {
        let entries = self.entries.read().await;
        let candidates: Vec<&PeerEntry> = entries
            .iter()
            .filter(|e| e.address != asked && e.origin == connect::PeerOrigin::Discovered)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % candidates.len();
        let entry = candidates[idx];
        Some((entry.peer.clone(), entry.address))
    }

    /// Every peer that could CORROBORATE an answer already given by the peer at `asked`.
    ///
    /// The plural of [`select_corroborating_peer`](Self::select_corroborating_peer), and it holds
    /// the same two requirements: a different address, and
    /// [`PeerOrigin::Discovered`](connect::PeerOrigin). A caller corroborating a POSITIVE answer
    /// wants all of them at once — asking them one at a time lets the first responder settle a
    /// claim about the chain, which is exactly the power a hostile peer has
    /// (dig_ecosystem#2462).
    ///
    /// Returns an empty vector when the pool holds nobody who qualifies, which is the honest
    /// answer that there is nobody to corroborate with.
    pub async fn select_corroborating_peers(&self, asked: SocketAddr) -> Vec<(Peer, SocketAddr)> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| e.address != asked && e.origin == connect::PeerOrigin::Discovered)
            .map(|e| (e.peer.clone(), e.address))
            .collect()
    }

    /// Remove a peer from the pool and asynchronously connect a replacement.
    pub async fn eject_peer(&self, addr: SocketAddr) {
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.address != addr);
        }
        log::debug!(
            "peer ejected from pool; will refill on next request (network={:?})",
            self.network,
        );
    }

    /// Whether the pool has at least one usable peer.
    pub async fn has_peers(&self) -> bool {
        !self.entries.read().await.is_empty()
    }

    /// How many peers the pool HOLDS right now.
    ///
    /// This is a live count of the connections currently in the pool, not
    /// [`max_peers`](Self::new)'s target: a pool that is still filling reports what it has, and
    /// reports the target only once it has reached it. A caller showing this number to a user is
    /// stating a fact about the machine, so a configured intention must never stand in for it.
    ///
    /// A peer is removed by [`eject_peer`](Self::eject_peer), which runs when a request to it
    /// FAILS. So the count is of peers held and believed usable; a connection that has died
    /// silently is still counted until something tries to use it. That is the same liveness
    /// standard [`has_peers`](Self::has_peers) has always answered by, made countable.
    pub async fn peer_count(&self) -> usize {
        self.entries.read().await.len()
    }

    /// How many peers the pool holds that are INDEPENDENT opinions.
    ///
    /// [`peer_count`](Self::peer_count) answers "how many connections do I have"; this answers
    /// "how many of them could corroborate each other". They differ by the peers reached from a
    /// preferred address — an operator's trusted node or one on this machine — which are excellent
    /// peers to READ from and are not evidence about the chain independent of this host. A caller
    /// deciding whether enough separate sources agree MUST use this number, because counting a
    /// co-resident node as an independent voice is the thing that made a single local process able
    /// to look like a full peer set (dig_ecosystem#2648).
    pub async fn independent_peer_count(&self) -> usize {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| e.origin == connect::PeerOrigin::Discovered)
            .count()
    }

    /// Whether the pool can honestly attempt a CORROBORATED read right now.
    ///
    /// The peer that answered cannot corroborate itself, so the question is whether
    /// [`CORROBORATION_FLOOR`] OTHER independent peers are held — hence the `- 1`.
    ///
    /// **This refuses; it never degrades.** Corroborating against however many peers happen to be
    /// present turns a four-voice quorum into a two-voice one that still reports itself
    /// corroborated, and no consumer downstream can tell those apart. A caller handed
    /// [`CorroborationReadiness::Insufficient`] must decline the read, not proceed with fewer
    /// voices.
    pub async fn corroboration_readiness(&self) -> CorroborationReadiness {
        let independent = self.independent_peer_count().await;
        let corroborators = independent.saturating_sub(1);
        if corroborators >= CORROBORATION_FLOOR {
            CorroborationReadiness::Armed { corroborators }
        } else {
            CorroborationReadiness::Insufficient {
                corroborators,
                required: CORROBORATION_FLOOR,
            }
        }
    }

    /// Rotate out the OLDEST discovered peer that has outlived [`PEER_LIFETIME`], if any.
    ///
    /// Returns the address ejected, so a caller can log or refill deliberately. This is NC-12's
    /// cycling half and it is driven by AGE alone: a peer that has answered every request is
    /// exactly the peer this removes, because a set that never fails is a set an attacker only has
    /// to capture once. The pool's other eviction, [`eject_peer`](Self::eject_peer), fires on
    /// request FAILURE and cannot substitute for this — a captured peer does not fail.
    ///
    /// Only [`PeerOrigin::Discovered`](connect::PeerOrigin) entries are rotated. A priority entry
    /// is the operator's own node or one on this machine; cycling it would re-dial the same
    /// address, spending a handshake to change nothing.
    ///
    /// One per call, so cycling can never empty the pool in a single sweep.
    pub async fn cycle_expired_peers(&self) -> Option<SocketAddr> {
        let mut entries = self.entries.write().await;
        let now = Instant::now();

        let oldest = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.origin == connect::PeerOrigin::Discovered
                    && now.duration_since(e.admitted_at) >= PEER_LIFETIME
            })
            .min_by_key(|(_, e)| e.admitted_at)
            .map(|(idx, e)| (idx, e.address));

        let (idx, address) = oldest?;
        entries.remove(idx);
        log::debug!("peer {address} rotated out after {PEER_LIFETIME:?} (NC-12 cycling)");
        Some(address)
    }

    /// One maintenance pass: rotate out an over-age peer, then refill toward capacity.
    ///
    /// Cycling before refilling is deliberate. Refilling first would find the pool at capacity and
    /// do nothing, so the rotation would leave a permanently smaller pool.
    pub async fn maintain(&self) {
        self.cycle_expired_peers().await;
        self.try_refill().await;
    }

    /// Admit a connection, or reject it, deciding under the WRITE lock.
    ///
    /// Returns whether it was admitted. Rejected because the pool is full, or because its address
    /// is already held — a pool of N connections to one address reports itself healthy while being
    /// a single point of both failure and deceit (dig_ecosystem#2648).
    ///
    /// **Both checks are made while HOLDING the write lock, and that placement is the whole
    /// correctness of this.** Dials run concurrently, so any check made before acquiring the lock —
    /// under the read lock, or by the caller — is a time-of-check/time-of-use gap: two fills of the
    /// same address each observe it absent, then each pushes, and the duplicate is admitted by
    /// exactly the code written to prevent it. The check and the push must be one critical section.
    async fn admit(&self, peer: Peer, address: SocketAddr, origin: connect::PeerOrigin) -> bool {
        let mut entries = self.entries.write().await;

        if entries.len() >= self.max_peers {
            log::debug!("peer {address} not admitted: pool is at capacity");
            return false;
        }
        if entries.iter().any(|e| e.address == address) {
            log::debug!("peer {address} not admitted: already held");
            return false;
        }

        entries.push(PeerEntry {
            peer,
            address,
            origin,
            admitted_at: Instant::now(),
        });
        log::debug!("peer admitted: {address} ({origin:?})");
        true
    }

    /// If the pool is under capacity, try to connect one new peer.
    /// Also spawns a background task to handle its inbound `NewPeakWallet`
    /// messages.
    pub async fn try_refill(&self) {
        let held: Vec<SocketAddr> = {
            let entries = self.entries.read().await;
            if entries.len() >= self.max_peers {
                return;
            }
            entries.iter().map(|e| e.address).collect()
        };

        // `held` is a hint to the dial, not the guard: it saves dialling an address already in the
        // pool (the local one is offered on every call), and it may be stale the moment it is read.
        // `admit` re-decides under the write lock, which is where the invariant actually holds.
        match connect::connect_random_peer_excluding(
            self.network,
            &self.tls,
            self.connect_timeout,
            &held,
        )
        .await
        {
            Ok((peer, addr, receiver, origin)) => {
                if self.admit(peer, addr, origin).await {
                    // A replacement session is a RECONNECT: bump the generation and announce it
                    // BEFORE the new session can publish anything, so a subscriber never has to
                    // infer a discontinuity from the frames themselves.
                    let generation = self.fanout.begin_generation().await;
                    self.spawn_receiver_handler(generation, receiver);
                    log::debug!("replacement peer connected: {addr}");
                }
            }
            Err(e) => log::warn!("replacement peer connect failed: {e}"),
        }
    }

    // -----------------------------------------------------------------------
    // Receiver helpers (handle NewPeakWallet from peers)
    // -----------------------------------------------------------------------

    /// Spawn a background task that reads inbound messages from one peer session, updating the
    /// shared peak height and fanning every recognised frame out to the pool's subscribers.
    ///
    /// `generation` identifies the session. Every frame this task emits carries it, so a
    /// subscriber can tell frames of a replaced session from frames of the current one.
    pub fn spawn_receiver_handler(
        &self,
        generation: Generation,
        mut receiver: mpsc::Receiver<Message>,
    ) {
        let peak = Arc::clone(&self.peak_height);
        let fanout = Arc::clone(&self.fanout);
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg.msg_type {
                    ProtocolMessageTypes::NewPeakWallet => {
                        if let Ok(new_peak) = NewPeakWallet::from_bytes(&msg.data) {
                            let prev = peak.fetch_max(new_peak.height, Ordering::Relaxed);
                            if new_peak.height > prev {
                                log::debug!("new peak from peer: {}", new_peak.height);
                            }
                            fanout
                                .publish(PoolFrame::Peak {
                                    generation,
                                    height: new_peak.height,
                                    header_hash: new_peak.header_hash,
                                })
                                .await;
                        }
                    }
                    ProtocolMessageTypes::CoinStateUpdate => {
                        if let Ok(update) = CoinStateUpdate::from_bytes(&msg.data) {
                            fanout
                                .publish(PoolFrame::CoinStates {
                                    generation,
                                    height: update.height,
                                    fork_height: update.fork_height,
                                    items: update.items,
                                })
                                .await;
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    /// Subscribe to this pool's frames, with room for `capacity` unread ones.
    ///
    /// Falling further behind than `capacity` ENDS the subscription — see
    /// [`FrameSubscription`](super::frames::FrameSubscription) for why a gap is not an option.
    pub async fn subscribe_frames(&self, capacity: usize) -> FrameSubscription {
        self.fanout.subscribe(capacity).await
    }

    /// The generation frames are currently tagged with.
    pub fn frame_generation(&self) -> Generation {
        self.fanout.generation()
    }
}

/// Construction and admission reachable from OTHER modules' tests.
///
/// [`PeerPool::new`] dials the network, so a test of anything built ON the pool — the backend's
/// absence corroboration, for one — cannot use it. These wrap the private internals rather than
/// widening them, so production code keeps exactly one admission path.
#[cfg(test)]
impl PeerPool {
    pub(crate) fn for_tests(max_peers: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls: connect::create_generated_tls().expect("generate a TLS identity"),
            network: NetworkType::Mainnet,
            connect_timeout: Duration::from_millis(1),
            peak_height: Arc::new(AtomicU32::new(0)),
            fanout: Arc::new(FrameFanout::new()),
        }
    }

    pub(crate) async fn admit_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
    ) -> bool {
        self.admit(peer, address, origin).await
    }

    /// The addresses currently held, in pool order.
    pub(crate) async fn held_addresses_for_tests(&self) -> Vec<SocketAddr> {
        self.entries
            .read()
            .await
            .iter()
            .map(|e| e.address)
            .collect()
    }

    /// Admit a connection as if it had entered the pool at `admitted_at`.
    ///
    /// Age is otherwise only reachable by waiting, and a test that waits five minutes is a test
    /// nobody runs. Wrapping the private field rather than widening it keeps production code on
    /// exactly one admission path.
    pub(crate) async fn admit_at_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
        admitted_at: Instant,
    ) -> bool {
        if !self.admit(peer, address, origin).await {
            return false;
        }
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.address == address) {
            entry.admitted_at = admitted_at;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::connect::{create_generated_tls, PeerOrigin};
    use crate::peer::plurality::{default_max_peers, QUORUM_SAMPLE};
    use crate::peer::test_support::{address, loopback_peer};

    use super::PeerPool as _Pool;
    fn empty_pool(max_peers: usize) -> PeerPool {
        _Pool::for_tests(max_peers)
    }

    /// **The defect, and the one shape that separates a locked re-check from a TOCTOU dedupe.**
    ///
    /// Eight fills of the SAME address are admitted CONCURRENTLY, which is how the pool fills in
    /// production: `PeerPool::new` races `max_peers` dials with no knowledge of each other, and each
    /// may return the same priority address. A dedupe that reads the entry list before taking the
    /// write lock passes a sequential test and fails this one — every task observes the address
    /// absent, then every task pushes.
    ///
    /// `max_peers` is 8, not 1, deliberately: a capacity of one would make the pool reject the
    /// duplicates for being FULL rather than for being duplicates, and would stay green with the
    /// distinctness check deleted entirely.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_address_cannot_fill_the_pool_however_many_fills_race() {
        let pool = Arc::new(empty_pool(8));
        let peer = loopback_peer().await;
        let occupied = address(1);

        let mut fills = Vec::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admit(peer, occupied, PeerOrigin::Priority).await
            }));
        }

        let admitted = futures_util::future::join_all(fills)
            .await
            .into_iter()
            .filter(|r| *r.as_ref().expect("the admission task must not panic"))
            .count();

        assert_eq!(
            admitted, 1,
            "exactly one fill of an address may be admitted"
        );
        assert_eq!(
            pool.peer_count().await,
            1,
            "eight concurrent fills of one address must leave one connection, not eight"
        );
    }

    /// The control that keeps the test above honest: concurrency itself must not cost admissions.
    ///
    /// Without this, an `admit` that rejected everything after the first — or that lost racing
    /// pushes — would satisfy the distinctness test while breaking the pool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_addresses_all_fill_concurrently() {
        let pool = Arc::new(empty_pool(8));
        let peer = loopback_peer().await;

        let mut fills = Vec::new();
        for octet in 1..=8u8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admit(peer, address(octet), PeerOrigin::Discovered)
                    .await
            }));
        }
        futures_util::future::join_all(fills).await;

        assert_eq!(
            pool.peer_count().await,
            8,
            "eight distinct addresses must all be admitted"
        );
    }

    /// Capacity is enforced in the same critical section, so racing fills cannot overshoot it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_fills_never_exceed_max_peers() {
        let pool = Arc::new(empty_pool(3));
        let peer = loopback_peer().await;

        let mut fills = Vec::new();
        for octet in 1..=10u8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admit(peer, address(octet), PeerOrigin::Discovered)
                    .await
            }));
        }
        futures_util::future::join_all(fills).await;

        assert_eq!(pool.peer_count().await, 3, "max_peers is a hard ceiling");
    }

    /// **A preferred peer is not a corroborating one.**
    ///
    /// Two `Discovered` peers sit beside one `Priority` peer, so the two counts differ by exactly
    /// the priority entry. A single-origin fixture cannot show that: all-priority or all-discovered
    /// both make the two counts move together, which an implementation returning `peer_count` for
    /// both would satisfy.
    #[tokio::test]
    async fn a_preferred_peer_is_held_but_not_counted_as_an_independent_opinion() {
        let pool = empty_pool(5);
        let peer = loopback_peer().await;

        assert!(
            pool.admit(peer.clone(), address(1), PeerOrigin::Priority)
                .await
        );
        assert!(
            pool.admit(peer.clone(), address(2), PeerOrigin::Discovered)
                .await
        );
        assert!(pool.admit(peer, address(3), PeerOrigin::Discovered).await);

        assert_eq!(pool.peer_count().await, 3, "three connections are held");
        assert_eq!(
            pool.independent_peer_count().await,
            2,
            "the co-resident peer is held and read from, but is not an independent voice"
        );
    }

    /// An ejected address is admissible again — distinctness must not become a permanent ban.
    #[tokio::test]
    async fn an_ejected_address_can_be_admitted_again() {
        let pool = empty_pool(5);
        let peer = loopback_peer().await;
        let addr = address(1);

        assert!(pool.admit(peer.clone(), addr, PeerOrigin::Discovered).await);
        assert!(
            !pool.admit(peer.clone(), addr, PeerOrigin::Discovered).await,
            "still held, so still a duplicate"
        );

        pool.eject_peer(addr).await;

        assert!(
            pool.admit(peer, addr, PeerOrigin::Discovered).await,
            "a re-dialled peer must be admissible after ejection"
        );
        assert_eq!(pool.peer_count().await, 1);
    }

    /// `max_peers: 0` attempts no connection at all, so the pool is deterministically
    /// empty offline — an exact, network-free fixture for the empty-pool branch.
    async fn pool_with_no_connection_attempts(
        requirement: PeerRequirement,
    ) -> Result<PeerPool, ChiaQueryError> {
        PeerPool::new(
            NetworkType::Mainnet,
            create_generated_tls().expect("generate a TLS identity"),
            0,
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

    /// The fix: with a fallback able to serve, an empty pool must not deny the client.
    #[tokio::test]
    async fn empty_pool_is_tolerated_when_peers_are_optional() {
        let pool = pool_with_no_connection_attempts(PeerRequirement::Optional)
            .await
            .expect("an optional peer pool must construct with zero peers");
        assert!(!pool.has_peers().await);
    }

    /// **The count is what is HELD, never what was asked for.**
    ///
    /// Built by hand rather than through [`PeerPool::new`] so `max_peers` can be a realistic 5
    /// while the pool provably holds nothing — the one shape that separates a measurement from a
    /// configured intention. A `peer_count` that returned `max_peers` would satisfy every
    /// assertion reachable through the offline constructor, whose `max_peers` is necessarily 0,
    /// and would then report "5 peers" on a machine holding none.
    #[tokio::test]
    async fn an_unfilled_pool_counts_what_it_holds_not_the_target_it_was_given() {
        let pool = empty_pool(5);

        assert_eq!(
            pool.peer_count().await,
            0,
            "held is 0 while the target is 5"
        );
        assert!(!pool.has_peers().await);
    }

    /// **NC-12 cycling: an over-age peer is rotated out by the TIMER, with nothing having failed.**
    ///
    /// The distinguishing fixture is the pair. One entry was admitted well past `PEER_LIFETIME`
    /// ago, one moments ago, and BOTH are healthy — no request is made, so `eject_peer`, the
    /// pool's only other eviction, is never reached. A cycler that fired on failure, or one that
    /// swept indiscriminately, changes the outcome here: the first leaves both peers, the second
    /// removes both.
    #[tokio::test]
    async fn an_over_age_peer_is_rotated_out_with_no_request_having_failed() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        let stale = address(1);
        let fresh = address(2);
        let long_past = Instant::now() - (PEER_LIFETIME + Duration::from_secs(100));

        assert!(
            pool.admit_at_for_tests(peer.clone(), stale, PeerOrigin::Discovered, long_past)
                .await
        );
        assert!(
            pool.admit_at_for_tests(peer, fresh, PeerOrigin::Discovered, Instant::now())
                .await
        );

        let rotated = pool.cycle_expired_peers().await;

        assert_eq!(
            rotated,
            Some(stale),
            "the peer past its lifetime must be rotated out on age alone"
        );
        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![fresh],
            "cycling must remove the over-age peer and keep the fresh one"
        );
    }

    /// The control: a pool of healthy, recent peers is left ALONE.
    ///
    /// Without it, a cycler that ejected the oldest entry unconditionally — no lifetime check at
    /// all — would satisfy the test above while churning the pool on every pass.
    #[tokio::test]
    async fn peers_within_their_lifetime_are_not_rotated() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=3u8 {
            assert!(
                pool.admit_at_for_tests(
                    peer.clone(),
                    address(octet),
                    PeerOrigin::Discovered,
                    Instant::now() - (PEER_LIFETIME - Duration::from_secs(30)),
                )
                .await
            );
        }

        assert_eq!(
            pool.cycle_expired_peers().await,
            None,
            "a peer inside its lifetime must not be rotated"
        );
        assert_eq!(pool.peer_count().await, 3);
    }

    /// A priority entry is not rotated: cycling it would re-dial the same address.
    #[tokio::test]
    async fn an_over_age_priority_peer_is_not_rotated() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;
        let long_past = Instant::now() - (PEER_LIFETIME + Duration::from_secs(100));

        assert!(
            pool.admit_at_for_tests(peer, address(1), PeerOrigin::Priority, long_past)
                .await
        );

        assert_eq!(pool.cycle_expired_peers().await, None);
        assert_eq!(pool.peer_count().await, 1);
    }

    /// **The sizing property: one priority entry must not cost the quorum.**
    ///
    /// A pool filled to the SHIPPED default — one priority entry, which is the ordinary case
    /// because the dialler tries the loopback first, and discovered peers for the rest — must
    /// still hold more independent voices than a sample needs. At the previous default of 5 this
    /// fixture yields four, and the assertion fails.
    #[tokio::test]
    async fn one_priority_entry_does_not_cost_the_quorum() {
        let pool = empty_pool(default_max_peers());
        let peer = loopback_peer().await;

        assert!(
            pool.admit(peer.clone(), address(1), PeerOrigin::Priority)
                .await
        );
        for octet in 2..=(default_max_peers() as u8) {
            assert!(
                pool.admit(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(pool.peer_count().await, default_max_peers());
        // A whole sample, plus the one session a subscriber is following and which therefore
        // cannot corroborate itself.
        let owed = QUORUM_SAMPLE + 1;
        let independent = pool.independent_peer_count().await;
        assert!(
            independent >= owed,
            "a full pool holding one priority entry still owes a whole sample plus the session being followed; it holds {independent}"
        );
    }

    /// **Below the floor the pool REFUSES rather than corroborating with fewer voices.**
    ///
    /// Two discovered peers means exactly one corroborator once the answering peer is set aside —
    /// one short. The wrong implementation is not an error, it is a *degradation*: proceeding on
    /// that single second opinion and still calling the result corroborated. So the assertion is
    /// on the refusal AND on the count it reports, which a bare boolean could not distinguish from
    /// an empty pool.
    #[tokio::test]
    async fn a_pool_below_the_corroboration_floor_refuses_rather_than_degrading() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=2u8 {
            assert!(
                pool.admit(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(
            pool.corroboration_readiness().await,
            CorroborationReadiness::Insufficient {
                corroborators: 1,
                required: CORROBORATION_FLOOR,
            }
        );
    }

    /// The control: at the floor exactly, corroboration ARMS.
    ///
    /// Pins the bound from the other side — a gate that refused everything would satisfy the test
    /// above on its own.
    #[tokio::test]
    async fn a_pool_at_the_corroboration_floor_arms() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=(CORROBORATION_FLOOR as u8 + 1) {
            assert!(
                pool.admit(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(
            pool.corroboration_readiness().await,
            CorroborationReadiness::Armed {
                corroborators: CORROBORATION_FLOOR
            }
        );
    }

    /// A preferred peer is not a corroborator, so it cannot arm the gate.
    ///
    /// Same peer COUNT as the arming control above, different origins — the one fixture shape that
    /// separates "enough connections" from "enough independent voices" (dig_ecosystem#2648).
    #[tokio::test]
    async fn priority_peers_cannot_arm_the_corroboration_gate() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        assert!(
            pool.admit(peer.clone(), address(1), PeerOrigin::Discovered)
                .await
        );
        for octet in 2..=(CORROBORATION_FLOOR as u8 + 1) {
            assert!(
                pool.admit(peer.clone(), address(octet), PeerOrigin::Priority)
                    .await
            );
        }

        assert!(matches!(
            pool.corroboration_readiness().await,
            CorroborationReadiness::Insufficient { .. }
        ));
    }
}
