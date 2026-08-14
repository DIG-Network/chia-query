use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chia_protocol::{Message, NewPeakWallet, ProtocolMessageTypes};
use chia_traits::Streamable;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, RwLock};

use chia_wallet_sdk::client::Peer;
use tokio_tungstenite::Connector;

use crate::types::ChiaQueryError;
use crate::NetworkType;

use super::connect;

// ---------------------------------------------------------------------------
// Pool entry
// ---------------------------------------------------------------------------

struct PeerEntry {
    peer: Peer,
    address: SocketAddr,
    /// How this peer was reached. Held so a caller counting independent opinions can tell a
    /// preferred local node from a discovered one — see [`connect::PeerOrigin`].
    origin: connect::PeerOrigin,
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
                pool.spawn_receiver_handler(receiver);
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
                    self.spawn_receiver_handler(receiver);
                    log::debug!("replacement peer connected: {addr}");
                }
            }
            Err(e) => log::warn!("replacement peer connect failed: {e}"),
        }
    }

    // -----------------------------------------------------------------------
    // Receiver helpers (handle NewPeakWallet from peers)
    // -----------------------------------------------------------------------

    /// Spawn a background task that reads inbound messages from a peer's
    /// receiver channel and updates the shared peak height.  This mirrors
    /// the pattern used by chia-block-listener.
    pub fn spawn_receiver_handler(&self, mut receiver: mpsc::Receiver<Message>) {
        let peak = Arc::clone(&self.peak_height);
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if msg.msg_type == ProtocolMessageTypes::NewPeakWallet {
                    if let Ok(new_peak) = NewPeakWallet::from_bytes(&msg.data) {
                        let prev = peak.fetch_max(new_peak.height, Ordering::Relaxed);
                        if new_peak.height > prev {
                            log::debug!("new peak from peer: {}", new_peak.height);
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::connect::{create_generated_tls, PeerOrigin};

    /// A pool holding nothing, with a realistic `max_peers`, ready to be filled by hand.
    ///
    /// Built directly rather than through [`PeerPool::new`] because the constructor dials the
    /// network; admission is what these tests are about, and it is reachable without one.
    fn empty_pool(max_peers: usize) -> PeerPool {
        PeerPool {
            entries: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls: create_generated_tls().expect("generate a TLS identity"),
            network: NetworkType::Mainnet,
            connect_timeout: Duration::from_millis(1),
            peak_height: Arc::new(AtomicU32::new(0)),
        }
    }

    /// A real [`Peer`], built over a genuine loopback websocket rather than mocked.
    ///
    /// `Peer::from_websocket` reads the socket's own `peer_addr`, so there is no way to construct
    /// one without a live socket. The returned peer is CLONEABLE (`Peer` is an `Arc` inside), which
    /// is what lets a test offer the *same* connection under several addresses — the shape a
    /// duplicate actually takes.
    async fn loopback_peer() -> Peer {
        use tokio::net::{TcpListener, TcpStream};
        use tokio_tungstenite::MaybeTlsStream;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback listener");
        let addr = listener.local_addr().expect("read the listener address");

        // Hold the server side open for the life of the test; dropping it would close the
        // connection under the peer being tested.
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                    let _keep_open = ws;
                    std::future::pending::<()>().await;
                }
            }
        });

        let stream = TcpStream::connect(addr).await.expect("dial the listener");
        let (ws, _) = tokio_tungstenite::client_async(
            format!("ws://{addr}/ws"),
            MaybeTlsStream::Plain(stream),
        )
        .await
        .expect("complete the websocket handshake");

        let (peer, _receiver) =
            Peer::from_websocket(ws, Default::default()).expect("build a peer from the websocket");
        peer
    }

    fn address(last_octet: u8) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, last_octet)),
            8444,
        )
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
}
