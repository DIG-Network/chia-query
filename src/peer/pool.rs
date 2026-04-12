use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chia::protocol::{Message, NewPeakWallet, ProtocolMessageTypes};
use chia::traits::Streamable;
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
    /// concurrently.  At least one peer must succeed; otherwise we return
    /// [`ChiaQueryError::PeerDiscoveryFailed`].
    pub async fn new(
        network: NetworkType,
        tls: Connector,
        max_peers: usize,
        connect_timeout: Duration,
    ) -> Result<Self, ChiaQueryError> {
        let peak_height = Arc::new(AtomicU32::new(0));

        // Connect to peers concurrently.
        let mut futures = FuturesUnordered::new();
        for _ in 0..max_peers {
            let t = tls.clone();
            futures.push(async move {
                connect::connect_random_peer(network, &t, connect_timeout).await
            });
        }

        let mut initial: Vec<PeerEntry> = Vec::new();
        let mut receivers = Vec::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok((peer, addr, receiver)) => {
                    initial.push(PeerEntry { peer, address: addr });
                    receivers.push(receiver);
                }
                Err(e) => log::debug!("initial peer connect failed: {e}"),
            }
        }

        if initial.is_empty() {
            return Err(ChiaQueryError::PeerDiscoveryFailed);
        }

        let pool = Self {
            entries: RwLock::new(initial),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls,
            network,
            connect_timeout,
            peak_height,
        };

        // Spawn receiver handlers for initial peers (must happen after pool
        // construction so peak_height Arc is available).
        for receiver in receivers {
            pool.spawn_receiver_handler(receiver);
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

    /// If the pool is under capacity, try to connect one new peer.
    /// Also spawns a background task to handle its inbound `NewPeakWallet`
    /// messages.
    pub async fn try_refill(&self) {
        let current = self.entries.read().await.len();
        if current >= self.max_peers {
            return;
        }
        match connect::connect_random_peer(self.network, &self.tls, self.connect_timeout).await {
            Ok((peer, addr, receiver)) => {
                self.spawn_receiver_handler(receiver);
                let mut entries = self.entries.write().await;
                if entries.len() < self.max_peers {
                    entries.push(PeerEntry {
                        peer,
                        address: addr,
                    });
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
