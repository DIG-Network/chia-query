//! [`ChiaLightClient`] — a subscribing Chia wallet-protocol light client that BORROWS the crate's
//! peer pool instead of dialling a connection of its own.
//!
//! # What moved here, and why
//!
//! This is `chia-peer`'s light client, folded into chia-query (dig_ecosystem#2761). `chia-peer`
//! held its own TLS connection, its own DNS-introducer discovery, its own IPv6-first candidate
//! ordering and its own reconnect loop — a second, independently-maintained copy of everything
//! [`crate::peer::connect`] and [`crate::peer::pool`] already do. A node running both therefore
//! held two connections to two independently-chosen full nodes, with two notions of the peak and
//! nothing able to reconcile them.
//!
//! One crate cannot disagree with itself. That is the whole reason this is a MERGE rather than a
//! relocation: `dig-node-core` pinned `chia-query = "=0.5.1"` — an exact-equals pin on a
//! foundation crate — solely so two sibling crates would agree about a third crate's minor. There
//! is now nothing left to disagree.
//!
//! # The subscription follows ONE session, and says which
//!
//! A subscription is server-side state on one connection, so this client PINS a pooled session as
//! its anchor (see [`fetcher`]) and its drive-loop accepts frames from that source ALONE. A
//! `CoinStateUpdate` is an unsolicited push carrying no request id, so accepting one from an
//! unfollowed peer would let any held peer inject coin states this client never asked for,
//! indistinguishable from the ones it did.
//!
//! The pool enforces the same boundary one layer down: a frame subscription is opened ON a session
//! and receives only that session's frames (chia-query#34). Before that, every held peer's frames
//! landed in this client's queue — where an unfollowed peer talking fast could fill it and END the
//! subscription this client was using to follow the peer it had chosen.
//!
//! # NC-12 is untouched
//!
//! Corroboration remains [`PeerPool`](crate::peer::pool::PeerPool)'s: plurality sizing, the
//! `Discovered`-only independent count, [`CorroborationReadiness`], and periodic peer cycling all
//! continue to run over the same pool this client borrows from. Folding a subscriber in adds a
//! borrower; it removes no voices. Nothing here sets a `trusted` flag on a dialled peer — the pool
//! dials with `PeerOptions::default()` and this module does not touch that.

pub mod cache;
pub mod error;
pub mod fetcher;
pub mod provider;

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use chia_protocol::{Bytes32, CoinStateFilters, SpendBundle};
use dig_chainsource_interface::{ProviderId, ProviderInfo, ProviderKind};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use crate::peer::connect::PeerOrigin;
use crate::peer::frames::{FrameSource, FrameSubscription, PoolFrame, SourcedFrame};
use crate::peer::PeerBackend;

use cache::CoinStateCache;
use error::LightClientError;
use fetcher::{CoinStateFetcher, PooledFetcher};

pub use provider::LightClientProvider;

/// The default try-order priority a light-client provider registers with (lower = tried earlier).
///
/// 20 places it ahead of the coinset.org HTTP tier, which is the ordering `chia-peer` established
/// and dig-node depends on: a subscribing session sees a spend land before an HTTP index does.
pub const DEFAULT_PROVIDER_PRIORITY: i32 = 20;

/// How many frames a light client may fall behind before its subscription is terminated.
///
/// Terminating is the intended outcome of overflow, not a failure of it — a SKIPPED
/// `CoinStateUpdate` is a spend the cache never learns about, after which every read reports spent
/// money as present. Sized to absorb a burst of per-block pushes across a full pool while staying
/// far below anything that would make a slow consumer's backlog a memory problem.
const FRAME_BUFFER: usize = 1024;

/// The outcome of submitting a spend bundle, mapped from the node's `TransactionAck`.
///
/// Each refusal carries the node's OWN reason where it gave one, because the reasons are not
/// interchangeable: an unknown parent resolves itself, a fee below the floor never will, and a bad
/// aggregate signature means the bundle must be rebuilt (#48).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Admitted to the mempool (ack status `1`) — pending block confirmation.
    Accepted,
    /// NOT admitted (ack status `2`, Chia's `PENDING`): held for an unknown parent, or declined
    /// below the fee floor. The node is not holding this bundle in its mempool and may never
    /// admit it.
    NotAdmitted { reason: Option<String> },
    /// Rejected by the node (ack status `3`).
    Failed { reason: Option<String> },
    /// An unrecognised ack status byte. Never read as admission.
    Unknown { status: u8, reason: Option<String> },
}

impl SubmitOutcome {
    fn from_ack(status: u8, reason: Option<String>) -> Self {
        match status {
            1 => SubmitOutcome::Accepted,
            2 => SubmitOutcome::NotAdmitted { reason },
            3 => SubmitOutcome::Failed { reason },
            status => SubmitOutcome::Unknown { status, reason },
        }
    }

    /// Whether the bundle was ADMITTED to the mempool.
    ///
    /// True for ack status 1 alone. This formerly also covered status 2, which is the node
    /// declining to admit — so a caller was told its spend was taken when no mempool held it
    /// (#48).
    pub fn is_accepted(&self) -> bool {
        matches!(self, SubmitOutcome::Accepted)
    }

    /// The node's own words for a refusal, where it gave any.
    pub fn reason(&self) -> Option<&str> {
        match self {
            SubmitOutcome::Accepted => None,
            SubmitOutcome::NotAdmitted { reason }
            | SubmitOutcome::Failed { reason }
            | SubmitOutcome::Unknown { reason, .. } => reason.as_deref(),
        }
    }
}

/// A subscribing Chia wallet-protocol light client over a shared [`PeerBackend`].
pub struct ChiaLightClient {
    fetcher: PooledFetcher,
    cache: Arc<RwLock<CoinStateCache>>,
    drive: Option<JoinHandle<()>>,
}

impl ChiaLightClient {
    /// Builds a light client over `backend`'s pool and starts the drive-loop that keeps its
    /// subscription cache current.
    ///
    /// No connection is made here: the pool is already holding sessions, and this client pins one
    /// of them the first time it subscribes.
    pub async fn new(backend: Arc<PeerBackend>, request_timeout: Duration) -> Self {
        let cache = Arc::new(RwLock::new(CoinStateCache::new()));
        // A frame subscription names ONE session (chia-query#34) and this client chooses its
        // session lazily, on the first subscribing read. So the drive-loop is started with nothing
        // to follow and is handed a subscription by each anchoring.
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();
        let fetcher = PooledFetcher::new(backend.clone(), request_timeout, subscriptions_tx);
        let drive = spawn_drive_loop(subscriptions_rx, cache.clone(), fetcher.clone());
        Self {
            fetcher,
            cache,
            drive: Some(drive),
        }
    }

    /// Subscribes to `coin_ids` on the anchor session and seeds the cache with their current state.
    ///
    /// Wraps `request_coin_state(subscribe = true)`; future changes stream back via the drive-loop.
    pub async fn subscribe_coins(&self, coin_ids: Vec<Bytes32>) -> Result<(), LightClientError> {
        let states = self.fetcher.coin_states(coin_ids.clone(), true).await?;
        let mut cache = self.cache.write().await;
        cache.track_coins(coin_ids);
        cache.seed(states);
        Ok(())
    }

    /// Subscribes to every coin paying to `puzzle_hashes` under `filters`, seeding the cache.
    ///
    /// Wraps `request_puzzle_state(subscribe = true)` (paging until finished).
    pub async fn subscribe_puzzle_hashes(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
    ) -> Result<(), LightClientError> {
        let states = self
            .fetcher
            .puzzle_states(puzzle_hashes.clone(), filters, true)
            .await?;
        let mut cache = self.cache.write().await;
        cache.track_puzzle_hashes(puzzle_hashes);
        cache.seed(states);
        Ok(())
    }

    /// Submits `bundle` to the network, mapping the node's ack to a typed [`SubmitOutcome`].
    ///
    /// This is a WRITE path and is deliberately NOT part of the reads-only `ChainSource` surface.
    pub async fn submit_spend(
        &self,
        bundle: SpendBundle,
    ) -> Result<SubmitOutcome, LightClientError> {
        let (status, reason) = self.fetcher.send_transaction(bundle).await?;
        Ok(SubmitOutcome::from_ack(status, reason))
    }

    /// The current peak `(height, header_hash)` as observed on the followed session, if known.
    ///
    /// Deliberately the FOLLOWED peer's peak and not
    /// [`PeerPool::peak_height`](crate::peer::pool::PeerPool::peak_height), which is the highest
    /// any held peer has claimed. The cache's coin states come from one session, and the
    /// no-coin-above-the-peak invariant that keeps a confirmation count from underflowing is only
    /// meaningful if the peak came from the same session as the coins.
    pub async fn peak(&self) -> Option<(u32, Bytes32)> {
        self.cache.read().await.peak()
    }

    /// Removes the subscription to `coin_ids` and stops tracking them locally.
    pub async fn unsubscribe_coins(&self, coin_ids: Vec<Bytes32>) -> Result<(), LightClientError> {
        self.fetcher
            .remove_coin_subscriptions(coin_ids.clone())
            .await?;
        self.cache.write().await.untrack_coins(&coin_ids);
        Ok(())
    }

    /// Whether the subscription set is no longer armed on a session this client is following.
    ///
    /// A consumer polling this can tell a chain with nothing to say from a stream that stopped
    /// talking — the distinction a silent light client otherwise hides.
    ///
    /// True after ANY loss of the anchor, not only after the followed session announced its own
    /// end: a read that fails unpins the anchor too, and from that moment the drive-loop is
    /// applying nothing (chia-query#59). Signalled rather than acted on — re-arming needs the
    /// caller's runtime and its error handling, and a drive-loop that re-subscribed itself would
    /// retry forever against a peer set it cannot see, with nothing able to observe that it was
    /// failing.
    pub fn needs_rearm(&self) -> bool {
        self.fetcher.needs_rearm()
    }

    /// Re-anchors on a live pooled session and re-arms the existing subscription set, so a dropped
    /// connection recovers without the caller re-subscribing.
    ///
    /// Cheaper than its `chia-peer` ancestor by exactly one dial: the pool is already holding
    /// replacement sessions, so this re-issues subscriptions rather than reconnecting.
    pub async fn reconnect(&self) -> Result<(), LightClientError> {
        let (coins, puzzle_hashes) = {
            let cache = self.cache.read().await;
            (cache.subscribed_coins(), cache.subscribed_puzzle_hashes())
        };
        if !coins.is_empty() {
            self.subscribe_coins(coins).await?;
        }
        if !puzzle_hashes.is_empty() {
            self.subscribe_puzzle_hashes(puzzle_hashes, all_coin_states())
                .await?;
        }
        // Cleared only once every re-subscription has SUCCEEDED. Clearing first would report an
        // armed subscription set after a rearm that failed halfway.
        self.fetcher.clear_rearm();
        Ok(())
    }

    /// Exposes the read side as a [`LightClientProvider`] for registration in a chain-source
    /// registry.
    ///
    /// `handle` MUST belong to a multi-thread tokio runtime (the sync facade blocks on it).
    ///
    /// Call this AFTER subscribing. The descriptor's [`ProviderKind`] is read from the session this
    /// client is actually anchored to, and before the first subscription there is none — so an
    /// early call reports the conservative [`Custom`](ProviderKind::Custom) rather than guessing.
    pub async fn as_chain_source_provider(
        &self,
        handle: tokio::runtime::Handle,
    ) -> LightClientProvider {
        LightClientProvider::new(
            Arc::new(self.fetcher.clone()),
            self.cache.clone(),
            handle,
            self.provider_info().await,
        )
    }

    /// The provider descriptor this client registers with.
    ///
    /// [`LocalNode`](ProviderKind::LocalNode) when the answering session was reached from a
    /// configured or co-resident address, [`Custom`](ProviderKind::Custom) when it came from a DNS
    /// introducer. This is what the pool OBSERVED, not what an operator declared — `chia-peer`
    /// derived the same field from a config flag, so a discovered peer answering a
    /// `config.endpoint` client was reported as the operator's own node.
    ///
    /// `trustless` is always `false`: a light-client answer is one peer's word. The registry's
    /// custody view is operator-assigned and fails closed, and this flag is advisory there — it
    /// never grants trust, which is why reporting the origin honestly matters more than the
    /// priority does.
    pub async fn provider_info(&self) -> ProviderInfo {
        let kind = match self.fetcher.current_anchor().await.map(|a| a.origin) {
            Some(PeerOrigin::Priority) => ProviderKind::LocalNode,
            Some(PeerOrigin::Discovered) | None => ProviderKind::Custom,
        };
        ProviderInfo {
            id: ProviderId(Cow::Borrowed("chia-query-light-client")),
            kind,
            priority: DEFAULT_PROVIDER_PRIORITY,
            trustless: false,
        }
    }
}

impl Drop for ChiaLightClient {
    fn drop(&mut self) {
        if let Some(handle) = self.drive.take() {
            handle.abort();
        }
    }
}

/// The filter set a re-arm uses: everything, so a re-subscription cannot narrow what the original
/// subscription covered.
fn all_coin_states() -> CoinStateFilters {
    CoinStateFilters {
        include_spent: true,
        include_unspent: true,
        include_hinted: true,
        min_amount: 0,
    }
}

/// What one turn of the drive-loop's wait resolved to.
///
/// Named rather than folded into the `select!` arms so the borrow of the subscription being read
/// ends with the `select!` expression itself — the arm that ADOPTS a new subscription has to be
/// free to replace the one the other arm is reading from.
enum Turn {
    /// A newly pinned anchor's subscription, or `None` once no further anchoring can arrive.
    Anchored(Option<FrameSubscription>),
    /// A frame from the followed session, or `None` once that subscription has ended.
    Received(Option<SourcedFrame>),
}

/// Spawns the background task that keeps `cache` current from the anchor session's frames.
///
/// It follows ONE session at a time — the anchor — and is handed a fresh subscription over
/// `subscriptions` each time one is pinned. The pool no longer delivers other peers' frames here at
/// all (chia-query#34): a subscription is opened ON a session and receives only that session's
/// frames, so a peer this client never chose can neither be applied to the cache nor fill the queue
/// and end the subscription.
///
/// # Why the two waits are a `select!` and not nested loops
///
/// A newly pinned anchor must be adopted while the PREVIOUS subscription is still open, because
/// nothing forces that one shut. Read failure unpins the anchor
/// ([`discard`](fetcher::PooledFetcher)) but ejecting a peer removes a bookkeeping entry only: it
/// publishes no [`PoolFrame::SessionEnded`], the session's reader task holds a raw message receiver
/// rather than the pooled `Peer`, and `recv` has no deadline. So a peer that simply goes QUIET after
/// one failed read used to hold this loop on its own subscription for as long as its transport
/// stayed open, at no cost to itself — an untrusted dialled peer imposing an indefinite denial by
/// doing nothing (chia-query#59, NC-12). Worse than a stall: the cache froze while every consumer
/// surface still reported it healthy.
///
/// Adopting the newer subscription DROPS the superseded one, which is the correct direction of
/// error here. Re-anchoring a turn too eagerly costs one redundant subscription; waiting on a dead
/// session costs a silently stale cache. The dropped subscription's remaining frames were already
/// unusable — its source is no longer the anchor, so [`follows`] rejects every one of them.
///
/// [`follows`] remains as the second half of chia-query#34's guarantee, and it is not redundant. A
/// subscription outlives the anchoring that created it: between a failed read unpinning the anchor
/// and the next read pinning a new one, this loop is still reading the OLD subscription, and the
/// frames still arriving on it answer questions this client is no longer relying on that peer for.
fn spawn_drive_loop(
    mut subscriptions: mpsc::UnboundedReceiver<FrameSubscription>,
    cache: Arc<RwLock<CoinStateCache>>,
    fetcher: PooledFetcher,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut followed: Option<FrameSubscription> = None;
        // Cleared if the anchoring channel ever closes. The loop keeps draining the session it is
        // already following rather than returning, because abandoning a live subscription is the
        // silent freeze this whole function exists to avoid.
        let mut anchorings_open = true;

        loop {
            let Some(subscription) = followed.as_mut() else {
                if !anchorings_open {
                    return;
                }
                match subscriptions.recv().await {
                    Some(next) => followed = Some(next),
                    None => return,
                }
                continue;
            };
            let source = subscription.source();

            let turn = tokio::select! {
                // Biased towards adopting a new anchor, so the switch happens at the first
                // opportunity rather than a pseudo-random fraction of the time. It cannot starve
                // the frame arm: an anchoring is one message per re-anchor, driven by this client's
                // own reads, and an empty channel resolves to pending immediately. Both `recv`s are
                // cancel-safe, so the arm that loses drops without consuming a message.
                biased;
                anchored = subscriptions.recv(), if anchorings_open => Turn::Anchored(anchored),
                frame = subscription.recv() => Turn::Received(frame),
            };

            match turn {
                Turn::Anchored(Some(next)) => followed = Some(next),
                Turn::Anchored(None) => anchorings_open = false,
                Turn::Received(Some(sourced)) => {
                    if !follows(fetcher.anchor_source().await, sourced.source) {
                        continue;
                    }
                    if let AfterFrame::Resubscribe = apply_frame(&cache, sourced).await {
                        fetcher.release_anchor(source).await;
                    }
                }
                // This subscription ended — the session stopped, the pool was dropped, or this
                // client fell behind the peer it CHOSE and was terminated rather than silently
                // skipped. Either way the cache can no longer be trusted to be current, and saying
                // so is the point (see `frames::FrameSubscription`).
                //
                // Release is guarded on the followed session, so a subscription ending after the
                // client has already re-anchored elsewhere does not unpin the healthy anchor — and
                // it is `release_anchor` that raises the re-arm signal, for exactly the sessions
                // whose loss really left the subscription set unarmed.
                Turn::Received(None) => {
                    fetcher.release_anchor(source).await;
                    followed = None;
                }
            }
        }
    })
}

/// Whether a frame from `source` belongs to the session this client is currently anchored to.
///
/// `None` — nothing pinned — follows NOTHING. Before the first subscription there is no push this
/// client could have asked for, so treating an unanchored client as following everything would admit
/// exactly the unsolicited coin states the anchor exists to exclude.
///
/// The comparison is on the whole [`FrameSource`], address AND session. An address-only test cannot
/// separate a session from a replacement dialled to the same address after a reconnect, which is the
/// common case rather than an exotic one.
fn follows(anchor: Option<FrameSource>, source: FrameSource) -> bool {
    anchor.is_some_and(|anchored| anchored == source)
}

/// What the drive-loop must do about the SESSION after a frame has been applied to the cache.
///
/// Returned rather than done inline so the cache effect of a frame can be tested apart from the
/// session bookkeeping, which needs a live peer to express at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterFrame {
    /// The session continues; nothing further to do.
    Continue,
    /// The followed session is gone. Unpin it and tell the caller to re-arm.
    Resubscribe,
}

/// Applies one attributed frame from the followed session to `cache`.
async fn apply_frame(cache: &RwLock<CoinStateCache>, sourced: SourcedFrame) -> AfterFrame {
    match sourced.frame {
        // The `Reset` that OPENS this subscription, and the only one it can now see: a subscription
        // names one session, so a new connection at the followed address is a different source that
        // this subscription never receives (chia-query#34). It arrives immediately after the anchor
        // was pinned and the re-arm it would once have signalled is already under way, so treating
        // it as a reason to re-arm would unpin a healthy anchor on every single anchoring.
        //
        // The fact it used to carry has not been lost, it has moved: a replaced session announces
        // itself to this client as `SessionEnded` on the subscription that is ending, below.
        PoolFrame::Reset => AfterFrame::Continue,
        PoolFrame::Peak {
            height,
            header_hash,
        } => {
            cache.write().await.set_peak(height, header_hash);
            AfterFrame::Continue
        }
        PoolFrame::CoinStates {
            height,
            fork_height,
            peak_hash,
            items,
        } => {
            let spent: Vec<Bytes32> = items
                .iter()
                .filter(|state| state.spent_height.is_some())
                .map(|state| state.coin.coin_id())
                .collect();
            let mut cache = cache.write().await;
            cache.apply_update(&items, height, fork_height, peak_hash);
            cache.untrack_coins(&spent);
            AfterFrame::Continue
        }
        PoolFrame::SessionEnded { reason } => {
            log::debug!(
                "light-client session {:?} ended: {reason:?}",
                sourced.source.session
            );
            AfterFrame::Resubscribe
        }
    }
}

#[cfg(test)]
mod tests;
