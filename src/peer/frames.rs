//! Per-subscriber fan-out of the frames arriving on a pooled peer's session.
//!
//! # Why the pool needs this at all
//!
//! A pooled session's inbound `mpsc::Receiver<Message>` is consumed by ONE task. Before this
//! module that task folded `NewPeakWallet` into an atomic and discarded everything else, so a
//! consumer that needs the frames themselves — a wallet replica following `CoinStateUpdate` —
//! could not be served from a pooled session and had to dial its own. That is the reason the node
//! ran several independent peer stacks (dig_ecosystem#2761).
//!
//! # Every frame names the session it came from
//!
//! The pool holds many peers at once and fans all of their frames into one subscription, so a
//! frame that does not say who sent it is a claim from *the pool*, which no peer in it is entitled
//! to make. `CoinStateUpdate` is an UNSOLICITED push carrying no request id, so an unattributed
//! fan-out lets any held peer inject fabricated coin states that a subscriber cannot tell from the
//! peer it deliberately followed — and cannot eject, because nothing knows who sent them.
//!
//! Attribution lives on [`SourcedFrame`], the envelope, rather than on the individual
//! [`PoolFrame`] variants. A variant added later cannot forget to carry it, and a subscriber
//! cannot read a frame without having its source in hand.
//!
//! # Sessions, not a pool-wide generation
//!
//! A [`SessionId`] identifies ONE peer connection for the life of the pool. It is allocated when
//! that connection is admitted and never changes, so the identity a frame carries stays true
//! whatever else the pool does afterwards.
//!
//! This is deliberately not a pool-wide counter. A pool of N independent sessions has no single
//! "current" generation to be in: a counter bumped by every reconnect makes every OTHER session's
//! frames look stale, so a consumer honouring it discards N-1 peers' frames on every ordinary
//! refill. Staleness is a property of one peer's stream, and it is signalled on that stream.
//!
//! [`PoolFrame::Reset`] opens a session, [`PoolFrame::SessionEnded`] closes it, and between them
//! everything a subscriber sees from that source belongs to one continuous connection.
//!
//! # Overflow terminates the subscriber; it never skips a frame
//!
//! Each subscriber gets a BOUNDED channel, because an unbounded one turns a slow consumer into
//! unbounded memory. When that channel is full the subscription is DROPPED and its receiver
//! observes the stream end.
//!
//! The tempting alternative — drop the frame, keep the subscription — is the failure this ordering
//! exists to prevent. A missed `CoinStateUpdate` is a coin whose spend the replica never learns
//! about, so the replica goes on reporting `Synced` while reading spent money as present. A
//! terminated stream is a fact the consumer can act on; a gap is indistinguishable from quiet.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chia_protocol::{Bytes32, CoinState};
use tokio::sync::{mpsc, Mutex};

/// One peer connection, for the life of the pool.
///
/// A newtype rather than a bare `u64` so a session cannot be confused with a height, which is the
/// other monotonically increasing number every frame carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);

/// WHO a frame came from.
///
/// Both halves are load-bearing and neither substitutes for the other. The `address` is what a
/// subscriber matches against the peer it chose to follow, and what it hands to
/// [`PeerPool::eject_peer`](super::pool::PeerPool::eject_peer) to remove a peer whose frames it
/// rejected. The `session` distinguishes two connections to the same address across a reconnect,
/// so a late frame from a replaced session cannot be mistaken for a frame of its replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameSource {
    pub address: SocketAddr,
    pub session: SessionId,
}

/// Why a session stopped producing frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndReason {
    /// The transport closed: the peer went away, or the connection dropped.
    Disconnected,
    /// The peer sent a message whose type this crate recognises but whose body it could not
    /// decode.
    ///
    /// The session ends rather than the frame being skipped. What a malformed message CONTAINED is
    /// exactly what cannot be known, so continuing would leave the subscriber's state missing an
    /// update it would never learn it missed — the gap this module exists to prevent — and a peer
    /// able to induce that at will chooses when the replica goes quietly wrong.
    UndecodableFrame,
    /// The peer sent a well-formed frame carrying more coin states than
    /// [`MAX_FRAME_COIN_STATES`] allows.
    ///
    /// A DIFFERENT fact from [`UndecodableFrame`](Self::UndecodableFrame), and reported separately
    /// rather than folded into it: that one says the bytes could not be read, this one says they
    /// were read and the peer asked for more work than any honest peer needs. An operator
    /// diagnosing a dropped session, and a consumer deciding whether to redial the same address,
    /// want to tell "this peer is broken" from "this peer is hostile".
    OversizedFrame,
}

/// The most coin states one [`PoolFrame::CoinStates`] may carry before the session is ENDED.
///
/// # Derived from the protocol, not chosen
///
/// A Chia full node caps ONE subscription response at `max_subscribe_response_items = 100_000`
/// states, so that is the node's own ceiling on how much a single answer may carry and no honest
/// peer needs more than it in one frame. Taking the node's number rather than a round one is what
/// makes this a bound on a hostile peer instead of a second, tighter policy that honest peers must
/// also satisfy.
///
/// # Deliberately LARGE, because the starving direction is the dangerous one here
///
/// A per-block `CoinStateUpdate` is bounded far below this by block cost — low tens of thousands
/// of coin changes at the very most — so the cap is nowhere near an honest update. Erring small
/// would terminate honest sessions during ordinary chain activity, which is a bound that starves
/// the work it protects; erring large costs one frame's memory once.
///
/// One frame's memory is the whole exposure, and that is a property of chia-query#34's partition
/// rather than of this number: a frame is now cloned once per subscriber FOLLOWING its own source
/// — normally one — so `O(items x subscribers)` is already gone. This bounds what a single frame
/// can cost; it is not, and must not be relied on as, the bound on how that cost scales.
pub const MAX_FRAME_COIN_STATES: usize = 100_000;

/// One frame from a pooled peer session, as a subscriber sees it.
///
/// Carried inside a [`SourcedFrame`], which names the session it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolFrame {
    /// This session has BEGUN. Anything derived from an earlier session at the same address is
    /// stale.
    ///
    /// Delivered before any other frame of the session, never after one.
    Reset,
    /// A peer announced a new peak.
    Peak { height: u32, header_hash: Bytes32 },
    /// A peer reported coin states changing at `height`.
    CoinStates {
        height: u32,
        fork_height: u32,
        /// The header hash of the peak this update was observed against.
        ///
        /// Carried because `CoinStateUpdate` carries it and a subscriber tracking the peak needs
        /// the height and the hash to arrive TOGETHER. Dropping it forces a subscriber to pair the
        /// new height with whatever hash it already had, which names a block that never existed at
        /// that height — and does so most often during a reorg, exactly when the pairing is what a
        /// consumer is relying on.
        peak_hash: Bytes32,
        items: Vec<CoinState>,
    },
    /// This session has ENDED and will produce no further frames.
    ///
    /// A subscriber following this source learns that its stream stopped, rather than being left
    /// with a silence it cannot distinguish from a quiet chain.
    SessionEnded { reason: SessionEndReason },
}

/// A frame together with the session that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcedFrame {
    pub source: FrameSource,
    pub frame: PoolFrame,
}

/// The receiving half of a subscription.
///
/// [`recv`](Self::recv) returning `None` means the subscription ENDED — either the pool was
/// dropped or this subscriber fell behind and was terminated rather than silently skipped. A
/// consumer that treats `None` as "nothing more to do" is reading a desync as quiet; treat it as
/// "resubscribe and rebuild".
pub struct FrameSubscription {
    source: FrameSource,
    receiver: mpsc::Receiver<SourcedFrame>,
}

impl FrameSubscription {
    /// The session this subscription follows.
    ///
    /// Carried on the subscription rather than left to be read off the first frame, so a consumer
    /// that PINS this session can name it — including its [`SessionId`] — before it has received
    /// anything. Comparing the whole source is what distinguishes a session from its replacement at
    /// the same address; an address alone cannot, and a consumer holding only the address tears down
    /// a healthy new anchor when its predecessor's death is finally announced.
    pub fn source(&self) -> FrameSource {
        self.source
    }

    /// The next frame, or `None` once the subscription has ended.
    pub async fn recv(&mut self) -> Option<SourcedFrame> {
        self.receiver.recv().await
    }

    /// The next frame if one is already queued.
    pub fn try_recv(&mut self) -> Result<SourcedFrame, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

/// The pool's fan-out: subscribers partitioned BY SOURCE, each with its own bounded queue.
///
/// # A subscription follows ONE session, and sees only that session's frames
///
/// The partition is by [`FrameSource`], not by subscriber, and that is the whole security of it.
/// Overflow terminates a subscriber (above), and before this the frames that filled a subscriber's
/// queue came from EVERY held session — so a peer that talked fast could push a subscriber over
/// its capacity and end the subscription that subscriber was using to follow a DIFFERENT peer. A
/// cross-peer denial primitive, reachable by a peer nobody chose (chia-query#34, NC-12).
///
/// The queueing was never useful work either: `RequestPuzzleState{subscribe: true}` is server-side
/// state on the connection it was sent on, and a `CoinStateUpdate` carries no request id, so an
/// update from any OTHER held peer is unattributable to any subscription this process holds. SPEC
/// 5h already obliges a subscriber to discard every frame whose source is not its own. The fan-out
/// was queueing frames the contract requires be thrown away, and then terminating the consumer for
/// receiving them.
///
/// So the fix is to stop DELIVERING them rather than to ration them. A per-source credit budget or
/// a shared queue with a smarter eviction policy both keep the shared queue and add a limiter
/// around it — and a limiter keyed on which peer is talking is state an untrusted peer influences,
/// which is a denial primitive in its own right. Here the only peer that can end a subscription is
/// the one the subscriber CHOSE, which invariant 5e already lets it name and eject.
///
/// # A source is registered while it is LIVE, and only then
///
/// The map holds an entry for a source from [`open_session`] until its
/// [`PoolFrame::SessionEnded`] is published, and [`subscribe`](Self::subscribe) returns `None` for
/// any source without one. That is what stops a subscription attaching to a session that has
/// already stopped — which would otherwise hang forever waiting for an end frame that was
/// published before it arrived — and it bounds the map to the sessions actually running.
///
/// [`open_session`]: Self::open_session
pub struct FrameFanout {
    subscribers: Mutex<HashMap<FrameSource, Vec<mpsc::Sender<SourcedFrame>>>>,
    next_session: AtomicU64,
}

impl Default for FrameFanout {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameFanout {
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(HashMap::new()),
            next_session: AtomicU64::new(0),
        }
    }

    /// Follow `source`, with room for `capacity` unread frames.
    ///
    /// `None` when `source` is not a live session — it was never opened, or it has already ended.
    /// A subscription to a stopped session could never be closed by a `SessionEnded` that was
    /// published before it existed, so it is refused rather than left waiting on a frame that will
    /// never come.
    ///
    /// `capacity` is the consumer's own promise about how far behind it may fall: beyond it the
    /// subscription is terminated rather than thinned. Only frames from `source` are counted
    /// against it.
    ///
    /// The subscription opens with a synthesised [`PoolFrame::Reset`], queued from INSIDE this
    /// call so it precedes anything the session's task publishes next. Invariant 5e — a session is
    /// announced before its first frame — therefore holds per SUBSCRIPTION, not merely per
    /// session: a subscriber that joins mid-session is told, in its own stream, that everything it
    /// may hold from an earlier session at this address is stale.
    pub async fn subscribe(
        self: &Arc<Self>,
        source: FrameSource,
        capacity: usize,
    ) -> Option<FrameSubscription> {
        let (sender, mut receiver) = mpsc::channel(capacity.max(1));
        let mut subscribers = self.subscribers.lock().await;
        let followers = subscribers.get_mut(&source)?;

        // Queued before the sender is registered, so this subscriber's Reset cannot be preceded by
        // a frame published in the gap. `capacity >= 1`, so it always fits.
        let opened = sender
            .try_send(SourcedFrame {
                source,
                frame: PoolFrame::Reset,
            })
            .is_ok();
        debug_assert!(opened, "a channel of capacity >= 1 accepts its first frame");
        if !opened {
            receiver.close();
            return None;
        }

        followers.push(sender);
        Some(FrameSubscription { source, receiver })
    }

    /// How many subscriptions are still live, across every source.
    pub async fn subscriber_count(&self) -> usize {
        self.subscribers
            .lock()
            .await
            .values()
            .map(Vec::len)
            .sum::<usize>()
    }

    /// Allocate the identity of a new session at `address`.
    ///
    /// Allocation publishes nothing and registers nothing: a connection is identified before the
    /// pool decides whether to admit it, so a rejected duplicate cannot announce a session that
    /// never ran. [`open_session`](Self::open_session) announces an admitted one.
    pub fn allocate_session(&self, address: SocketAddr) -> FrameSource {
        FrameSource {
            address,
            session: SessionId(self.next_session.fetch_add(1, Ordering::Relaxed)),
        }
    }

    /// Register `source` as LIVE and announce that it has begun.
    ///
    /// Registration is what makes the session subscribable; until it runs,
    /// [`subscribe`](Self::subscribe) answers `None`. The [`PoolFrame::Reset`] published here
    /// reaches whoever is already following — nobody, since nothing can subscribe to an
    /// unregistered source — and is kept because it is the frame that states the invariant at the
    /// session's own layer.
    pub async fn open_session(&self, source: FrameSource) {
        self.subscribers.lock().await.entry(source).or_default();
        self.publish(source, PoolFrame::Reset).await;
    }

    /// Deliver `frame` from `source` to the subscribers FOLLOWING `source`, terminating any that
    /// has fallen behind.
    ///
    /// A subscriber whose queue is FULL is removed, which drops the sender and ends its stream. A
    /// subscriber whose receiver is already gone is removed too; that one is ordinary tidying.
    /// Both are scoped to `source`: a subscriber following a different session is not reachable
    /// from here at all, which is the point (chia-query#34).
    ///
    /// A [`PoolFrame::SessionEnded`] additionally DEREGISTERS the source. Its followers have the
    /// end frame queued, their senders drop, and each `recv` returns `None` once it has read that
    /// frame — a session-scoped subscription has exactly the session's lifetime, and there is
    /// nothing after it to wait for. A frame published for an unregistered source is discarded,
    /// which is the correct reading of "no live session produced this".
    pub async fn publish(&self, source: FrameSource, frame: PoolFrame) {
        let ends_session = matches!(frame, PoolFrame::SessionEnded { .. });
        let sourced = SourcedFrame { source, frame };
        let mut subscribers = self.subscribers.lock().await;

        let Some(followers) = subscribers.get_mut(&source) else {
            return;
        };
        followers.retain(|sender| match sender.try_send(sourced.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!(
                    "frame subscriber fell behind the session it follows; terminating its \
                     subscription rather than dropping a frame it would never learn it missed"
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });

        if ends_session {
            subscribers.remove(&source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, last)), 8444)
    }

    /// Allocate AND open a session, which is what makes it subscribable.
    async fn live_session(fanout: &Arc<FrameFanout>, last: u8) -> FrameSource {
        let source = fanout.allocate_session(addr(last));
        fanout.open_session(source).await;
        source
    }

    fn peak(height: u32) -> PoolFrame {
        PoolFrame::Peak {
            height,
            header_hash: Bytes32::new([height as u8; 32]),
        }
    }

    fn ended() -> PoolFrame {
        PoolFrame::SessionEnded {
            reason: SessionEndReason::Disconnected,
        }
    }

    /// Everything currently queued for `subscription`, without awaiting.
    fn drained(subscription: &mut FrameSubscription) -> Vec<SourcedFrame> {
        let mut seen = Vec::new();
        while let Ok(sourced) = subscription.try_recv() {
            seen.push(sourced);
        }
        seen
    }

    fn frames_of(seen: &[SourcedFrame]) -> Vec<PoolFrame> {
        seen.iter().map(|f| f.frame.clone()).collect()
    }

    /// **A peer that is not followed CANNOT end a subscription — chia-query#34's done condition.**
    ///
    /// The subscriber follows Y with a capacity of 4 and X floods 64 frames while Y is quiet. Under
    /// the shared queue this module replaced, X's frames landed in the follower's queue, overflowed
    /// it, and terminated the subscription Y's follower was using — a cross-peer denial primitive
    /// from a peer nobody chose (NC-12).
    ///
    /// The assertions are built so the nearest wrong implementations both fail: a shared queue
    /// terminates the subscription, so no frame from Y arrives at all; a shared queue that DROPPED
    /// frames instead of terminating would deliver X's frames here, which the source assertion
    /// catches.
    #[tokio::test]
    async fn a_peer_that_is_not_followed_cannot_end_the_subscription() {
        let fanout = Arc::new(FrameFanout::new());
        let followed = live_session(&fanout, 1).await;
        let flooder = live_session(&fanout, 2).await;

        let mut subscription = fanout
            .subscribe(followed, 4)
            .await
            .expect("the followed session is live");

        for height in 1..=64u32 {
            fanout.publish(flooder, peak(height)).await;
        }
        fanout.publish(followed, peak(500)).await;

        let seen = drained(&mut subscription);

        assert!(
            seen.iter().all(|f| f.source == followed),
            "a subscription must receive ONLY the frames of the session it follows: {seen:?}"
        );
        assert_eq!(
            frames_of(&seen),
            vec![PoolFrame::Reset, peak(500)],
            "the subscription opens with its own Reset and then receives the followed peer's frame"
        );
        assert_eq!(
            fanout.subscriber_count().await,
            1,
            "the subscription must still be live after 64 frames from a peer it never chose"
        );
    }

    /// **The control, and the half that stops the fix becoming "drop frames".**
    ///
    /// A subscriber that cannot keep up with the peer it CHOSE is still terminated (invariant 5d,
    /// unchanged). Without this test, a `publish` that silently thinned every queue would satisfy
    /// the test above.
    #[tokio::test]
    async fn the_followed_peer_can_still_terminate_its_own_follower() {
        let fanout = Arc::new(FrameFanout::new());
        let followed = live_session(&fanout, 1).await;

        let mut subscription = fanout
            .subscribe(followed, 4)
            .await
            .expect("the followed session is live");

        for height in 1..=64u32 {
            fanout.publish(followed, peak(height)).await;
        }

        let seen = drained(&mut subscription);

        assert!(
            seen.len() <= 4,
            "no more than `capacity` frames may be delivered before the overflow: {seen:?}"
        );
        assert_eq!(
            subscription.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected),
            "the subscription must be TERMINATED, not merely empty with frames silently skipped"
        );
        assert_eq!(
            fanout.subscriber_count().await,
            0,
            "the overflowing subscription must be dropped, not retained and thinned"
        );
    }

    /// **A subscription opens with a `Reset` of its OWN, before any other frame.**
    ///
    /// Invariant 5e holds per SUBSCRIPTION, not merely per session: a subscriber joining
    /// mid-session predates the session's own `Reset`, so without a synthesised one it would build
    /// state on top of whatever it already held for that address. The assertion is on the ORDER,
    /// which the frames' contents alone cannot show.
    #[tokio::test]
    async fn a_subscription_opens_with_a_reset_before_its_first_frame() {
        let fanout = Arc::new(FrameFanout::new());
        let session = live_session(&fanout, 1).await;

        // The session has been running for a while before anyone follows it.
        fanout.publish(session, peak(100)).await;

        let mut subscription = fanout
            .subscribe(session, 8)
            .await
            .expect("the session is live");
        fanout.publish(session, peak(101)).await;

        let frames = frames_of(&drained(&mut subscription));

        assert_eq!(
            frames,
            vec![PoolFrame::Reset, peak(101)],
            "the subscription must open with a Reset and then see only what was published after \
             it joined: {frames:?}"
        );
    }

    /// **`SessionEnded` closes the subscription, and the subscriber READS it first.**
    ///
    /// The end frame must be delivered before the stream stops, or the consumer cannot tell a peer
    /// that stopped from a chain that is quiet — the distinction this module exists to preserve.
    #[tokio::test]
    async fn session_end_is_delivered_and_then_closes_the_subscription() {
        let fanout = Arc::new(FrameFanout::new());
        let session = live_session(&fanout, 1).await;

        let mut subscription = fanout
            .subscribe(session, 8)
            .await
            .expect("the session is live");

        fanout.publish(session, ended()).await;

        let frames = frames_of(&drained(&mut subscription));

        assert_eq!(
            frames,
            vec![PoolFrame::Reset, ended()],
            "the end frame must be READ before the stream stops: {frames:?}"
        );
        assert_eq!(
            subscription.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected),
            "and nothing follows it"
        );
        assert_eq!(
            fanout.subscriber_count().await,
            0,
            "a session-scoped subscription has exactly the session's lifetime"
        );
    }

    /// **A subscription binds a SESSION, not an address.**
    ///
    /// A reconnect to the same address is a DIFFERENT peer as far as anything this crate can
    /// verify. A subscription that followed the address would be silently re-pointed at a new
    /// peer's claims while the consumer went on believing it was reading the one it chose.
    #[tokio::test]
    async fn a_subscription_binds_a_session_and_not_an_address() {
        let fanout = Arc::new(FrameFanout::new());
        let first = live_session(&fanout, 1).await;

        let mut subscription = fanout
            .subscribe(first, 8)
            .await
            .expect("the first session is live");

        fanout.publish(first, ended()).await;

        // A replacement is dialled at the SAME address.
        let second = live_session(&fanout, 1).await;
        assert_eq!(second.address, first.address);
        assert_ne!(second.session, first.session);
        fanout.publish(second, peak(700)).await;

        let seen = drained(&mut subscription);

        assert!(
            seen.iter().all(|f| f.source == first),
            "the old subscription must receive nothing from the session that replaced it: {seen:?}"
        );
        assert_eq!(
            frames_of(&seen),
            vec![PoolFrame::Reset, ended()],
            "it saw its own session open and close, and nothing else"
        );
        assert_eq!(
            subscription.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected),
            "it ended with its own session"
        );
    }

    /// **A session that has already ENDED cannot be followed.**
    ///
    /// Its `SessionEnded` was published before the subscription existed, so a subscription created
    /// afterwards would wait forever for an end frame that has already gone — silence a consumer
    /// reads as a quiet chain. Refusing is the honest answer, and it is what bounds the fan-out's
    /// map to the sessions actually running.
    #[tokio::test]
    async fn a_session_that_has_ended_cannot_be_followed() {
        let fanout = Arc::new(FrameFanout::new());
        let session = live_session(&fanout, 1).await;

        fanout.publish(session, ended()).await;

        assert!(
            fanout.subscribe(session, 8).await.is_none(),
            "a stopped session must not be subscribable"
        );

        let never_opened = fanout.allocate_session(addr(9));
        assert!(
            fanout.subscribe(never_opened, 8).await.is_none(),
            "an allocated but unannounced session must not be subscribable either"
        );
    }

    /// **Two followers of the SAME session both receive its frames.**
    ///
    /// The partition is by source, not one subscriber per source: a per-source slot that only ever
    /// held its last registrant would pass every test above and silently drop one consumer.
    #[tokio::test]
    async fn every_follower_of_a_session_receives_its_frames() {
        let fanout = Arc::new(FrameFanout::new());
        let session = live_session(&fanout, 1).await;

        let mut first = fanout.subscribe(session, 8).await.expect("live");
        let mut second = fanout.subscribe(session, 8).await.expect("live");

        fanout.publish(session, peak(100)).await;

        for subscription in [&mut first, &mut second] {
            assert_eq!(
                frames_of(&drained(subscription)),
                vec![PoolFrame::Reset, peak(100)]
            );
        }
        assert_eq!(fanout.subscriber_count().await, 2);
    }

    /// **Overflow terminates ONLY the follower that overflowed.**
    ///
    /// Two consumers follow the same peer and one stops reading. Removing every follower on the
    /// first overflow — the shape a `retain` over one shared queue makes easy to write — would take
    /// the healthy consumer down with it.
    #[tokio::test]
    async fn overflow_terminates_only_the_follower_that_overflowed() {
        let fanout = Arc::new(FrameFanout::new());
        let session = live_session(&fanout, 1).await;

        let mut slow = fanout.subscribe(session, 2).await.expect("live");
        let mut healthy = fanout.subscribe(session, 64).await.expect("live");

        for height in 1..=32u32 {
            fanout.publish(session, peak(height)).await;
            // The healthy consumer keeps up; the slow one never reads.
            while healthy.try_recv().is_ok() {}
        }

        while slow.try_recv().is_ok() {}
        assert_eq!(
            slow.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected),
            "the slow consumer is terminated"
        );
        assert_eq!(
            fanout.subscriber_count().await,
            1,
            "and the consumer that kept up is still subscribed"
        );

        fanout.publish(session, peak(999)).await;
        assert_eq!(
            healthy.try_recv().map(|f| f.frame),
            Ok(peak(999)),
            "the healthy consumer still receives frames after its sibling was terminated"
        );
    }

    /// **Frames still name the peer that sent them.**
    ///
    /// Attribution is what lets a subscriber reject and EJECT a peer whose frames it does not
    /// accept (invariant 5e). Source-scoping the delivery does not make the envelope redundant:
    /// two sessions at DIFFERENT addresses must stay distinguishable, and a `FrameSource`
    /// hard-coded to anything at all would fail here.
    #[tokio::test]
    async fn a_frame_names_the_peer_that_sent_it() {
        let fanout = Arc::new(FrameFanout::new());
        let honest = live_session(&fanout, 1).await;
        let other = live_session(&fanout, 2).await;

        let mut following_honest = fanout.subscribe(honest, 8).await.expect("live");
        let mut following_other = fanout.subscribe(other, 8).await.expect("live");

        fanout.publish(honest, peak(100)).await;
        fanout.publish(other, peak(999)).await;

        let from_honest = drained(&mut following_honest);
        let from_other = drained(&mut following_other);

        assert!(from_honest.iter().all(|f| f.source.address == addr(1)));
        assert!(from_other.iter().all(|f| f.source.address == addr(2)));
        assert_ne!(
            honest, other,
            "two peers must not share one frame identity, or an injected frame is \
             indistinguishable from the followed peer's"
        );
        assert_eq!(
            from_other.last().map(|f| f.frame.clone()),
            Some(peak(999)),
            "a peer's own follower still receives its frames verbatim — attribution is what lets \
             a subscriber reject and eject its sender, not a filter here"
        );
    }

    /// **A session's identity does not move when ANOTHER session starts.**
    ///
    /// The pool-wide generation this replaces was captured per handler at spawn, so a second
    /// session made the first's frames report a generation that was no longer current.
    #[tokio::test]
    async fn a_second_session_does_not_change_the_identity_of_the_first() {
        let fanout = Arc::new(FrameFanout::new());
        let first = live_session(&fanout, 1).await;
        let mut subscription = fanout.subscribe(first, 8).await.expect("live");

        fanout.publish(first, peak(100)).await;
        let second = live_session(&fanout, 2).await;
        fanout.publish(second, peak(101)).await;
        fanout.publish(first, peak(102)).await;

        let from_first: Vec<PoolFrame> = drained(&mut subscription)
            .into_iter()
            .filter(|f| f.source == first)
            .map(|f| f.frame)
            .collect();

        assert_eq!(
            from_first,
            vec![PoolFrame::Reset, peak(100), peak(102)],
            "both of peer 1's frames must arrive under peer 1's own identity, before and after \
             peer 2 connected"
        );
        assert_ne!(first.session, second.session, "sessions must be distinct");
    }

    /// **Opening a session resets exactly its OWN followers.**
    ///
    /// The nearest wrong implementation is a pool-wide reset: it would emit a `Reset` that a
    /// subscriber following peer 1 must honour, invalidating state peer 1 never invalidated.
    #[tokio::test]
    async fn opening_a_session_resets_only_that_session() {
        let fanout = Arc::new(FrameFanout::new());
        let followed = live_session(&fanout, 1).await;
        let mut subscription = fanout.subscribe(followed, 8).await.expect("live");

        // A second peer connects, which must not disturb the first's follower.
        let _other = live_session(&fanout, 2).await;
        fanout.publish(followed, peak(100)).await;

        let frames = frames_of(&drained(&mut subscription));

        assert_eq!(
            frames.iter().filter(|f| **f == PoolFrame::Reset).count(),
            1,
            "the followed session must be reset exactly once — when this subscription opened, and \
             never because another peer connected: {frames:?}"
        );
    }

    /// The cap is the node's own subscription-response ceiling, restated rather than re-derived.
    ///
    /// Pinned so an edit that "rounds it off" is a failing test: a tighter number terminates honest
    /// sessions during ordinary chain activity, which is the starving direction.
    #[test]
    fn the_frame_item_cap_is_the_nodes_own_subscription_response_ceiling() {
        assert_eq!(MAX_FRAME_COIN_STATES, 100_000);
    }
}
