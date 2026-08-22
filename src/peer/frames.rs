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
}

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
    receiver: mpsc::Receiver<SourcedFrame>,
}

impl FrameSubscription {
    /// The next frame, or `None` once the subscription has ended.
    pub async fn recv(&mut self) -> Option<SourcedFrame> {
        self.receiver.recv().await
    }

    /// The next frame if one is already queued.
    pub fn try_recv(&mut self) -> Result<SourcedFrame, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

/// The pool's fan-out: many subscribers, each with its own bounded queue.
pub struct FrameFanout {
    subscribers: Mutex<Vec<mpsc::Sender<SourcedFrame>>>,
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
            subscribers: Mutex::new(Vec::new()),
            next_session: AtomicU64::new(0),
        }
    }

    /// Open a subscription with room for `capacity` unread frames.
    ///
    /// `capacity` is the consumer's own promise about how far behind it may fall: beyond it the
    /// subscription is terminated rather than thinned.
    pub async fn subscribe(self: &Arc<Self>, capacity: usize) -> FrameSubscription {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        self.subscribers.lock().await.push(sender);
        FrameSubscription { receiver }
    }

    /// How many subscriptions are still live.
    pub async fn subscriber_count(&self) -> usize {
        self.subscribers.lock().await.len()
    }

    /// Allocate the identity of a new session at `address`.
    ///
    /// Allocation publishes nothing: a connection is identified before the pool decides whether to
    /// admit it, so a rejected duplicate cannot announce a session that never ran.
    /// [`open_session`](Self::open_session) announces an admitted one.
    pub fn allocate_session(&self, address: SocketAddr) -> FrameSource {
        FrameSource {
            address,
            session: SessionId(self.next_session.fetch_add(1, Ordering::Relaxed)),
        }
    }

    /// Announce that `source` has begun, before it can publish anything else.
    ///
    /// The [`PoolFrame::Reset`] is delivered from INSIDE this call, so it is queued before any
    /// frame the session's own task publishes. Publishing it from that task instead would make the
    /// ordering a race.
    pub async fn open_session(&self, source: FrameSource) {
        self.publish(source, PoolFrame::Reset).await;
    }

    /// Deliver `frame` from `source` to every live subscriber, terminating any that has fallen
    /// behind.
    ///
    /// A subscriber whose queue is FULL is removed, which drops the sender and ends its stream. A
    /// subscriber whose receiver is already gone is removed too; that one is ordinary tidying.
    pub async fn publish(&self, source: FrameSource, frame: PoolFrame) {
        let sourced = SourcedFrame { source, frame };
        let mut subscribers = self.subscribers.lock().await;
        subscribers.retain(|sender| match sender.try_send(sourced.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!(
                    "frame subscriber fell behind; terminating its subscription rather than \
                     dropping a frame it would never learn it missed"
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, last)), 8444)
    }

    fn source(fanout: &FrameFanout, last: u8) -> FrameSource {
        fanout.allocate_session(addr(last))
    }

    fn peak(height: u32) -> PoolFrame {
        PoolFrame::Peak {
            height,
            header_hash: Bytes32::new([height as u8; 32]),
        }
    }

    /// **Every frame names the peer that sent it.**
    ///
    /// Two DIFFERENT peers publish the same kind of frame, which is the only fixture that can
    /// distinguish attribution from a constant: a source hard-coded to anything at all, or an
    /// envelope naming the pool rather than the peer, would give both frames one identity and fail
    /// here. A single-peer fixture cannot see that.
    ///
    /// This is the property whose absence let any held peer inject `CoinStateUpdate`s
    /// indistinguishable from the followed peer's.
    #[tokio::test]
    async fn a_frame_names_the_peer_that_sent_it() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(8).await;

        let honest = source(&fanout, 1);
        let liar = source(&fanout, 2);

        fanout.publish(honest, peak(100)).await;
        fanout.publish(liar, peak(999)).await;

        let first = subscription.try_recv().expect("the honest frame");
        let second = subscription.try_recv().expect("the injected frame");

        assert_eq!(first.source.address, addr(1));
        assert_eq!(second.source.address, addr(2));
        assert_ne!(
            first.source, second.source,
            "two peers must not share one frame identity, or an injected frame is \
             indistinguishable from the followed peer's"
        );
        assert_eq!(
            second.frame,
            peak(999),
            "the injected frame is still delivered — attribution is what lets a subscriber \
             reject and eject its sender, not a filter here"
        );
    }

    /// **A session's identity does not move when ANOTHER session starts.**
    ///
    /// The pool-wide generation this replaces was captured per handler at spawn, so a second
    /// session made the first's frames report a generation that was no longer current: a consumer
    /// honouring the contract discarded every earlier peer's frames on every ordinary refill.
    ///
    /// Peer 1 publishes, peer 2 opens, peer 1 publishes again, and both of peer 1's frames must
    /// carry the SAME source. A fixture with one peer cannot express this at all.
    #[tokio::test]
    async fn a_second_session_does_not_change_the_identity_of_the_first() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(8).await;

        let first = source(&fanout, 1);
        fanout.publish(first, peak(100)).await;

        let second = source(&fanout, 2);
        fanout.open_session(second).await;
        fanout.publish(second, peak(101)).await;

        fanout.publish(first, peak(102)).await;

        let mut from_first = Vec::new();
        while let Ok(sourced) = subscription.try_recv() {
            if sourced.source == first {
                from_first.push(sourced.frame);
            }
        }

        assert_eq!(
            from_first,
            vec![peak(100), peak(102)],
            "both of peer 1's frames must arrive under peer 1's own identity, before and after \
             peer 2 connected"
        );
        assert_ne!(first.session, second.session, "sessions must be distinct");
    }

    /// **`Reset` opens exactly its OWN session and does not disturb another.**
    ///
    /// The nearest wrong implementation is a pool-wide reset: it would emit a `Reset` that a
    /// subscriber following peer 1 must honour, invalidating state peer 1 never invalidated. Here
    /// the only `Reset` peer 1 sees is its own.
    #[tokio::test]
    async fn opening_a_session_resets_only_that_session() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(8).await;

        let followed = source(&fanout, 1);
        fanout.open_session(followed).await;
        fanout.publish(followed, peak(100)).await;

        let other = source(&fanout, 2);
        fanout.open_session(other).await;

        let mut resets_for_followed = 0usize;
        let mut resets_for_other = 0usize;
        while let Ok(sourced) = subscription.try_recv() {
            if sourced.frame == PoolFrame::Reset {
                if sourced.source == followed {
                    resets_for_followed += 1;
                } else if sourced.source == other {
                    resets_for_other += 1;
                }
            }
        }

        assert_eq!(
            resets_for_followed, 1,
            "the followed session must be reset exactly once — when it opened, and never because \
             another peer connected"
        );
        assert_eq!(resets_for_other, 1);
    }

    /// **`Reset` precedes the first frame of its session.**
    ///
    /// The assertion is on the ORDER — a `Reset` emitted after the session's first frame, or not
    /// emitted at all, both fail here, and neither is visible from the frames' contents alone.
    #[tokio::test]
    async fn reset_is_delivered_before_the_first_frame_of_its_session() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(8).await;

        let session = source(&fanout, 1);
        fanout.open_session(session).await;
        fanout.publish(session, peak(101)).await;

        let mut seen = Vec::new();
        while let Ok(sourced) = subscription.try_recv() {
            seen.push(sourced.frame);
        }

        let reset_at = seen
            .iter()
            .position(|f| *f == PoolFrame::Reset)
            .expect("a session must announce itself with a Reset");
        let first_frame_at = seen
            .iter()
            .position(|f| matches!(f, PoolFrame::Peak { height: 101, .. }))
            .expect("the session's frame must be delivered");

        assert!(
            reset_at < first_frame_at,
            "Reset must precede the first frame of its session: {seen:?}"
        );
    }

    /// **A subscriber that falls behind is TERMINATED, never silently thinned.**
    ///
    /// Capacity is 2 and four frames are published without a single `recv`, so the third
    /// overflows. The assertions are built so the nearest wrong implementation — drop the frame,
    /// keep the subscription — cannot pass: it would deliver frames 1, 2 and then 4 (once room
    /// appeared), leaving the stream OPEN. Here the stream must END after the two it accepted, and
    /// the heights that overflowed must never appear.
    #[tokio::test]
    async fn a_subscriber_that_overflows_is_terminated_rather_than_missing_a_frame() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(2).await;
        let session = source(&fanout, 1);

        for height in 1..=4u32 {
            fanout.publish(session, peak(height)).await;
        }

        // Drained without ever awaiting, so a subscription that was WRONGLY kept alive fails an
        // assertion here instead of hanging this test on a `recv` that never resolves.
        let mut delivered = Vec::new();
        let ended = loop {
            match subscription.try_recv() {
                Ok(SourcedFrame {
                    frame: PoolFrame::Peak { height, .. },
                    ..
                }) => delivered.push(height),
                Ok(_) => {}
                Err(err) => break err,
            }
        };

        assert_eq!(
            delivered,
            vec![1, 2],
            "only the frames that fit may be delivered, and the stream must then end"
        );
        assert_eq!(
            ended,
            mpsc::error::TryRecvError::Disconnected,
            "the subscription must be TERMINATED, not merely empty with frames silently skipped"
        );
        assert_eq!(
            fanout.subscriber_count().await,
            0,
            "the overflowing subscription must be dropped, not retained and thinned"
        );
    }

    /// The control: a subscriber that keeps up is not terminated.
    ///
    /// Without it, a `publish` that terminated EVERY subscriber would satisfy the overflow test.
    #[tokio::test]
    async fn a_subscriber_that_keeps_up_stays_subscribed() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(2).await;
        let session = source(&fanout, 1);

        for height in 1..=4u32 {
            fanout.publish(session, peak(height)).await;
            assert!(matches!(
                subscription.try_recv(),
                Ok(SourcedFrame {
                    frame: PoolFrame::Peak { .. },
                    ..
                })
            ));
        }

        assert_eq!(fanout.subscriber_count().await, 1);
    }
}
