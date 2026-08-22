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
//!
//! # Generations
//!
//! Frames from two different sessions are not one stream. A reconnect increments the
//! [`Generation`] and delivers an explicit [`PoolFrame::Reset`] BEFORE any frame of the new
//! session, so a consumer knows its incremental state is stale rather than inferring it from a
//! peak that moved backwards.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chia_protocol::{Bytes32, CoinState};
use tokio::sync::{mpsc, Mutex};

/// Which peer session a frame came from. Incremented on every reconnect.
///
/// A newtype rather than a bare `u64` so a generation cannot be confused with a height, which is
/// the other monotonically increasing number every frame carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

/// One frame from a pooled peer session, as a subscriber sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolFrame {
    /// The session behind this stream was replaced. Everything derived from earlier frames of an
    /// earlier generation is stale, and the frames after this one belong to `generation`.
    ///
    /// Delivered before any frame of the new generation, never after it.
    Reset { generation: Generation },
    /// A peer announced a new peak.
    Peak {
        generation: Generation,
        height: u32,
        header_hash: Bytes32,
    },
    /// A peer reported coin states changing at `height`.
    CoinStates {
        generation: Generation,
        height: u32,
        fork_height: u32,
        items: Vec<CoinState>,
    },
}

/// The receiving half of a subscription.
///
/// [`recv`](Self::recv) returning `None` means the subscription ENDED — either the pool was
/// dropped or this subscriber fell behind and was terminated rather than silently skipped. A
/// consumer that treats `None` as "nothing more to do" is reading a desync as quiet; treat it as
/// "resubscribe and rebuild".
pub struct FrameSubscription {
    receiver: mpsc::Receiver<PoolFrame>,
}

impl FrameSubscription {
    /// The next frame, or `None` once the subscription has ended.
    pub async fn recv(&mut self) -> Option<PoolFrame> {
        self.receiver.recv().await
    }

    /// The next frame if one is already queued.
    pub fn try_recv(&mut self) -> Result<PoolFrame, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

/// The pool's fan-out: many subscribers, each with its own bounded queue.
pub struct FrameFanout {
    subscribers: Mutex<Vec<mpsc::Sender<PoolFrame>>>,
    generation: AtomicU64,
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
            generation: AtomicU64::new(0),
        }
    }

    /// The generation frames are currently being tagged with.
    pub fn generation(&self) -> Generation {
        Generation(self.generation.load(Ordering::Acquire))
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

    /// Begin a new generation and announce it, returning the generation now in force.
    ///
    /// The [`PoolFrame::Reset`] is delivered from INSIDE this call, so it is queued before any
    /// frame the caller subsequently publishes for the new session. Publishing the reset from the
    /// new session's own task instead would make the ordering a race.
    pub async fn begin_generation(&self) -> Generation {
        let generation = Generation(self.generation.fetch_add(1, Ordering::AcqRel) + 1);
        self.publish(PoolFrame::Reset { generation }).await;
        generation
    }

    /// Deliver `frame` to every live subscriber, terminating any that has fallen behind.
    ///
    /// A subscriber whose queue is FULL is removed, which drops the sender and ends its stream. A
    /// subscriber whose receiver is already gone is removed too; that one is ordinary tidying.
    pub async fn publish(&self, frame: PoolFrame) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.retain(|sender| match sender.try_send(frame.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!(
                    "frame subscriber fell behind; terminating its subscription rather than                      dropping a frame it would never learn it missed"
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

    fn hash(byte: u8) -> Bytes32 {
        Bytes32::new([byte; 32])
    }

    fn peak(generation: u64, height: u32) -> PoolFrame {
        PoolFrame::Peak {
            generation: Generation(generation),
            height,
            header_hash: hash(height as u8),
        }
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

        for height in 1..=4u32 {
            fanout.publish(peak(0, height)).await;
        }

        // Drained without ever awaiting, so a subscription that was WRONGLY kept alive fails an
        // assertion here instead of hanging this test on a `recv` that never resolves.
        let mut delivered = Vec::new();
        let ended = loop {
            match subscription.try_recv() {
                Ok(PoolFrame::Peak { height, .. }) => delivered.push(height),
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

        for height in 1..=4u32 {
            fanout.publish(peak(0, height)).await;
            assert!(matches!(
                subscription.try_recv(),
                Ok(PoolFrame::Peak { .. })
            ));
        }

        assert_eq!(fanout.subscriber_count().await, 1);
    }

    /// **`Reset` precedes the first frame of the new generation.**
    ///
    /// A frame is published on generation 0, the session is replaced, and a frame is published on
    /// generation 1. The assertion is on the ORDER — a `Reset` emitted after the first
    /// post-reconnect frame, or not emitted at all, both fail here, and neither is visible from
    /// the frames' contents alone.
    #[tokio::test]
    async fn reset_is_delivered_before_the_first_frame_of_the_new_generation() {
        let fanout = Arc::new(FrameFanout::new());
        let mut subscription = fanout.subscribe(8).await;

        fanout.publish(peak(0, 100)).await;

        let generation = fanout.begin_generation().await;
        assert_eq!(generation, Generation(1));
        fanout
            .publish(PoolFrame::Peak {
                generation,
                height: 101,
                header_hash: hash(2),
            })
            .await;

        let mut seen = Vec::new();
        while let Ok(frame) = subscription.try_recv() {
            seen.push(frame);
        }

        let reset_at = seen
            .iter()
            .position(|f| matches!(f, PoolFrame::Reset { .. }))
            .expect("a reconnect must announce itself with a Reset");
        let new_frame_at = seen
            .iter()
            .position(|f| matches!(f, PoolFrame::Peak { height: 101, .. }))
            .expect("the post-reconnect frame must be delivered");

        assert!(
            reset_at < new_frame_at,
            "Reset must precede the first frame of the new generation: {seen:?}"
        );
        assert_eq!(
            seen[reset_at],
            PoolFrame::Reset {
                generation: Generation(1)
            },
            "the Reset must name the generation the following frames belong to"
        );
    }
}
