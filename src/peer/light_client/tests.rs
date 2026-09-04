//! Tests for what the FOLD changed.
//!
//! The ported halves — the cache's reorg + confirmation-invariant rules, the provider's fail-closed
//! mapping, the paging bounds — keep their own tests in their own modules and are unchanged by the
//! move. What is new here is that the light client BORROWS a session instead of owning one, and
//! every test below is built to fail against the nearest wrong version of that:
//!
//! - a drive-loop that applies every pooled peer's frames rather than the followed one's
//! - a peak whose height and header hash come from different messages
//! - an anchor release that fires for any session rather than the pinned one
//! - a provider descriptor that asserts a trust level instead of reading the one it has

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chia_protocol::{Bytes32, Coin, CoinState};
use dig_chainsource_interface::ProviderKind;
use tokio::sync::RwLock;

use super::*;
use crate::peer::connect::PeerOrigin;
use crate::peer::frames::{FrameSource, SessionEndReason, SessionId};
use crate::peer::test_support::{address, loopback_peer};

/// The session this client follows.
const ANCHOR: u8 = 1;
/// A DIFFERENT peer, held by the same pool, that this client never subscribed through.
///
/// Every attribution test needs it. A fixture with only the anchor cannot tell a drive-loop that
/// filters by source from one that applies everything it is handed, because with one source those
/// two implementations are observationally identical.
const IMPOSTOR: u8 = 2;

fn source(octet: u8, session: u64) -> FrameSource {
    FrameSource {
        address: address(octet),
        session: SessionId(session),
    }
}

fn coin(seed: u8) -> Coin {
    Coin::new(Bytes32::new([seed; 32]), Bytes32::new([seed ^ 3; 32]), 1)
}

fn hash(seed: u8) -> Bytes32 {
    Bytes32::new([seed; 32])
}

/// A cache already following `coin`, with a peak at `(height, hash(peak_seed))`.
async fn cache_tracking(c: Coin, height: u32, peak_seed: u8) -> RwLock<CoinStateCache> {
    let mut cache = CoinStateCache::new();
    cache.set_peak(height, hash(peak_seed));
    cache.track_coins([c.coin_id()]);
    RwLock::new(cache)
}

// ---------------------------------------------------------------------------
// Attribution: the drive-loop follows ONE session
// ---------------------------------------------------------------------------

#[test]
fn a_frame_from_the_followed_session_is_applied() {
    assert!(
        follows(Some(source(ANCHOR, 1)), source(ANCHOR, 1)),
        "the anchor's own frames must be applied, or the client learns nothing at all"
    );
}

#[test]
fn a_frame_from_another_held_peer_is_ignored() {
    assert!(
        !follows(Some(source(ANCHOR, 1)), source(IMPOSTOR, 2)),
        "a CoinStateUpdate carries no request id, so an unfollowed peer's push is \
         indistinguishable from the followed peer's and must never be applied"
    );
}

#[test]
fn an_unanchored_client_follows_nothing() {
    assert!(
        !follows(None, source(ANCHOR, 1)),
        "before the first subscription there is no push this client could have asked for"
    );
}

/// The same session id at a different address is still a different peer.
///
/// Session ids are allocated per connection, so this pair cannot occur in production — the test
/// exists because the cheapest wrong filter is one keyed on the session id alone, which would pass
/// every other attribution test here.
#[test]
fn attribution_is_by_address_not_by_session_id_alone() {
    assert!(!follows(Some(source(ANCHOR, 1)), source(IMPOSTOR, 1)));
}

/// **The same ADDRESS with a different session is a different peer too.**
///
/// This is the half an address-only filter cannot express, and it is the common case rather than an
/// exotic one: a reconnect frequently lands on the same address. The dead session's parting
/// `SessionEnded` would otherwise be read as the LIVE anchor's, unpinning a healthy connection.
#[test]
fn attribution_is_by_session_not_by_address_alone() {
    assert!(
        !follows(Some(source(ANCHOR, 2)), source(ANCHOR, 1)),
        "a replaced session at the same address must not speak for its replacement"
    );
}

// ---------------------------------------------------------------------------
// The peak's height and hash arrive together
// ---------------------------------------------------------------------------

/// A `CoinStateUpdate` advancing the peak must carry ITS OWN header hash into the cache.
///
/// The nearest wrong implementation pairs the new height with whatever hash the cache already had
/// — which is what dropping `peak_hash` from the fan-out forced. The fixture seeds a DIFFERENT
/// prior hash so the two are distinguishable: with the prior and the new hash equal, both versions
/// pass.
#[tokio::test]
async fn a_coin_states_frame_carries_its_own_peak_hash() {
    let c = coin(9);
    let cache = cache_tracking(c, 100, 0xAA).await;

    apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 1),
            frame: PoolFrame::CoinStates {
                height: 200,
                fork_height: 199,
                peak_hash: hash(0xBB),
                items: vec![CoinState {
                    coin: c,
                    created_height: Some(150),
                    spent_height: None,
                }],
            },
        },
    )
    .await;

    assert_eq!(
        cache.read().await.peak(),
        Some((200, hash(0xBB))),
        "the height and the header hash must come from the SAME message; pairing a new height \
         with the previous hash names a block that never existed at that height"
    );
}

#[tokio::test]
async fn a_coin_states_frame_untracks_a_spent_coin_while_retaining_its_state() {
    let c = coin(11);
    let id = c.coin_id();
    let cache = cache_tracking(c, 100, 0xAA).await;

    apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 1),
            frame: PoolFrame::CoinStates {
                height: 200,
                fork_height: 199,
                peak_hash: hash(0xBB),
                items: vec![CoinState {
                    coin: c,
                    created_height: Some(100),
                    spent_height: Some(150),
                }],
            },
        },
    )
    .await;

    let cache = cache.read().await;
    assert!(cache.get(id).is_some(), "spent state is retained for reads");
    assert!(!cache.is_subscribed_coin(id), "the spent coin is untracked");
}

#[tokio::test]
async fn a_peak_frame_advances_the_cache_peak() {
    let cache = RwLock::new(CoinStateCache::new());
    let after = apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 1),
            frame: PoolFrame::Peak {
                height: 500,
                header_hash: hash(0xCC),
            },
        },
    )
    .await;
    assert_eq!(after, AfterFrame::Continue);
    assert_eq!(cache.read().await.peak(), Some((500, hash(0xCC))));
}

// ---------------------------------------------------------------------------
// A session ending is a fact the client must surface, not absorb
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_session_ending_asks_for_a_resubscribe() {
    let cache = RwLock::new(CoinStateCache::new());
    let after = apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 1),
            frame: PoolFrame::SessionEnded {
                reason: SessionEndReason::Disconnected,
            },
        },
    )
    .await;
    assert_eq!(
        after,
        AfterFrame::Resubscribe,
        "a stream that stopped must not read as a chain with nothing to say"
    );
}

/// **A `Reset` no longer asks for a re-subscribe, and that is the point of chia-query#34.**
///
/// A subscription names ONE session, so the only `Reset` this client can now see is the synthesised
/// one that OPENS its own subscription — published as the anchor is pinned, while the subscribing
/// reads that arm the session are still in flight. Treating it as a reason to re-arm would unpin a
/// healthy anchor on every anchoring, and loop.
///
/// The fact it used to carry has not been lost: a session being REPLACED reaches this client as the
/// `SessionEnded` of the subscription that is ending, which the test above pins.
#[tokio::test]
async fn the_reset_that_opens_a_subscription_does_not_ask_for_a_resubscribe() {
    let cache = RwLock::new(CoinStateCache::new());
    let after = apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 2),
            frame: PoolFrame::Reset,
        },
    )
    .await;
    assert_eq!(
        after,
        AfterFrame::Continue,
        "this subscription's own opening Reset must not tear down the anchor it just followed"
    );
}

// ---------------------------------------------------------------------------
// The anchor: pinned once, released only for itself
// ---------------------------------------------------------------------------

/// A backend holding loopback peers at the given `(octet, origin)` pairs.
async fn backend_holding(peers: &[(u8, PeerOrigin)]) -> Arc<PeerBackend> {
    let backend = PeerBackend::for_tests_with_capacity(peers.len());
    for (octet, origin) in peers {
        let peer = loopback_peer().await;
        assert!(
            backend
                .pool_for_tests()
                .admit_for_tests(peer, address(*octet), *origin)
                .await,
            "the test pool must admit its own fixture peers"
        );
    }
    Arc::new(backend)
}

/// A fetcher whose drive-loop channel is held open by the returned receiver.
///
/// The receiver is returned rather than dropped because `anchor` hands each newly pinned session's
/// subscription to it: dropping it would close the channel and turn every anchoring into a logged
/// "drive-loop has stopped", which is a different fixture from the one these tests mean.
fn fetcher_over(
    backend: Arc<PeerBackend>,
) -> (
    PooledFetcher,
    tokio::sync::mpsc::UnboundedReceiver<crate::peer::frames::FrameSubscription>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (PooledFetcher::new(backend, Duration::from_secs(1), tx), rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_anchor_is_pinned_once_and_reused() {
    let (fetcher, _subscriptions) =
        fetcher_over(backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await);
    let first = fetcher.anchor().await.expect("pin an anchor").address;
    let second = fetcher.anchor().await.expect("reuse the anchor").address;
    assert_eq!(
        first, second,
        "a second subscribing read must reuse the pinned session, or the subscription set is \
         split across the pool"
    );
}

/// Releasing is guarded on the SESSION, not unconditional.
///
/// The nearest wrong version clears whatever is pinned. With one peer in the pool that version is
/// indistinguishable from this one, so the fixture holds TWO and ends the session of the one that
/// is NOT the anchor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_another_peers_session_leaves_the_anchor_pinned() {
    let backend = backend_holding(&[
        (ANCHOR, PeerOrigin::Discovered),
        (IMPOSTOR, PeerOrigin::Discovered),
    ])
    .await;
    let (fetcher, _subscriptions) = fetcher_over(backend);
    let pinned = fetcher.anchor().await.expect("pin an anchor").source;
    let other_address = if pinned.address == address(ANCHOR) {
        address(IMPOSTOR)
    } else {
        address(ANCHOR)
    };
    let other = FrameSource {
        address: other_address,
        session: pinned.session,
    };

    fetcher.release_anchor(other).await;

    assert_eq!(
        fetcher.anchor_source().await,
        Some(pinned),
        "another peer's session ending must not tear down a healthy anchor"
    );
}

/// **A dead session at the ANCHOR'S OWN ADDRESS does not unpin its replacement.**
///
/// The reconnect path re-anchors, and the pool frequently offers the same address again — so the
/// predecessor's `SessionEnded`, which arrives whenever its transport finally closes, names an
/// address that is once more the anchor's. An address-guarded release tears down a healthy
/// connection here; a source-guarded one cannot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_session_at_the_anchors_address_does_not_unpin_its_replacement() {
    let (fetcher, _subscriptions) =
        fetcher_over(backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await);
    let pinned = fetcher.anchor().await.expect("pin an anchor").source;

    let predecessor = FrameSource {
        address: pinned.address,
        session: SessionId(pinned.session.0.wrapping_sub(1)),
    };
    fetcher.release_anchor(predecessor).await;

    assert_eq!(
        fetcher.anchor_source().await,
        Some(pinned),
        "a replaced session must not unpin the connection that replaced it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_the_anchors_own_session_unpins_it() {
    let (fetcher, _subscriptions) =
        fetcher_over(backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await);
    let pinned = fetcher.anchor().await.expect("pin an anchor").source;

    fetcher.release_anchor(pinned).await;

    assert_eq!(
        fetcher.anchor_source().await,
        None,
        "the pinned session is gone, so the next subscribing read must re-anchor"
    );
}

// ---------------------------------------------------------------------------
// The drive-loop follows the CURRENT anchor, not whichever session it saw first
// ---------------------------------------------------------------------------

/// A live drive-loop over `backend`, with the cache it maintains and the fetcher that feeds it.
///
/// Assembled from the same parts [`ChiaLightClient::new`] uses, rather than through the client,
/// because these tests drive the anchor directly — a subscribing read would need a fixture peer
/// that speaks the wallet protocol, which is a much larger harness for a property that is entirely
/// about which subscription the loop is reading.
fn drive_loop_over(
    backend: Arc<PeerBackend>,
) -> (Arc<RwLock<CoinStateCache>>, PooledFetcher, JoinHandle<()>) {
    let cache = Arc::new(RwLock::new(CoinStateCache::new()));
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let fetcher = PooledFetcher::new(backend, Duration::from_secs(1), tx);
    let drive = spawn_drive_loop(rx, cache.clone(), fetcher.clone());
    (cache, fetcher, drive)
}

/// The cache's peak once it changes, or `None` if it has not changed within `budget`.
///
/// Polled rather than awaited on a signal because the drive-loop's effect on the cache is the only
/// thing observable from outside it. The budget is generous: a wedged loop never answers at all, so
/// waiting longer only makes a failing run slower, never a passing one flakier.
async fn peak_within(cache: &RwLock<CoinStateCache>, budget: Duration) -> Option<(u32, Bytes32)> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some(peak) = cache.read().await.peak() {
            return Some(peak);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **The control: the harness itself works.**
///
/// One session, pinned and immediately followed. Without this, the two-session test below could
/// fail for want of a working publish path — a fanout that delivered nothing, a `follows` that
/// rejected everything, a cache that never records a peak — and be read as the wedge it is meant to
/// catch. This passes both with and without the fix; the next test is the one that discriminates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drive_loop_applies_a_frame_from_the_session_it_just_pinned() {
    let backend = backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await;
    let (cache, fetcher, _drive) = drive_loop_over(backend.clone());

    let pinned = fetcher.anchor().await.expect("pin an anchor").source;
    backend
        .pool_for_tests()
        .publish_for_tests(
            pinned,
            PoolFrame::Peak {
                height: 4242,
                header_hash: hash(0xEE),
            },
        )
        .await;

    assert_eq!(
        peak_within(&cache, Duration::from_secs(5)).await,
        Some((4242, hash(0xEE))),
        "the drive-loop must apply the frames of the session it is anchored to"
    );
}

/// **chia-query#59: a newly pinned anchor is followed while the PREVIOUS session is still live.**
///
/// The regression. Two sessions are live at once — the first unpinned by a failed read but never
/// closed, the second pinned in its place — and only a loop that stopped waiting on the first can
/// see the second speak.
///
/// Nothing in production forces the first session shut: `eject_peer` removes a bookkeeping entry
/// and publishes no `SessionEnded`, the reader task holds a raw message receiver rather than the
/// pooled `Peer`, and `recv` has no deadline. So the nested `while let` this replaced sat on the
/// first subscription for as long as that peer's transport stayed open — and a peer that simply
/// goes quiet after one failed read holds it there for free, freezing the cache while every
/// consumer surface still reported it healthy.
///
/// **The unpin is deliberately `release_anchor_at` alone, not the whole of `discard`.** Ejecting
/// would drop the pool below its capacity, and the next `anchor()` runs `maintain()` — which would
/// dial a real DNS introducer. The eject half is irrelevant to this property anyway: the wedge is
/// which subscription the loop is reading, and pool membership does not decide that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newly_pinned_anchor_is_followed_while_the_previous_session_is_still_live() {
    let backend = backend_holding(&[
        (ANCHOR, PeerOrigin::Discovered),
        (IMPOSTOR, PeerOrigin::Discovered),
    ])
    .await;
    let (cache, fetcher, _drive) = drive_loop_over(backend.clone());

    // 1. Pin a session. Its subscription reaches the drive-loop, which begins following it.
    let first = fetcher.anchor().await.expect("pin the first anchor").source;

    // 2. A failed read unpins it. The session stays LIVE and quiet — exactly the state a peer
    //    reaches by saying nothing at all.
    fetcher.release_anchor_at(first.address).await;

    // 3. Re-anchor. The pool's round-robin hands back the other held peer, so the loop now has a
    //    second, newer subscription waiting behind a first one that will never end.
    let second = fetcher
        .anchor()
        .await
        .expect("pin the second anchor")
        .source;
    assert_ne!(
        first, second,
        "the fixture must produce TWO concurrently-live sessions, or it cannot express the wedge \
         at all and would pass against the defect"
    );

    // 4. Only the new anchor speaks.
    backend
        .pool_for_tests()
        .publish_for_tests(
            second,
            PoolFrame::Peak {
                height: 4242,
                header_hash: hash(0xEE),
            },
        )
        .await;

    assert_eq!(
        peak_within(&cache, Duration::from_secs(5)).await,
        Some((4242, hash(0xEE))),
        "the drive-loop never picked up the newly pinned anchor: it is still waiting on a session \
         that was superseded and that nothing will ever close, so the cache is frozen while \
         reporting itself healthy (chia-query#59)"
    );
}

// ---------------------------------------------------------------------------
// Losing the anchor ASKS for a re-arm, however it was lost
// ---------------------------------------------------------------------------

/// **chia-query#59: the failed-read path raises the staleness signal too.**
///
/// `discard` unpins the anchor at all seven failed-read sites. From that moment `follows` rejects
/// every frame, so the cache is receiving nothing — and until this, `needs_rearm()` still answered
/// `false`, leaving a consumer unable to tell "no new coin states because nothing happened" from
/// "no new coin states because this client is following nobody".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_read_that_unpins_the_anchor_asks_for_a_rearm() {
    let client = light_client_over(&[(ANCHOR, PeerOrigin::Discovered)]).await;
    let pinned = client.fetcher.anchor().await.expect("pin an anchor");

    assert!(
        !client.needs_rearm(),
        "a freshly pinned anchor is armed; a client that always asked for a re-arm would report a \
         healthy stream as broken"
    );

    client.fetcher.release_anchor_at(pinned.address).await;

    assert!(
        client.needs_rearm(),
        "the anchor is gone, so the subscription set is armed on no session this client reads \
         from; silence here is indistinguishable from a quiet chain"
    );
}

/// The control for the test above: the signal tracks a REAL unpin, not any release call.
///
/// Without it, a `release_anchor_at` that raised the flag unconditionally would pass — and would
/// then report a re-arm every time a read failed against any peer that was never the anchor,
/// costing a redundant re-subscription of the whole set on ordinary one-shot read failures.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_a_peer_that_is_not_the_anchor_does_not_ask_for_a_rearm() {
    let client = light_client_over(&[
        (ANCHOR, PeerOrigin::Discovered),
        (IMPOSTOR, PeerOrigin::Discovered),
    ])
    .await;
    let pinned = client.fetcher.anchor().await.expect("pin an anchor");
    let other = if pinned.address == address(ANCHOR) {
        address(IMPOSTOR)
    } else {
        address(ANCHOR)
    };

    client.fetcher.release_anchor_at(other).await;

    assert!(
        !client.needs_rearm(),
        "a read that failed against a peer this client never pinned leaves the anchor — and the \
         subscription set on it — exactly as armed as it was"
    );
    assert_eq!(
        client.fetcher.anchor_source().await,
        Some(pinned.source),
        "and it leaves the anchor itself alone"
    );
}

// ---------------------------------------------------------------------------
// The provider descriptor reports the origin it OBSERVED
// ---------------------------------------------------------------------------

async fn light_client_over(peers: &[(u8, PeerOrigin)]) -> ChiaLightClient {
    ChiaLightClient::new(backend_holding(peers).await, Duration::from_secs(1)).await
}

/// The three descriptor cases, in ONE test over the same client shape.
///
/// Split into three tests they would each pass against a hard-coded constant matching that case;
/// together they cannot, because one constant cannot be two kinds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_provider_kind_tracks_the_anchors_observed_origin() {
    let unanchored = light_client_over(&[(ANCHOR, PeerOrigin::Priority)]).await;
    assert_eq!(
        unanchored.provider_info().await.kind,
        ProviderKind::Custom,
        "with no session pinned there is no origin to report, so the conservative kind stands"
    );

    let discovered = light_client_over(&[(ANCHOR, PeerOrigin::Discovered)]).await;
    discovered.fetcher.anchor().await.expect("pin an anchor");
    assert_eq!(
        discovered.provider_info().await.kind,
        ProviderKind::Custom,
        "an introducer-discovered peer is an anonymous source"
    );

    let priority = light_client_over(&[(ANCHOR, PeerOrigin::Priority)]).await;
    priority.fetcher.anchor().await.expect("pin an anchor");
    assert_eq!(
        priority.provider_info().await.kind,
        ProviderKind::LocalNode,
        "a configured or co-resident peer must be reported as one — chia-peer could not, because \
         its dialler had no origin concept, so a discovered peer answering a config.endpoint \
         client was described as the operator's own node"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_provider_never_declares_itself_trustless() {
    let client = light_client_over(&[(ANCHOR, PeerOrigin::Priority)]).await;
    client.fetcher.anchor().await.expect("pin an anchor");
    let info = client.provider_info().await;
    assert!(
        !info.trustless,
        "a light-client answer is one peer's word, whatever its origin"
    );
    assert_eq!(
        info.priority, DEFAULT_PROVIDER_PRIORITY,
        "the try-order chia-peer established is preserved"
    );
}

#[test]
fn the_provider_priority_is_ahead_of_the_coinset_tier() {
    assert_eq!(DEFAULT_PROVIDER_PRIORITY, 20);
}

// ---------------------------------------------------------------------------
// Nothing here grants a peer custody trust
// ---------------------------------------------------------------------------

/// NC-12: a `trusted` flag on a dialled peer is a custody grant. The pool dials with
/// `PeerOptions::default()`, and folding a subscriber in must not introduce one.
///
/// Asserted over the light client's own source text rather than a runtime value because the
/// property is the ABSENCE of a construction — there is no value to read when it is upheld. The
/// needle is assembled from fragments so this test cannot match itself.
#[test]
fn the_light_client_sets_no_trusted_flag() {
    let needle: String = ["trusted", ":", " true"].concat();
    for (name, body) in [
        ("mod.rs", include_str!("mod.rs")),
        ("fetcher.rs", include_str!("fetcher.rs")),
        ("provider.rs", include_str!("provider.rs")),
    ] {
        assert!(
            !body.contains(&needle),
            "{name} sets a trusted flag on a peer; that is a custody grant and no read may hand \
             one out"
        );
    }
}

// ---------------------------------------------------------------------------
// Submit-outcome mapping (ported behaviour, kept green through the move)
// ---------------------------------------------------------------------------

/// #48. The `is_accepted` assertion on status 2 used to read `assert!(Pending.is_accepted())` —
/// a test that ENCODED the defect, so the write path could not be corrected without it going red.
/// Status 2 is the node declining to admit the bundle, and the light client is a second, separate
/// implementation of the same ack mapping the peer tier does, so it carried the same wrong answer.
#[test]
fn submit_outcome_maps_ack_status() {
    assert_eq!(SubmitOutcome::from_ack(1, None), SubmitOutcome::Accepted);
    assert_eq!(
        SubmitOutcome::from_ack(2, None),
        SubmitOutcome::NotAdmitted { reason: None }
    );
    assert_eq!(
        SubmitOutcome::from_ack(3, None),
        SubmitOutcome::Failed { reason: None }
    );
    assert_eq!(
        SubmitOutcome::from_ack(9, None),
        SubmitOutcome::Unknown {
            status: 9,
            reason: None
        }
    );

    assert!(SubmitOutcome::Accepted.is_accepted());
    assert!(
        !SubmitOutcome::from_ack(2, None).is_accepted(),
        "a bundle the node declined to admit is not accepted"
    );
    assert!(!SubmitOutcome::from_ack(3, None).is_accepted());
    assert!(!SubmitOutcome::from_ack(9, None).is_accepted());
}

/// #48. Two different refusals must be tellable apart on the write path too. Asserting only that
/// `reason()` is `Some` would pass against an implementation echoing one canned string, so the
/// fixture requires each reason to equal its own input and the two to differ.
#[test]
fn submit_outcome_carries_the_nodes_own_reason() {
    let held = SubmitOutcome::from_ack(2, Some("unknown parent coin".into()));
    let bad_sig = SubmitOutcome::from_ack(3, Some("BAD_AGGREGATE_SIGNATURE".into()));

    assert_eq!(held.reason(), Some("unknown parent coin"));
    assert_eq!(bad_sig.reason(), Some("BAD_AGGREGATE_SIGNATURE"));
    assert_ne!(held.reason(), bad_sig.reason());
    assert_eq!(SubmitOutcome::Accepted.reason(), None);
}

/// `address` is the fixture helper the attribution tests key on; if two octets ever collided the
/// impostor tests above would silently become anchor tests.
#[test]
fn the_fixture_addresses_are_distinct() {
    let a: SocketAddr = address(ANCHOR);
    let b: SocketAddr = address(IMPOSTOR);
    assert_ne!(a, b);
}
