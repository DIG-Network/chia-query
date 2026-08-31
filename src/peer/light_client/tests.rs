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
        follows(Some(address(ANCHOR)), source(ANCHOR, 1)),
        "the anchor's own frames must be applied, or the client learns nothing at all"
    );
}

#[test]
fn a_frame_from_another_held_peer_is_ignored() {
    assert!(
        !follows(Some(address(ANCHOR)), source(IMPOSTOR, 2)),
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
    assert!(!follows(Some(address(ANCHOR)), source(IMPOSTOR, 1)));
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

/// A `Reset` is a NEW connection at the followed address, whose subscription set is empty.
#[tokio::test]
async fn a_reset_asks_for_a_resubscribe() {
    let cache = RwLock::new(CoinStateCache::new());
    let after = apply_frame(
        &cache,
        SourcedFrame {
            source: source(ANCHOR, 2),
            frame: PoolFrame::Reset,
        },
    )
    .await;
    assert_eq!(after, AfterFrame::Resubscribe);
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

fn fetcher_over(backend: Arc<PeerBackend>) -> PooledFetcher {
    PooledFetcher::new(backend, Duration::from_secs(1))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_anchor_is_pinned_once_and_reused() {
    let fetcher = fetcher_over(backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await);
    let first = fetcher.anchor().await.expect("pin an anchor").address;
    let second = fetcher.anchor().await.expect("reuse the anchor").address;
    assert_eq!(
        first, second,
        "a second subscribing read must reuse the pinned session, or the subscription set is \
         split across the pool"
    );
}

/// Releasing is guarded on the address, not unconditional.
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
    let fetcher = fetcher_over(backend);
    let pinned = fetcher.anchor().await.expect("pin an anchor").address;
    let other = if pinned == address(ANCHOR) {
        address(IMPOSTOR)
    } else {
        address(ANCHOR)
    };

    fetcher.release_anchor(other).await;

    assert_eq!(
        fetcher.anchor_address().await,
        Some(pinned),
        "another peer's session ending must not tear down a healthy anchor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_the_anchors_own_session_unpins_it() {
    let fetcher = fetcher_over(backend_holding(&[(ANCHOR, PeerOrigin::Discovered)]).await);
    let pinned = fetcher.anchor().await.expect("pin an anchor").address;

    fetcher.release_anchor(pinned).await;

    assert_eq!(
        fetcher.anchor_address().await,
        None,
        "the pinned session is gone, so the next subscribing read must re-anchor"
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
