//! `get_blockchain_state`'s peer-tier fallback must report `synced` from MEASURED corroboration,
//! never from a literal (dig_ecosystem#2765).
//!
//! The fixture that tells a measured answer apart from a hardcoded one needs a pool that is
//! quorum-short by construction -- one independent peer, announcing a real, nonzero peak -- so the
//! early "no peak observed" return cannot fire and the ONLY thing distinguishing a fixed
//! implementation from the literal it replaces is whether `synced` reads the peer count at all. A
//! fixture with zero peers proves nothing (the early return already covers it, unchanged); a
//! fixture with a full quorum proves nothing either, because `synced: true` from a literal and
//! `synced: true` from real corroboration are the same bit. Only "peak present, corroboration
//! short" tells them apart -- which is exactly the shape #64 already used for the scalar
//! coin-record read one file over.

use std::net::SocketAddr;
use std::time::Duration;

use crate::coinset::CoinsetClient;
use crate::peer::connect::PeerOrigin;
use crate::peer::test_support::{address, loopback_peer};

use super::{PeerBackend, QueryRouter};

/// A router over a pool holding exactly the peers in `peaks`, each admitted as
/// [`PeerOrigin::Discovered`] (so every one counts toward independence) at the given peak, with the
/// coinset tier declared but never dialled -- `coinset_fallback_enabled: false` makes the peer
/// tier the ONLY voice `get_blockchain_state` can consult, exactly as `scalar_coin_record_tests`
/// isolates the peer tier for the scalar coin-record read.
async fn router_over_peers_at_peak(peaks: &[u32]) -> QueryRouter {
    let backend = PeerBackend::for_tests_with_capacity(peaks.len().max(1));
    for (i, &peak) in peaks.iter().enumerate() {
        let addr: SocketAddr = address(u8::try_from(i + 1).expect("test fixture stays small"));
        assert!(
            backend
                .pool_for_tests()
                .admit_at_peak_for_tests(loopback_peer().await, addr, PeerOrigin::Discovered, peak)
                .await,
            "the fixture peer must be admitted as an independent (Discovered) peer"
        );
    }

    QueryRouter {
        peer: std::sync::Arc::new(backend),
        coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(1))
            .expect("a CoinsetClient over a never-dialled URL still constructs"),
        coinset_fallback_enabled: false,
    }
}

/// **The property under test.** A single peer's announced peak is a real, nonzero peak -- the
/// early "no peak observed" guard does not fire -- but ONE independent voice is below
/// `CORROBORATION_FLOOR` (2), so the peer tier must not assert `synced: true`.
///
/// Before this fix, `synced` was the literal `true` whenever `peak != 0`, so this exact fixture
/// (peak present, corroboration short) is the only shape that distinguishes the two: any fixture
/// with zero peers or a full quorum reads identically under the old code and the new.
#[tokio::test]
async fn peer_fallback_reports_unsynced_below_the_corroboration_floor() {
    let router = router_over_peers_at_peak(&[900]).await;

    let state = router
        .get_blockchain_state()
        .await
        .expect("a nonzero peak must still answer Ok");

    let sync = state
        .sync
        .expect("the peer-tier fallback always carries a sync block");
    assert!(
        !sync.synced,
        "one uncorroborated peer must not be reported as a settled sync state"
    );
    assert_eq!(
        state.peak.expect("peak block present").height,
        900,
        "the peak itself is still reported -- only the confident synced claim is withheld"
    );
}

/// The mirror: at least `CORROBORATION_FLOOR` independent peers agreeing on the same peak DOES
/// earn `synced: true`.
///
/// Without this control, a fix that made `synced` unconditionally `false` (refusing to ever
/// corroborate) would also pass the test above.
#[tokio::test]
async fn peer_fallback_reports_synced_at_the_corroboration_floor() {
    let router = router_over_peers_at_peak(&[900, 900]).await;

    let state = router
        .get_blockchain_state()
        .await
        .expect("two agreeing peers must still answer Ok");

    let sync = state
        .sync
        .expect("the peer-tier fallback always carries a sync block");
    assert!(
        sync.synced,
        "two independent peers agreeing on a peak IS measured corroboration"
    );
    assert_eq!(state.peak.expect("peak block present").height, 900);
}

/// The existing boundary this fix must not disturb: no peer has announced anything yet, so there
/// is no peak to report at all, and the call errors rather than inventing one.
#[tokio::test]
async fn peer_fallback_errors_when_no_peak_has_been_observed() {
    let router = router_over_peers_at_peak(&[]).await;

    let result = router.get_blockchain_state().await;

    assert!(
        result.is_err(),
        "a pool with no announced peak must never invent a synced state: got {result:?}"
    );
}
