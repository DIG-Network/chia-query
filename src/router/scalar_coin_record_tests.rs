//! The SCALAR `get_coin_record_by_name` must corroborate exactly like its `_opt` sibling.
//!
//! Every other coin read on [`QueryRouter`] was migrated onto the graded ladder as chia-query
//! grew NC-12 support (`get_coin_record_by_name_opt`, every `get_coin_records_by_*` shape) --
//! this scalar form was the one left calling the raw, ungraded [`peer_then_coinset`] helper,
//! so whichever peer answered FIRST decided a coin's `confirmed_block_index` /
//! `spent_block_index` outright. That is the exact class of defect NC-12 exists to catch, and
//! this crate's OWN [`ChiaQuery::wait_for_confirmation`](crate::ChiaQuery::wait_for_confirmation)
//! depended on the hole being closed: it polls this method before telling a caller a spend has
//! landed (dig_ecosystem#3034).
//!
//! The fixture is a pool holding exactly ONE peer, admitted as [`PeerOrigin::Discovered`] so it
//! counts toward independence. That is "quorum-short" by construction -- corroboration needs at
//! least two independent voices and this pool structurally cannot supply a second one -- which is
//! precisely the scenario an all-peers-agree or all-peers-fail fixture cannot see: agreement needs
//! nothing to fall back on, and a peer that never answers never produces a fact to trust in the
//! first place. Only a single peer that DOES answer distinguishes "believed outright" from
//! "refused for want of a second source".

use std::sync::Arc;
use std::time::Duration;

use chia_protocol::{Bytes32, Coin, CoinState, RespondCoinState};

use crate::coinset::CoinsetClient;
use crate::peer::connect::PeerOrigin;
use crate::peer::test_support::coin_state_peer;
use crate::types::ChiaQueryError;

use super::{PeerBackend, QueryRouter};

/// Every scripted coin state answers about this coin id -- 32 bytes of `0x11`, hex-encoded.
fn coin_id_hex() -> String {
    "11".repeat(32)
}

/// A `RespondCoinState` naming one coin, confirmed at height 12345 and unspent.
///
/// The identity of the coin data is irrelevant to what this test proves: with only one peer in
/// the pool there is no second answer to compare it against, so the outcome is governed entirely
/// by the corroboration FLOOR, never by the fields themselves.
fn scripted_presence() -> RespondCoinState {
    RespondCoinState {
        coin_ids: vec![Bytes32::new([0x11; 32])],
        coin_states: vec![CoinState {
            coin: Coin {
                parent_coin_info: Bytes32::new([0xAA; 32]),
                puzzle_hash: Bytes32::new([0xBB; 32]),
                amount: 1_000_000_000,
            },
            created_height: Some(12_345),
            spent_height: None,
        }],
    }
}

/// A router over a ONE-peer pool and a coinset tier that is declared but never dialled.
///
/// `coinset_fallback_enabled: false` makes the single peer's answer the ONLY voice the router can
/// possibly consult -- there is no second source to rescue an uncorroborated presence, which is
/// what makes this pool "quorum-short" rather than merely small.
async fn router_over_one_peer() -> QueryRouter {
    let backend = PeerBackend::for_tests_with_timeout(1, Duration::from_millis(500));
    let peer = coin_state_peer(scripted_presence()).await;
    let addr = peer.socket_addr();
    assert!(
        backend
            .pool_for_tests()
            .admit_for_tests(peer, addr, PeerOrigin::Discovered)
            .await,
        "the fixture peer must be admitted as an independent (Discovered) peer"
    );

    QueryRouter {
        peer: Arc::new(backend),
        coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(1))
            .expect("a CoinsetClient over a never-dialled URL still constructs"),
        coinset_fallback_enabled: false,
    }
}

/// Control: the absence-aware sibling already refuses a single uncorroborated peer.
///
/// This pins the fixture itself -- if this failed, the test would be exercising a scenario the
/// crate had never protected, not the scenario this PR closes.
#[tokio::test]
async fn opt_sibling_refuses_a_single_uncorroborated_peer() {
    let router = router_over_one_peer().await;

    let result = router.get_coin_record_by_name_opt(&coin_id_hex()).await;

    assert!(
        matches!(result, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "control fixture: get_coin_record_by_name_opt must already refuse a lone peer, got {result:?}"
    );
}

/// The property under test: the scalar read must fail the IDENTICAL way.
///
/// Before this fix, `get_coin_record_by_name` called the raw `peer_then_coinset` helper and
/// returned `Ok(record)` straight from this same single peer -- the defect the control test above
/// proves the crate no longer commits for the `_opt` form. A fixture where the peer never answers,
/// or where two peers agree, cannot tell the fixed code from the code it replaced: both would
/// error in the first case and both would succeed in the second. Only a lone ANSWERING peer does.
#[tokio::test]
async fn scalar_read_refuses_a_single_uncorroborated_peer_too() {
    let router = router_over_one_peer().await;

    let result = router.get_coin_record_by_name(&coin_id_hex()).await;

    assert!(
        matches!(result, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "a single uncorroborated peer must not be trusted by the scalar coin-record read either: \
         got {result:?}"
    );
}
