//! When a mempool refusal is FINAL, and when it earns one more peer (chia-query#50).
//!
//! `push_tx` is the crate's only write. The question these pin is not whether a bundle is valid —
//! nothing here can decide that — but whose ANSWER about it the caller is told, and how many times
//! the bundle is put on the wire to find out.
//!
//! The peers are REAL, over loopback websockets that speak just enough of the wallet protocol to
//! ack a `send_transaction`, and each counts the requests it answered. That counter is the whole
//! point of the fixture: the bound is "never more than two transmissions", and a bound nothing
//! counts is a comment rather than a property. Reading the returned verdict proves what a peer
//! SAID; only the count proves how many peers were ASKED.
//!
//! The coinset tier is configured and pointed at a closed port. A refusal that fell through to it
//! would surface as `AllSourcesFailed`, so every test asserting a refusal comes back verbatim is
//! also asserting that coinset was never consulted about it.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::QueryRouter;
use crate::coinset::CoinsetClient;
use crate::peer::connect::PeerOrigin;
use crate::peer::test_support::transaction_peer;
use crate::peer::PeerBackend;
use crate::types::{ChiaQueryError, MempoolInclusion, SpendBundle};

/// Chia's `MempoolInclusionStatus` bytes, named so a fixture reads as the verdict it scripts.
const SUCCESS: u8 = 1;
const PENDING: u8 = 2;
const FAILED: u8 = 3;

/// A refusal every honest node reaches from the same bytes.
const INTRINSIC: &str = "BAD_AGGREGATE_SIGNATURE";
/// A refusal that depends on which node was asked: this one is that node's fee policy.
const VIEW_DEPENDENT: &str = "INVALID_FEE_TOO_CLOSE_TO_ZERO";

/// An empty bundle. Its CONTENT is irrelevant here — no fixture inspects it, and the peers answer
/// from a script — but it must be a real one, because the push path serialises it on the way out.
fn bundle() -> SpendBundle {
    SpendBundle {
        coin_spends: Vec::new(),
        // The G2 point at infinity: the canonical encoding of an EMPTY aggregate, 0xc0 followed
        // by 95 zero bytes. A shorter or arbitrary string is rejected by BLS decoding before the
        // push reaches a peer at all, which would make every fixture here measure the parser.
        aggregated_signature: format!("0xc0{}", "00".repeat(95)),
    }
}

/// One scripted peer: how it answers, and how many pushes it answered.
struct Scripted {
    address: SocketAddr,
    pushes: Arc<AtomicUsize>,
}

impl Scripted {
    fn pushes(&self) -> usize {
        self.pushes.load(Ordering::SeqCst)
    }
}

/// A router over exactly the scripted peers given, in the order given, dialling nothing.
///
/// The peers are admitted as `Discovered` because that is the ordinary case and because the retry
/// selector must be shown working on peers no operator configured. `max_peers` is the number
/// admitted, so `try_refill` is a no-op and the pool cannot change under a push.
async fn router_over(script: Vec<(u8, Option<&str>)>) -> (QueryRouter, Vec<Scripted>) {
    let backend = PeerBackend::for_tests_with_timeout(script.len(), Duration::from_secs(5));
    let mut peers = Vec::new();

    for (status, error) in script {
        let (peer, pushes) = transaction_peer(status, error.map(str::to_string)).await;
        let address = peer.socket_addr();
        assert!(
            backend
                .pool_for_tests()
                .admit_for_tests(peer, address, PeerOrigin::Discovered)
                .await,
            "each scripted peer must be admitted under its own address"
        );
        peers.push(Scripted { address, pushes });
    }

    let router = QueryRouter {
        peer: Arc::new(backend),
        // A closed port: reachable in microseconds, and never successfully. Any fall-through to the
        // coinset tier therefore shows up as `AllSourcesFailed` rather than as a slow test.
        coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(200))
            .expect("a coinset client over a closed port is still a client"),
        coinset_fallback_enabled: true,
    };
    (router, peers)
}

fn total_pushes(peers: &[Scripted]) -> usize {
    peers.iter().map(Scripted::pushes).sum()
}

// ---------------------------------------------------------------------------
// A view-dependent refusal earns exactly one more peer
// ---------------------------------------------------------------------------

/// **THE DEFECT: one peer's fee policy vetoed a spend every other peer would have admitted.**
///
/// `push_tx` returned the first `Ok`, so `NotAdmitted` from the first peer asked was the answer.
/// Here that peer's refusal names its own fee floor — a property of the NODE, not of the bundle —
/// and the other peer admits the very same bytes. Under the old routing the caller was told its
/// transaction was not accepted, and `dig-wallet` then held its inputs reserved for the full TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_view_dependent_refusal_is_retried_and_the_admission_wins() {
    let (router, peers) = router_over(vec![
        (PENDING, Some(VIEW_DEPENDENT)),
        (SUCCESS, None),
    ])
    .await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(
        status.inclusion,
        MempoolInclusion::Admitted,
        "a peer that ADMITTED the bundle outranks a peer that refused it on its own fee policy"
    );
    assert!(status.success, "and the boolean must agree with the verdict");
    assert_eq!(
        total_pushes(&peers),
        2,
        "exactly two transmissions: the original and the one retry"
    );
    assert!(
        peers.iter().all(|p| p.pushes() == 1),
        "the retry must go to a DIFFERENT peer, never twice to the one that refused"
    );
}

/// **An intrinsic refusal is FINAL, and costs exactly one transmission.**
///
/// A bad aggregate signature is a property of the bytes: every honest node reaches it from the same
/// bundle, so a second peer buys a round trip and the identical answer. This is the assertion that
/// stops the fix becoming "always ask twice".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_intrinsic_refusal_is_final_and_is_never_retried() {
    let (router, peers) = router_over(vec![(FAILED, Some(INTRINSIC)), (SUCCESS, None)]).await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(status.inclusion, MempoolInclusion::Failed);
    assert_eq!(
        status.error.as_deref(),
        Some(INTRINSIC),
        "the refusing peer's own words are returned verbatim"
    );
    assert_eq!(
        total_pushes(&peers),
        1,
        "an intrinsic refusal must not be transmitted a second time, even though a peer that \
         would have admitted it is held"
    );
}

/// **The BOUND: two view-dependent refusals cost exactly TWO transmissions, and no third.**
///
/// Three peers are held, so an unbounded or eager retry has somewhere to go. This is the assertion
/// that prices the abuse: a peer wanting to INDUCE retries emits an unrecognised reason, and it
/// must buy exactly one extra round trip, once, with no loop to drive.
///
/// It also pins that coinset is never consulted about a refusal. The coinset tier is enabled and
/// unreachable, so a fall-through would surface as `AllSourcesFailed` instead of this refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_view_dependent_refusals_cost_two_transmissions_and_no_third() {
    let (router, peers) = router_over(vec![
        (PENDING, Some(VIEW_DEPENDENT)),
        (PENDING, Some("MEMPOOL_CONFLICT")),
        (PENDING, Some("UNKNOWN_UNSPENT")),
    ])
    .await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert!(
        !status.success,
        "nobody admitted the bundle, so the caller must not be told it was accepted"
    );
    assert_eq!(
        total_pushes(&peers),
        2,
        "a push is transmitted to at most two peers, whatever the refusals say"
    );
    assert_eq!(
        status.error.as_deref(),
        Some(VIEW_DEPENDENT),
        "neither view-dependent refusal outranks the other, so the answer to the caller's OWN \
         original transmission stands — which is what stops the reported reason depending on dial \
         order. `select_peer` round-robins from index 0 on a fresh pool, so that is the first peer \
         admitted here"
    );
}

/// **A definitive refusal from the second peer outranks a view-dependent one from the first.**
///
/// Both refused, so nothing was admitted and the caller gets a refusal either way — but WHICH
/// refusal is what an operator acts on. "Your fee is too low" and "your signature is invalid" are
/// different problems with different fixes, and the second is the one that is true of the bundle
/// wherever it is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_definitive_refusal_outranks_a_view_dependent_one() {
    let (router, peers) = router_over(vec![
        (PENDING, Some(VIEW_DEPENDENT)),
        (FAILED, Some(INTRINSIC)),
    ])
    .await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(
        status.error.as_deref(),
        Some(INTRINSIC),
        "the refusal that is true of the BUNDLE is the one the caller is told about"
    );
    assert_eq!(total_pushes(&peers), 2);
}

/// **A pool of ONE returns that peer's refusal rather than an error.**
///
/// There is nobody else to ask, which is an ordinary state and not a failure of the push. Turning
/// it into an `Err` would replace a real verdict — a peer answered, and said no — with a report
/// that nothing could be reached, which is a different and false statement about the network.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_of_one_returns_its_refusal_rather_than_an_error() {
    let (router, peers) = router_over(vec![(PENDING, Some(VIEW_DEPENDENT))]).await;

    let status = router
        .push_tx(&bundle())
        .await
        .expect("the one peer held DID answer, so this is a verdict and not a failure");

    assert_eq!(status.inclusion, MempoolInclusion::NotAdmitted);
    assert_eq!(status.error.as_deref(), Some(VIEW_DEPENDENT));
    assert_eq!(total_pushes(&peers), 1);
}

/// **The refuser is NOT ejected.**
///
/// Ejection is for a FAILED request; a refusal is a completed request with an answer. Ejecting on
/// refusal would let anyone holding a badly-fee'd bundle churn the pool's composition at will — a
/// limiter keyed on caller input, which is a denial primitive rather than a defence against one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_refuses_stays_in_the_pool() {
    let (router, peers) = router_over(vec![
        (PENDING, Some(VIEW_DEPENDENT)),
        (PENDING, Some(VIEW_DEPENDENT)),
    ])
    .await;
    let before = router.peer.peer_count().await;

    let _ = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(
        router.peer.peer_count().await,
        before,
        "both peers answered, so both are still held: a refusal is an answer, not a failure"
    );
    assert_eq!(total_pushes(&peers), 2);
}

/// **A refusal stating NO reason is retried.**
///
/// Silence is the cheapest thing a peer can offer, so reading it as a definitive verdict would make
/// the veto free. `None` is not evidence that the bundle is bad; it is the absence of evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_stating_no_reason_is_retried() {
    let (router, peers) = router_over(vec![(PENDING, None), (SUCCESS, None)]).await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(status.inclusion, MempoolInclusion::Admitted);
    assert_eq!(total_pushes(&peers), 2);
}

/// **An ADMISSION is final and is never transmitted again.**
///
/// The control for every test above. Without it, an implementation that always made two
/// transmissions would satisfy the retry tests while doubling the traffic of the common case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admission_is_never_transmitted_a_second_time() {
    let (router, peers) = router_over(vec![(SUCCESS, None), (SUCCESS, None)]).await;

    let status = router.push_tx(&bundle()).await.expect("a peer answered");

    assert_eq!(status.inclusion, MempoolInclusion::Admitted);
    assert_eq!(
        total_pushes(&peers),
        1,
        "there is nothing to retry once a mempool holds the bundle"
    );
}

/// **The `Err` path is unchanged: a transport failure still retries and then falls back.**
///
/// This is the TIMEOUT-AFTER-TRANSMIT case, where the bundle may be in flight without an
/// admission, and it is deliberately untouched by the refusal policy. With no peer able to answer
/// and coinset unreachable, the ladder must be walked to its end and report that every source
/// failed — never a fabricated verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_failure_still_walks_the_retry_then_coinset_ladder() {
    let backend = PeerBackend::for_tests_with_timeout(0, Duration::from_millis(50));
    let router = QueryRouter {
        peer: Arc::new(backend),
        coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(200))
            .expect("client"),
        coinset_fallback_enabled: true,
    };

    match router.push_tx(&bundle()).await {
        Err(ChiaQueryError::AllSourcesFailed { .. }) => {}
        other => panic!("every source failing must be reported as such: {other:?}"),
    }
}

/// **The retry goes to a DIFFERENT address, not twice to the peer that refused.**
///
/// Asking the same connection again returns the same opinion and spends a transmission to learn
/// nothing — and the exclusion is explicit rather than incidental, because the refuser is NOT
/// ejected, so the ordinary rotation could land on it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_retry_goes_to_a_different_address_than_the_refuser() {
    let (router, peers) = router_over(vec![(PENDING, Some(VIEW_DEPENDENT)), (SUCCESS, None)]).await;

    let _ = router.push_tx(&bundle()).await.expect("a peer answered");

    let asked: Vec<SocketAddr> = peers
        .iter()
        .filter(|p| p.pushes() > 0)
        .map(|p| p.address)
        .collect();
    assert_eq!(asked.len(), 2, "two distinct peers were asked");
    assert_ne!(asked[0], asked[1]);
}
