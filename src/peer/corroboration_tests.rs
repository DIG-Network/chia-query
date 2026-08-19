//! What the peer tier will and will not call an absence.
//!
//! These exercise the tier the suite has never reached. Every absence fixture elsewhere in the
//! ecosystem is built with `max_peers: 0`, so the peer road is never instantiated and the rule
//! under test here could be deleted without turning anything red (dig_ecosystem#2456).
//!
//! The peers are REAL — a `Peer` reads its address off a live socket and cannot be mocked — and
//! each one is admitted under its own [`Peer::socket_addr`], which is what lets a scripted read
//! give different peers different things to say.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::connect::PeerOrigin;
use super::pool::PeerPool;
use super::test_support::loopback_peer;
use super::{OptAnswer, PeerBackend};
use crate::types::ChiaQueryError;
use crate::NetworkType;

/// What a scripted peer says when asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Says {
    /// A successful response carrying nothing — the empty coin-state list the defect trusted.
    Absent,
    /// A successful response carrying the thing.
    Present,
    /// The read itself failed.
    Fails,
}

/// A backend over exactly the peers given, in the order given, dialling nothing.
///
/// `max_peers` is set to the number admitted so `try_refill` is a no-op: a test that reaches the
/// network is not a unit test, and a refill would also change the pool mid-read.
async fn backend_over(members: &[(Says, PeerOrigin)]) -> (PeerBackend, HashMap<SocketAddr, Says>) {
    let pool = PeerPool::for_tests(members.len());
    let mut script = HashMap::new();

    for (says, origin) in members {
        let peer = loopback_peer().await;
        let addr = peer.socket_addr();
        assert!(
            pool.admit_for_tests(peer, addr, *origin).await,
            "each scripted peer must be admitted under its own address"
        );
        script.insert(addr, *says);
    }

    let backend = PeerBackend {
        pool,
        network: NetworkType::Mainnet,
        request_timeout: Duration::from_millis(50),
    };
    (backend, script)
}

/// Run the corroborated read against `script`, reporting how many peers were actually asked.
async fn read_scripted(
    backend: &PeerBackend,
    script: &HashMap<SocketAddr, Says>,
) -> (Result<OptAnswer<&'static str>, ChiaQueryError>, usize) {
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&asked);

    let result = backend
        .read_opt_corroborated(move |peer| {
            let counter = Arc::clone(&counter);
            let says = *script
                .get(&peer.socket_addr())
                .expect("every peer in the pool is scripted");
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match says {
                    Says::Absent => Ok(None),
                    Says::Present => Ok(Some("the thing")),
                    Says::Fails => Err(ChiaQueryError::PeerConnection("scripted failure".into())),
                }
            }
        })
        .await;

    (result, asked.load(Ordering::SeqCst))
}

/// **The defect, stated as a test.**
///
/// One discovered peer answers successfully and empty, and no second answer arrives because there
/// is no second peer. The caller must NOT be handed an absence. Before this change the identical
/// fixture produced `Ok(None)` — indistinguishable, to every consumer, from a chain that was
/// consulted and provably lacks the thing.
#[tokio::test]
async fn one_peer_saying_absent_is_not_an_absence() {
    let (backend, script) = backend_over(&[(Says::Absent, PeerOrigin::Discovered)]).await;

    let (answer, asked) = read_scripted(&backend, &script).await;

    assert_eq!(
        answer.expect("a lone absent answer is not an ERROR, it is an ungraded fact"),
        OptAnswer::UncorroboratedAbsent,
        "absence on one peer's word must be reported as uncorroborated"
    );
    assert_eq!(asked, 1, "there was only one peer to ask");
}

/// A second peer that is not an INDEPENDENT peer cannot corroborate.
///
/// The fixture varies exactly one thing against the passing case below: the second peer's origin.
/// It is honest and it agrees — so if this returned corroborated absence, the reason would be that
/// a host-local node was counted as a witness to the chain, which is the failure
/// [`PeerPool::independent_peer_count`] exists to prevent. Nothing else in the fixture differs, so
/// nothing else can explain a green.
#[tokio::test]
async fn a_preferred_peer_agreeing_is_not_corroboration() {
    let (backend, script) = backend_over(&[
        (Says::Absent, PeerOrigin::Discovered),
        (Says::Absent, PeerOrigin::Priority),
    ])
    .await;

    let (answer, asked) = read_scripted(&backend, &script).await;

    assert_eq!(
        answer.expect("no read failed"),
        OptAnswer::UncorroboratedAbsent,
        "a preferred peer is not an independent voice, however honestly it agrees"
    );
    assert_eq!(asked, 1, "the preferred peer must not even be consulted");
}

/// The control: two independent peers, both absent, IS an absence.
///
/// Without this the three tests above would all pass on a backend that simply never reported
/// absence at all.
#[tokio::test]
async fn two_independent_peers_agreeing_is_an_absence() {
    let (backend, script) = backend_over(&[
        (Says::Absent, PeerOrigin::Discovered),
        (Says::Absent, PeerOrigin::Discovered),
    ])
    .await;

    let (answer, asked) = read_scripted(&backend, &script).await;

    assert_eq!(
        answer.expect("no read failed"),
        OptAnswer::CorroboratedAbsent
    );
    assert_eq!(
        asked, 2,
        "corroboration means a second peer was really asked"
    );
}

/// A contradiction is surfaced, never resolved.
///
/// One peer says absent and the other produces the thing. Neither answer is preferred — there is
/// nothing in them to prefer on — so the read fails with the disagreement rather than picking a
/// winner in either direction.
#[tokio::test]
async fn a_contradicting_peer_is_refused_not_broken_in_either_direction() {
    let (backend, script) = backend_over(&[
        (Says::Absent, PeerOrigin::Discovered),
        (Says::Present, PeerOrigin::Discovered),
    ])
    .await;

    let (answer, _) = read_scripted(&backend, &script).await;

    assert!(
        matches!(answer, Err(ChiaQueryError::SourcesDisagree(_))),
        "a contradiction is evidence about the sources, not a tie to break: got {answer:?}"
    );
}

/// A corroborator that cannot answer corroborates nothing.
///
/// The distinction this pins: a FAILED second opinion must not read as a confirmed first one. The
/// nearest wrong implementation treats "I asked and got no contradiction" as agreement, and that
/// implementation passes every other test in this file.
#[tokio::test]
async fn a_corroborator_that_fails_leaves_the_absence_uncorroborated() {
    let (backend, script) = backend_over(&[
        (Says::Absent, PeerOrigin::Discovered),
        (Says::Fails, PeerOrigin::Discovered),
    ])
    .await;

    let (answer, asked) = read_scripted(&backend, &script).await;

    assert_eq!(
        answer.expect("the FIRST peer answered, so the read did not fail"),
        OptAnswer::UncorroboratedAbsent,
        "silence from the second peer is not agreement"
    );
    assert_eq!(asked, 2, "the corroborator was asked and failed");
}

/// Presence must not acquire a quorum.
///
/// A returned record is checkable against its own fields, so a second opinion adds nothing and
/// costs a round trip on the common path. Asserting the ASK COUNT is what pins this: an
/// implementation that quorums presence too would still return the right value here.
#[tokio::test]
async fn presence_is_answered_by_the_first_peer_alone() {
    let (backend, script) = backend_over(&[
        (Says::Present, PeerOrigin::Discovered),
        (Says::Present, PeerOrigin::Discovered),
    ])
    .await;

    let (answer, asked) = read_scripted(&backend, &script).await;

    assert_eq!(
        answer.expect("no read failed"),
        OptAnswer::Found("the thing")
    );
    assert_eq!(
        asked, 1,
        "a positive answer is self-verifying; do not slow it down"
    );
}
