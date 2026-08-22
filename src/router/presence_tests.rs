//! What the router does with a presence the peer tier could not corroborate on its own.
//!
//! The absence ladder (`absence_tests`) decides when `Ok(None)` starts meaning *"the chain provably
//! does not have this"*. These decide when `Ok(Some)` starts meaning *"these heights are the
//! chain's"* — the direction that makes a caller STOP polling and record a fact
//! (dig_ecosystem#2462).

use super::settle_uncorroborated_presence;
use crate::types::{ChainClaim, ChiaQueryError};

/// A stand-in answer whose identity and whose CHAIN CLAIM can vary independently.
///
/// That separation is the whole point of the fixture: the attack is a peer returning a genuine
/// coin's identity with a fabricated height, so a double that could only vary one field could not
/// express the lie under test.
#[derive(Debug, PartialEq, Eq)]
struct Answer {
    identity: &'static str,
    height: u32,
}

impl ChainClaim for Answer {
    fn chain_claim(&self) -> String {
        format!("{} at {}", self.identity, self.height)
    }
}

fn answer(height: u32) -> Answer {
    Answer {
        identity: "coin-a",
        height,
    }
}

/// The coinset tier makes the same claim, so two independent sources now say the same thing.
#[test]
fn a_second_source_making_the_same_claim_makes_it_a_presence() {
    let settled = settle_uncorroborated_presence(answer(100), Some(Ok(Some(answer(100)))));
    assert_eq!(
        settled.expect("two agreeing sources is an answer"),
        Some(answer(100))
    );
}

/// **The uncorroborated path.** Nobody else could be asked, so the height is not evidence.
///
/// The fixture that matters is the ERROR: a caller must be unable to mistake this for `Ok(Some)`,
/// because that mistake is the whole defect — a DID mint recorded from a height one anonymous peer
/// invented.
#[test]
fn presence_nobody_can_corroborate_is_an_error_not_a_some() {
    let settled = settle_uncorroborated_presence(answer(100), None);
    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "an uncorroborated presence must never surface as Ok(Some): got {settled:?}"
    );
}

/// **The same coin, a different height, is a disagreement.**
///
/// Both sources produce a record for the same identity, so an implementation that corroborated on
/// *something came back* — or on the coin id, which is all the coin-id binding covers — would call
/// this agreement. The heights are the fields under attack and they differ.
#[test]
fn the_same_coin_at_a_different_height_is_a_disagreement() {
    let settled = settle_uncorroborated_presence(answer(100), Some(Ok(Some(answer(999)))));
    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "a fabricated height is caught by comparing claims, not identities: got {settled:?}"
    );
}

/// A second source that could not answer has not agreed with anything.
#[test]
fn a_second_source_that_fails_leaves_it_uncorroborated() {
    let settled = settle_uncorroborated_presence(
        answer(100),
        Some(Err(ChiaQueryError::CoinsetHttp("gateway timeout".into()))),
    );
    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "a failed second opinion is not a confirmed first one: got {settled:?}"
    );
}

/// The peer produces the thing, the coinset API says it is not there: refused, not resolved.
///
/// Note which way this does NOT go — it is equally not `Ok(None)`. Preferring either source would
/// be inventing a fact, and the positive direction is the one that makes a caller record it.
#[test]
fn a_source_reporting_absent_contradicts_the_presence() {
    let settled = settle_uncorroborated_presence(answer(100), Some(Ok(None)));
    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "present-then-absent is a disagreement, not a tie to break: got {settled:?}"
    );
}

// ---------------------------------------------------------------------------
// The graded answer reaching the settlement — placement, not logic
// ---------------------------------------------------------------------------
//
// Everything above tests `settle_uncorroborated_presence` as a function. That proves the rule is
// right and proves NOTHING about whether it runs: an `UncorroboratedFound` arm returning
// `Ok(Some(v))` at the call site satisfies every test above, because none of them reach the call
// site. These drive `QueryRouter::settle_peer_answer`, which is where the fix actually lives.
//
// The sibling `UncorroboratedAbsent` arm (from #2456) was in exactly that state and is covered
// here too, rather than left in the condition this one was found in.

use super::QueryRouter;
use crate::coinset::CoinsetClient;
use crate::peer::{OptAnswer, PeerBackend};
use std::time::Duration;

/// A router that dials nothing: the peer pool is empty and the coinset base URL is unroutable.
///
/// Neither tier is ever reached — `settle_peer_answer` is handed the graded answer AND the coinset
/// future by the caller — so this only has to exist, not work.
fn router(coinset_fallback_enabled: bool) -> QueryRouter {
    QueryRouter {
        peer: std::sync::Arc::new(PeerBackend::for_tests()),
        coinset: CoinsetClient::new("http://127.0.0.1:1", Duration::from_millis(1))
            .expect("build a client that is never called"),
        coinset_fallback_enabled,
    }
}

/// The coinset future used wherever the tier MUST NOT be consulted.
///
/// It answers, and it answers differently. A settlement that wrongly awaited it would produce
/// `SourcesDisagree`, which the assertions below distinguish from the expected verdict — so
/// "never consulted" is checked, not assumed.
async fn a_coinset_answer_that_must_not_be_used() -> Result<Option<Answer>, ChiaQueryError> {
    Ok(Some(answer(999)))
}

/// **The placement, stated as a test.** An uncorroborated presence must not escape the settlement.
///
/// With the fallback disabled there is no second source, so the only correct outcome is the
/// refusal. An arm that returned the record — the shape this fix replaced — passes every
/// function-level test in this file and fails here.
#[tokio::test]
async fn an_uncorroborated_presence_does_not_escape_the_settlement() {
    let settled = router(false)
        .settle_peer_answer(
            OptAnswer::UncorroboratedFound(answer(100)),
            a_coinset_answer_that_must_not_be_used(),
        )
        .await;

    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "the record must not reach the caller as a fact: got {settled:?}"
    );
}

/// The uncorroborated presence really is put to the coinset tier when there is one.
///
/// Pins the other half of the placement: the arm must CALL the settlement, not merely refuse. A
/// disagreeing second source is used because it distinguishes "consulted" from "returned the
/// record anyway" — an arm returning `Ok(Some(v))` produces `Ok`, not this error.
#[tokio::test]
async fn an_uncorroborated_presence_is_put_to_the_coinset_tier() {
    let settled = router(true)
        .settle_peer_answer(OptAnswer::UncorroboratedFound(answer(100)), async {
            Ok(Some(answer(999)))
        })
        .await;

    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "the second source was consulted and contradicted the first: got {settled:?}"
    );
}

/// The sibling arm, in the same shape: an uncorroborated ABSENCE does not escape either.
///
/// Pre-existing from #2456 and equally unproven at its call site until now.
#[tokio::test]
async fn an_uncorroborated_absence_does_not_escape_the_settlement() {
    let settled = router(false)
        .settle_peer_answer(
            OptAnswer::<Answer>::UncorroboratedAbsent,
            a_coinset_answer_that_must_not_be_used(),
        )
        .await;

    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedAbsence(_))),
        "an absence nobody corroborated must not reach the caller as Ok(None): got {settled:?}"
    );
}

/// The controls: a corroborated answer in either direction passes straight through.
///
/// Without these, the three tests above would pass on a settlement that refused everything.
#[tokio::test]
async fn corroborated_answers_pass_through_in_both_directions() {
    let present = router(false)
        .settle_peer_answer(
            OptAnswer::Found(answer(100)),
            a_coinset_answer_that_must_not_be_used(),
        )
        .await;
    assert_eq!(
        present.expect("a corroborated presence is an answer"),
        Some(answer(100))
    );

    let absent = router(false)
        .settle_peer_answer(
            OptAnswer::<Answer>::CorroboratedAbsent,
            a_coinset_answer_that_must_not_be_used(),
        )
        .await;
    assert_eq!(absent.expect("a corroborated absence is an answer"), None);
}
