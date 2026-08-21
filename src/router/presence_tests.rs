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
