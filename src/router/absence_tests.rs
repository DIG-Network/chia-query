//! What the router does with an absence the peer tier could not corroborate on its own.
//!
//! The peer tier hands up an ungraded fact; this is where it becomes the router's published
//! contract, so these pin the exact point at which `Ok(None)` starts meaning *"the chain provably
//! does not have this"* (dig_ecosystem#2456).

use super::settle_uncorroborated_absence;
use crate::types::ChiaQueryError;

/// The coinset tier agrees, so two independent sources now say the same thing.
#[test]
fn a_second_source_agreeing_makes_it_an_absence() {
    let settled = settle_uncorroborated_absence::<&str>(Some(Ok(None)));
    assert_eq!(settled.expect("two agreeing sources is an answer"), None);
}

/// **The uncorroborated path.** Nobody else could be asked, so nothing is known.
///
/// The fixture that matters is the ERROR, not the absence: a caller must be unable to mistake this
/// for `Ok(None)`, because that mistake is the whole defect — a DID mint's funding coin reported
/// as provably missing on one anonymous peer's empty list.
#[test]
fn absence_nobody_can_corroborate_is_an_error_not_a_none() {
    let settled = settle_uncorroborated_absence::<&str>(None);
    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedAbsence(_))),
        "an uncorroborated absence must never surface as Ok(None): got {settled:?}"
    );
}

/// A second source that could not answer has not agreed with anything.
#[test]
fn a_second_source_that_fails_leaves_it_uncorroborated() {
    let settled = settle_uncorroborated_absence::<&str>(Some(Err(ChiaQueryError::CoinsetHttp(
        "gateway timeout".into(),
    ))));
    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedAbsence(_))),
        "a failed second opinion is not a confirmed first one: got {settled:?}"
    );
}

/// The peer says absent, the coinset API produces the thing: refused, not resolved.
///
/// Note which way this does NOT go — it is equally not `Ok(Some)`. Preferring the positive answer
/// would be defensible-sounding and would still be inventing a fact, since a source willing to
/// fabricate an absence is equally willing to fabricate a record.
#[test]
fn contradiction_between_the_tiers_is_surfaced() {
    let settled = settle_uncorroborated_absence(Some(Ok(Some("the thing"))));
    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "a disagreement is evidence, not a tie to break: got {settled:?}"
    );
}
