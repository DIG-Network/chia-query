//! What the router does with a population answer the peer tier could not corroborate on its own.
//!
//! The scalar ladders (`presence_tests`, `absence_tests`) decide when a single record becomes a
//! fact. These decide the same thing for a SET — the read a wallet balance and the collateral
//! census are taken through, and the one where an omission is free while an addition costs
//! on-chain collateral (chia-query#47).

use super::settle_uncorroborated_set;
use crate::peer::SetProjection;
use crate::types::{ChiaQueryError, Coin, CoinRecord};

/// A coin record with only the fields the set rule reads. `n` names the coin.
fn coin(n: u8, created: u32, spent: Option<u32>) -> CoinRecord {
    CoinRecord {
        coin: Coin {
            parent_coin_info: format!("parent{n:02}"),
            puzzle_hash: "ph".into(),
            amount: u64::from(n),
        },
        confirmed_block_index: created,
        spent_block_index: spent.unwrap_or(0),
        spent: spent.is_some(),
        coinbase: false,
        timestamp: 0,
    }
}

/// The caller's question, in the shape every graded read passes down.
fn unfiltered() -> SetProjection {
    SetProjection {
        start_height: None,
        end_height: None,
        include_spent: true,
    }
}

/// The coinset tier reports the same set at the same height, so two independent sources agree.
#[test]
fn a_second_source_reporting_the_same_set_makes_it_corroborated() {
    let peer = vec![coin(1, 10, None), coin(2, 20, None)];

    let settled = settle_uncorroborated_set(
        peer.clone(),
        100,
        unfiltered(),
        Some(Ok(vec![coin(2, 20, None), coin(1, 10, None)])),
    )
    .expect("two agreeing sources is an answer");

    assert_eq!(settled.as_of_height, 100);
    assert_eq!(settled.items.len(), peer.len());
}

/// **The attack, at the router tier.** A coinset answer missing a coin the peer reported is a
/// disagreement, and the caller gets `Err` rather than the shorter of the two sets.
#[test]
fn a_second_source_that_omits_a_coin_is_a_disagreement_not_the_shorter_set() {
    let settled = settle_uncorroborated_set(
        vec![coin(1, 10, None), coin(2, 20, None)],
        100,
        unfiltered(),
        Some(Ok(vec![coin(1, 10, None)])),
    );

    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "an omission must never be settled by taking the smaller set: got {settled:?}"
    );
}

/// **The uncorroborated path.** Nobody else could be asked, so the set is not evidence.
///
/// The fixture that matters is the ERROR: a caller must be unable to mistake this for a short but
/// usable answer, because that mistake is the whole defect — a census counting fewer stores than
/// exist, permanently (dig-node#405).
#[test]
fn a_set_nobody_will_second_is_an_error_not_a_shorter_ok() {
    let settled = settle_uncorroborated_set(vec![coin(1, 10, None)], 100, unfiltered(), None);

    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "an uncorroborated set must never surface as Ok: got {settled:?}"
    );
}

/// A second source that could not answer has not agreed with anything.
#[test]
fn a_second_source_that_fails_leaves_the_set_uncorroborated() {
    let settled = settle_uncorroborated_set(
        vec![coin(1, 10, None)],
        100,
        unfiltered(),
        Some(Err(ChiaQueryError::CoinsetHttp("gateway timeout".into()))),
    );

    assert!(
        matches!(settled, Err(ChiaQueryError::UncorroboratedPresence(_))),
        "a failed second source is not a second source: got {settled:?}"
    );
}

/// **Coinset is held to the peer answer's height, not to its own tip.**
///
/// The coinset tier answers as of NOW, so it legitimately holds a coin created after `H` and a
/// spend that landed after `H`. Comparing its raw answer would report a disagreement between two
/// honest sources on every active puzzle hash — the "too strict" failure, which stalls the census
/// on a transport error.
#[test]
fn coinset_is_normalised_to_the_peer_answers_height_before_comparison() {
    let peer_at_100 = vec![coin(1, 10, None), coin(2, 20, None)];
    let coinset_at_the_tip = vec![
        coin(1, 10, None),
        coin(2, 20, Some(140)),
        coin(3, 130, None),
    ];

    let settled = settle_uncorroborated_set(
        peer_at_100,
        100,
        unfiltered(),
        Some(Ok(coinset_at_the_tip)),
    )
    .expect("a fresher second source is not a disagreement");

    assert_eq!(settled.items.len(), 2, "the coin created at 130 is not yet");
    assert_eq!(settled.as_of_height, 100);
}

/// The empty set is a VALUE. Two sources agreeing that nothing is there is an answer, not a
/// failure — and it must not be reachable any other way.
#[test]
fn two_sources_agreeing_on_nothing_is_a_corroborated_empty_set() {
    let settled = settle_uncorroborated_set(Vec::new(), 100, unfiltered(), Some(Ok(Vec::new())))
        .expect("agreement on emptiness is agreement");

    assert!(settled.items.is_empty());
    assert_eq!(settled.as_of_height, 100);
}

/// The mirror of the omission case: coinset holding a coin the peer did not report, at a height
/// the peer had already reached, is a disagreement too. Addition costs an attacker real collateral,
/// but the rule is equality either way.
#[test]
fn a_second_source_that_adds_a_settled_coin_is_a_disagreement() {
    let settled = settle_uncorroborated_set(
        vec![coin(1, 10, None)],
        100,
        unfiltered(),
        Some(Ok(vec![coin(1, 10, None), coin(9, 90, None)])),
    );

    assert!(
        matches!(settled, Err(ChiaQueryError::SourcesDisagree(_))),
        "a coin settled at 90 is inside the answer's own height: got {settled:?}"
    );
}
