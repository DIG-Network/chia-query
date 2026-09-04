//! What the peer tier will and will not call a corroborated POPULATION.
//!
//! The scalar corroboration tests next door decide when one record becomes a fact. These decide it
//! for a set, which is the harder half: two honest peers at different peaks legitimately return
//! different sets, so the rule has to separate LAG from LYING or it is useless in one direction and
//! dangerous in the other.
//!
//! The three probes chia-query#35/#47 ask for are here by name:
//!
//! 1. [`a_peer_that_hides_a_coin_makes_the_read_refuse`] — one source omits a coin, the read
//!    returns `Err(SourcesDisagree)` and never a shorter set.
//! 2. [`two_peers_one_block_apart_corroborate_at_the_settled_height`] — peers at `P` and `P + 1`
//!    agree, and the answer is dated `P - SETTLED_LAG` with neither the new coin nor the new spend
//!    reflected.
//! 3. [`a_pool_below_the_floor_reports_uncorroborated`] — a pool that cannot corroborate says so
//!    instead of degrading.
//!
//! The peers are REAL — a `Peer` reads its address off a live socket and cannot be mocked — and
//! each is admitted at its own address and its own announced peak, which is what lets a scripted
//! read give different peers different things to say about different heights.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use super::connect::PeerOrigin;
use super::plurality::{PEAK_LAG_EVICTION, SETTLED_LAG};
use super::pool::PeerPool;
use super::set_agreement::{as_of_is_supported, HeightedSet, SetAnswer, SetProjection};
use super::test_support::{loopback_peer, puzzle_state_peer};
use super::PeerBackend;
use crate::types::{ChiaQueryError, Coin, CoinRecord};
use crate::NetworkType;

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

/// What one scripted peer answers: its raw set, and the height it says the set is a snapshot of.
type Script = HashMap<SocketAddr, HeightedSet<CoinRecord>>;

/// A backend over exactly the peers given, in the order given, dialling nothing.
///
/// `max_peers` is the number admitted so `try_refill` is a no-op: a test that reaches the network is
/// not a unit test, and a refill would also change the pool mid-read.
async fn backend_over(
    members: Vec<(PeerOrigin, u32, HeightedSet<CoinRecord>)>,
) -> (PeerBackend, Script) {
    let pool = PeerPool::for_tests(members.len());
    let mut script = Script::new();

    for (origin, announced_peak, answer) in members {
        let peer = loopback_peer().await;
        let addr = peer.socket_addr();
        assert!(
            pool.admit_at_peak_for_tests(peer, addr, origin, announced_peak)
                .await,
            "each scripted peer must be admitted under its own address"
        );
        script.insert(addr, answer);
    }

    let backend = PeerBackend {
        pool,
        network: NetworkType::Mainnet,
        request_timeout: Duration::from_millis(50),
    };
    (backend, script)
}

/// Run the graded set read against `script`.
async fn read_scripted(
    backend: &PeerBackend,
    script: &Script,
    projection: SetProjection,
) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
    backend
        .read_set_corroborated(
            move |peer| {
                let answer = script
                    .get(&peer.socket_addr())
                    .expect("every peer in the pool is scripted")
                    .clone();
                async move { Ok(answer) }
            },
            projection,
        )
        .await
}

fn unfiltered() -> SetProjection {
    SetProjection {
        start_height: None,
        end_height: None,
        include_spent: true,
    }
}

/// A set every source reports identically, at the same height.
fn agreeing(as_of_height: u32) -> HeightedSet<CoinRecord> {
    HeightedSet {
        items: vec![coin(1, 10, None), coin(2, 20, None)],
        as_of_height,
    }
}

/// **Probe 1 (chia-query#47).** One source hides a coin; the read REFUSES.
///
/// The established behaviour returned whatever the first responsive source said, so a peer that
/// stayed quiet about one coin made the network look smaller — and the census derives a per-store
/// requirement from that count, so the omission became a lower requirement, permanently
/// (dig-node#405). The assertion that matters is not merely that an error came back but that a
/// SHORTER SET did not.
#[tokio::test]
async fn a_peer_that_hides_a_coin_makes_the_read_refuse() {
    let hiding = HeightedSet {
        items: vec![coin(1, 10, None)],
        as_of_height: 100,
    };
    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, 100, agreeing(100)),
        (PeerOrigin::Discovered, 100, agreeing(100)),
        (PeerOrigin::Discovered, 100, hiding),
    ])
    .await;

    let answer = read_scripted(&backend, &script, unfiltered()).await;

    assert!(
        matches!(answer, Err(ChiaQueryError::SourcesDisagree(_))),
        "one source omitting a coin must refuse, never return the shorter set: got {answer:?}"
    );
}

/// **Probe 2 (the honest-lag case).** Peers at `P` and `P + 1` CORROBORATE.
///
/// The faster peer has seen a coin created at `P + 1` and a spend that landed at `P + 1`. Held to
/// the settled height both peers passed some time ago, the two sets are identical — so ordinary lag
/// produces agreement rather than a permanent `SourcesDisagree`, which is the failure that would
/// stall every balance read on an active puzzle hash.
///
/// The answer is dated, and the date is checked: `P - SETTLED_LAG`, not the faster peer's tip.
#[tokio::test]
async fn two_peers_one_block_apart_corroborate_at_the_settled_height() {
    const P: u32 = 100;

    let slow = HeightedSet {
        items: vec![coin(1, 10, None), coin(2, 20, None)],
        as_of_height: P,
    };
    let fast = HeightedSet {
        items: vec![
            coin(1, 10, None),
            // spent one block after the slow peer's tip
            coin(2, 20, Some(P + 1)),
            // created one block after the slow peer's tip
            coin(3, P + 1, None),
        ],
        as_of_height: P + 1,
    };

    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, P, slow),
        (PeerOrigin::Discovered, P + 1, fast.clone()),
        (PeerOrigin::Discovered, P + 1, fast),
    ])
    .await;

    let answer = read_scripted(&backend, &script, unfiltered())
        .await
        .expect("a one-block lead is not a disagreement");

    let SetAnswer::Corroborated {
        items,
        as_of_height,
    } = answer
    else {
        panic!("three agreeing peers must corroborate: got {answer:?}");
    };

    assert_eq!(
        as_of_height,
        P - SETTLED_LAG,
        "the answer is dated by the SLOWEST source, less the settled lag"
    );
    assert_eq!(items.len(), 2, "the coin created at P+1 did not exist yet");
    assert!(
        items.iter().all(|c| !c.spent),
        "the spend that landed at P+1 had not happened at the settled height"
    );
}

/// **Probe 3.** A pool that cannot corroborate says so rather than degrading.
///
/// One discovered peer answers and there is nobody independent to put it to. The answer is carried
/// — it is a real answer and the router may yet find a second voice for it — but it is NOT reported
/// as corroborated, and the router settles it (see `router::set_settlement_tests`).
#[tokio::test]
async fn a_pool_below_the_floor_reports_uncorroborated() {
    let (backend, script) = backend_over(vec![(PeerOrigin::Discovered, 100, agreeing(100))]).await;

    let answer = read_scripted(&backend, &script, unfiltered())
        .await
        .expect("a lone answer is not an ERROR, it is an ungraded fact");

    assert!(
        matches!(answer, SetAnswer::Uncorroborated { .. }),
        "one voice is not corroboration: got {answer:?}"
    );
}

/// A peer that is not an INDEPENDENT peer cannot corroborate a set either.
///
/// Two peers agree, but one of them was reached from a preferred address — an operator's node or
/// one on this machine — which is an excellent peer to READ from and is not evidence about the
/// chain independent of this host (dig_ecosystem#2648). Without this, a single local process could
/// corroborate every set read on the machine.
#[tokio::test]
async fn a_priority_peer_cannot_corroborate_a_set() {
    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, 100, agreeing(100)),
        (PeerOrigin::Priority, 100, agreeing(100)),
        (PeerOrigin::Priority, 100, agreeing(100)),
    ])
    .await;

    let answer = read_scripted(&backend, &script, unfiltered())
        .await
        .expect("agreement from a co-resident node is not an error");

    assert!(
        matches!(answer, SetAnswer::Uncorroborated { .. }),
        "priority peers are not independent voices: got {answer:?}"
    );
}

/// **A badly-lagging peer never gets to drag the date down: the pool evicts it first.**
///
/// A peer announcing a peak far below the pool's reference trails by more than
/// [`PEAK_LAG_EVICTION`](super::plurality::PEAK_LAG_EVICTION), and `pick` runs the pool's
/// maintenance pass before every read — so it is gone before the round starts. The two survivors
/// then leave only ONE corroborator behind the answering peer, and the read reports exactly that
/// rather than corroborating on a pool it has just shrunk.
///
/// This is the interaction between the set rule and the membership policy, and it is worth pinning:
/// eviction bounds how far a peer with a LOW ANNOUNCED PEAK can move the common height, and the
/// honest report of the resulting thin pool is what stops that bound from being paid for with a
/// silently degraded quorum.
#[tokio::test]
async fn a_peer_announcing_a_far_behind_peak_is_evicted_before_it_can_move_the_date() {
    let recent = HeightedSet {
        items: vec![coin(1, 10, None), coin(2, 90, None)],
        as_of_height: 100,
    };

    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, 100, recent.clone()),
        (PeerOrigin::Discovered, 100, recent.clone()),
        (
            PeerOrigin::Discovered,
            50,
            HeightedSet {
                items: vec![coin(1, 10, None)],
                as_of_height: 50,
            },
        ),
    ])
    .await;

    let answer = read_scripted(&backend, &script, unfiltered())
        .await
        .expect("evicting a laggard is not an error");

    assert_eq!(
        answer.as_of_height(),
        100 - SETTLED_LAG,
        "the evicted peer's height must not reach the round"
    );
    assert_eq!(
        answer.items().len(),
        2,
        "and the coin created at 90 must still be in the answer"
    );
    assert!(
        matches!(answer, SetAnswer::Uncorroborated { .. }),
        "one corroborator is below the floor, and that is reported rather than absorbed: \
         got {answer:?}"
    );
}

/// **What eviction does NOT cover: a peer that announces a current peak and ANSWERS at an old one.**
///
/// Membership is judged on the announced peak, and the puzzle-state read's as-of height comes from
/// the RESPONSE, so a peer in good standing can still answer as of a much older block. Because the
/// common height is a `min`, that drags the whole round back to the older block.
///
/// **The answer is still TRUE — it is a correct statement about the chain at that height — and
/// `as_of_height` is what says which height that is.** This is the residue the graded type exists
/// to carry: a hostile source can make an answer STALE and cannot make it WRONG, and a consumer
/// that ignores the date is the one who turns the first into the second. The alternative aggregate,
/// a maximum, would let one inflated claim win outright.
#[tokio::test]
async fn a_source_answering_as_of_an_old_block_moves_the_date_not_the_truth() {
    let recent = HeightedSet {
        items: vec![coin(1, 10, None), coin(2, 90, None)],
        as_of_height: 100,
    };
    // Announces a current peak, so the pool keeps it; answers as of block 50.
    let backdating = HeightedSet {
        items: vec![coin(1, 10, None)],
        as_of_height: 50,
    };

    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, 100, recent.clone()),
        (PeerOrigin::Discovered, 100, recent),
        (PeerOrigin::Discovered, 100, backdating),
    ])
    .await;

    let answer = read_scripted(&backend, &script, unfiltered())
        .await
        .expect("held to the older block, every source reports the same set");

    let SetAnswer::Corroborated {
        items,
        as_of_height,
    } = answer
    else {
        panic!("the sets match once normalised, so this corroborates: got {answer:?}");
    };

    assert_eq!(
        as_of_height,
        50 - SETTLED_LAG,
        "the date follows the lowest as-of height in the round"
    );
    assert_eq!(
        items.len(),
        1,
        "the coin created at 90 is above the height the round was held to, and the caller can \
         see that from as_of_height"
    );
}

/// A corroborator whose read FAILS has agreed with nothing, and is not counted towards the floor.
///
/// Silence is the cheapest thing a hostile peer can offer, so reading "I asked and heard no
/// contradiction" as agreement would make the floor free to clear.
#[tokio::test]
async fn a_corroborator_that_fails_does_not_count_towards_the_floor() {
    let (backend, script) = backend_over(vec![
        (PeerOrigin::Discovered, 100, agreeing(100)),
        (PeerOrigin::Discovered, 100, agreeing(100)),
        (PeerOrigin::Discovered, 100, agreeing(100)),
    ])
    .await;

    // Everyone but the first peer to be asked fails. `select_peer` is round-robin from index 0, so
    // the peer that answers is the one admitted first, and the other two are the corroborators.
    let answering = *script.keys().min().expect("three peers were admitted");
    let answer = backend
        .read_set_corroborated(
            |peer| {
                let addr = peer.socket_addr();
                let scripted = script.get(&addr).expect("scripted").clone();
                async move {
                    if addr == answering {
                        Ok(scripted)
                    } else {
                        Err(ChiaQueryError::PeerConnection("scripted failure".into()))
                    }
                }
            },
            unfiltered(),
        )
        .await;

    match answer {
        Ok(SetAnswer::Uncorroborated { .. }) => {}
        // The round-robin index is not something this test controls, so the case where the
        // ANSWERING peer is one of the failing ones is a legitimate outcome too: the read fails
        // outright, which is also not a corroboration.
        Err(ChiaQueryError::PeerConnection(_)) => {}
        other => panic!("silence must never read as agreement: got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The as-of height a peer states on the WIRE is held to what it has announced
// ---------------------------------------------------------------------------

/// **A peer cannot state an as-of height its own announcements do not support.**
///
/// Pinned from BOTH sides of the bar, because either alone passes a wrong implementation: a rule
/// that refused everything satisfies the "too low" case, and one that accepted everything satisfies
/// the "at the bar" case.
#[test]
fn the_supported_as_of_bar_is_pinned_from_both_sides() {
    const PEAK: u32 = 9_000_000;

    assert!(
        as_of_is_supported(Some(PEAK), PEAK),
        "answering as of the block it just announced is the ordinary case"
    );
    assert!(
        as_of_is_supported(Some(PEAK), PEAK - PEAK_LAG_EVICTION),
        "exactly at the bar is supported — a bound that starves the work it protects is a defect \
         in its own right"
    );
    assert!(
        !as_of_is_supported(Some(PEAK), PEAK - PEAK_LAG_EVICTION - 1),
        "one block past the bar is not"
    );
    assert!(
        !as_of_is_supported(Some(PEAK), 2),
        "and the attack itself — a current peak beside an ancient answer — is refused"
    );
}

/// **There is no UPPER bound, deliberately.**
///
/// A source overstating its as-of height cannot lower the round's `min`, so it cannot hide a coin
/// behind a height nobody else reached; and its own set is normalised at the round's height like
/// every other, so an answer short of what it claimed reads as a contradiction. Bounding above
/// would refuse an honest peer whose long paged walk finished after its last announcement reached
/// us — a stall bought for no security.
#[test]
fn a_source_may_answer_above_the_peak_it_last_announced() {
    assert!(as_of_is_supported(Some(100), 100 + 10_000));
}

/// **A peer that has announced NOTHING supports no as-of height at all.**
///
/// Zero would read as a real height, drag the round's `min` to it, normalise every answer to the
/// empty set, and report that emptiness as corroborated — which is the same under-report by a
/// different door.
#[test]
fn a_peer_that_has_announced_nothing_supports_no_as_of_height() {
    assert!(!as_of_is_supported(None, 0));
    assert!(!as_of_is_supported(None, 9_000_000));
}

/// A backend holding exactly one peer that ANSWERS puzzle-state, admitted at `announced_peak`.
async fn backend_over_answering_peer(
    announced_peak: u32,
    response: chia_protocol::RespondPuzzleState,
) -> (PeerBackend, chia_wallet_sdk::client::Peer) {
    let pool = PeerPool::for_tests(1);
    let peer = puzzle_state_peer(response).await;
    let addr = peer.socket_addr();
    assert!(
        pool.admit_at_peak_for_tests(peer.clone(), addr, PeerOrigin::Discovered, announced_peak)
            .await
    );
    let backend = PeerBackend {
        pool,
        network: NetworkType::Mainnet,
        request_timeout: Duration::from_secs(5),
    };
    (backend, peer)
}

fn finished_at(height: u32) -> chia_protocol::RespondPuzzleState {
    chia_protocol::RespondPuzzleState::new(
        vec![chia_protocol::Bytes32::new([9; 32])],
        height,
        chia_protocol::Bytes32::new([0xAB; 32]),
        true,
        Vec::new(),
    )
}

const PUZZLE_HASH: &str = "0909090909090909090909090909090909090909090909090909090909090909";

/// **THE DEFECT (chia-query#56): a peer's WIRE-reported as-of height, taken on its word, lets one
/// peer drag the round's common height low enough to clip every honest answer to the empty set.**
///
/// The peer here announces a current peak — so the pool holds it, and lag eviction never looks at
/// it — and answers as of block 2. `common_height` is a `min`, so that one number becomes the whole
/// round's, `normalise_at` drops every coin created above it, and the resulting empty sets AGREE.
/// The round then reports `Corroborated` emptiness: a peer that cannot forge a coin makes this
/// crate confidently report NO coins, on the path a wallet balance is read through.
///
/// The read must REFUSE instead. Both fixtures answer over a real websocket through the production
/// decode path, because the value under test only exists on that path — a scripted `HeightedSet`
/// would bypass the very check this pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wire_as_of_height_the_peer_has_not_announced_is_refused() {
    const PEAK: u32 = 9_000_000;
    let (backend, peer) = backend_over_answering_peer(PEAK, finished_at(2)).await;

    let answer = backend
        .do_puzzle_hash_query(&peer, &[PUZZLE_HASH], false)
        .await;

    match answer {
        Err(ChiaQueryError::PeerRejection(detail)) => {
            assert!(
                detail.contains("does not support"),
                "the refusal must name the reason it refused: {detail}"
            );
        }
        other => panic!(
            "a peer answering as of block 2 while announcing {PEAK} must be refused, not believed: \
             {other:?}"
        ),
    }
}

/// The control: a peer answering AT its announced peak is believed, and its as-of height survives.
///
/// Without this the test above passes against a read that refuses everything — which would take the
/// balance path from a wrong answer to no answer at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wire_as_of_height_its_announcements_support_is_accepted() {
    const PEAK: u32 = 9_000_000;
    let (backend, peer) = backend_over_answering_peer(PEAK, finished_at(PEAK)).await;

    let set = backend
        .do_puzzle_hash_query(&peer, &[PUZZLE_HASH], false)
        .await
        .expect("a peer answering at the peak it announced must be believed");

    assert_eq!(
        set.as_of_height, PEAK,
        "and the height it stated is carried through, not replaced by the pool's own"
    );
}
