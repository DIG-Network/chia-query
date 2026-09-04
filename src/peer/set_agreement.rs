//! What AGREEMENT means for a read that returns a SET.
//!
//! The two graded scalar reads compare a [`ChainClaim`] string and are done. A population read —
//! `coin_records_by_puzzle_hash` and its siblings — returns a `Vec<CoinRecord>`, and two HONEST
//! peers at different peaks legitimately return different vectors: a coin created in the last
//! block appears on one and not the other, and a coin spent in the last block is dropped by one
//! and kept by the other. So `Vec` equality would produce permanent
//! [`SourcesDisagree`](crate::types::ChiaQueryError::SourcesDisagree) on any active puzzle hash.
//!
//! The two rules that look reasonable and are not:
//!
//! - **Subset** — one peer may omit a coin unchallenged.
//! - **Union** — every peer may add coins unchallenged, and omissions are still free.
//!
//! Omission is the direction that costs the network money (chia-query#47): a source drops a coin by
//! staying quiet, while ADDING one requires somebody to have posted real collateral on chain. So
//! the rule here is EQUALITY, at a height both answers can be held to.
//!
//! # The anchor already existed
//!
//! `RespondPuzzleState` carries `height` and `header_hash` — the peer's own statement of the block
//! its answer is a snapshot of. `do_puzzle_hash_query` discarded both after the page loop. Keeping
//! the height is what makes set equality well defined: normalise every answer DOWN to a common
//! height, then compare.
//!
//! # The rule
//!
//! Two population answers AGREE iff, after both are normalised to the same common height `H`, they
//! are equal as sets keyed by coin identity, with an equal [`ChainClaim`] per coin.
//!
//! `H = min(as-of height of every answering source) - `[`SETTLED_LAG`], clamped down to the
//! caller's `end_height`. `H` normalises DOWN and never up: a coin created in the last couple of
//! blocks is absent from a corroborated answer, and [`SetAnswer::as_of_height`] says so. The
//! opposite choice — each peer judged at its own tip — reports settled money as missing without
//! saying it did.
//!
//! # Why the wire request must ask for spent coins
//!
//! Normalisation can only demote a spend it can SEE. If the request is made with
//! `include_spent = false`, a peer one block ahead silently omits a coin spent at `H + 1` and no
//! amount of normalisation recovers it, so an honest peer pair reads as a contradiction — or worse,
//! as agreement on a set that is missing a coin. The wire request is therefore made with
//! `include_spent = true` regardless of what the caller asked for, and the caller's projection is
//! applied client-side, AFTER agreement.
//!
//! # What a wrong version costs
//!
//! Too strict — permanent `SourcesDisagree`, and the census and balance reads stall on a transport
//! error. Costly, visible, survivable. Too loose — one hostile peer omits a coin, the census counts
//! fewer stores, the requirement drops, permanently (dig-node#405). Not survivable. When a case is
//! genuinely ambiguous this module refuses.

use std::collections::BTreeMap;

use crate::types::{ChainClaim, CoinRecord};

use super::plurality::SETTLED_LAG;

// ---------------------------------------------------------------------------
// SetAnswer
// ---------------------------------------------------------------------------

/// A population read graded the way [`OptAnswer`](super::OptAnswer) grades a scalar one.
///
/// There is deliberately **no absence arm**. An empty set is a VALUE — "nothing is there at `H`" —
/// and it is corroborated or not on exactly the same terms as a set of a thousand coins. The
/// absence vocabulary belongs to a scalar read, where "no such coin" and "a coin with these
/// fields" are different KINDS of answer; for a set they are the same kind with a different
/// cardinality, and giving the empty case its own arm would invite a consumer to treat a
/// corroborated emptiness as a failure.
///
/// Disagreement is NOT an arm either: it is
/// [`SourcesDisagree`](crate::types::ChiaQueryError::SourcesDisagree), an `Err`, so a consumer can
/// never read it as "unknown" or as "none". `Err` is unknown; `Ok` is agreement; an empty `Vec` is
/// never manufactured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetAnswer<T> {
    /// [`CORROBORATION_FLOOR`](super::plurality::CORROBORATION_FLOOR) independent peers returned
    /// the same normalised set at `as_of_height`.
    Corroborated {
        /// The agreed set, with the caller's projection applied.
        items: Vec<T>,
        /// The height every source in the round was held to.
        as_of_height: u32,
    },
    /// One peer's set at `as_of_height`; no floor of independent peers could be brought to agree.
    ///
    /// Not a failure and not evidence about the chain. The router settles it against another tier
    /// or surfaces it as
    /// [`UncorroboratedPresence`](crate::types::ChiaQueryError::UncorroboratedPresence).
    Uncorroborated {
        /// The one source's set, with the caller's projection applied.
        items: Vec<T>,
        /// The height that source was held to.
        as_of_height: u32,
    },
}

impl<T> SetAnswer<T> {
    /// The height this answer is a true statement about.
    ///
    /// Load-bearing, not decoration: a set normalised to `H` is a TRUE answer about the chain at
    /// `H`, and becomes a lie only if the consumer assumes it is about the tip.
    pub fn as_of_height(&self) -> u32 {
        match self {
            Self::Corroborated { as_of_height, .. } | Self::Uncorroborated { as_of_height, .. } => {
                *as_of_height
            }
        }
    }

    /// The set itself, whatever its grade.
    pub fn items(&self) -> &[T] {
        match self {
            Self::Corroborated { items, .. } | Self::Uncorroborated { items, .. } => items,
        }
    }

    /// The set itself, consuming the answer.
    pub fn into_items(self) -> Vec<T> {
        match self {
            Self::Corroborated { items, .. } | Self::Uncorroborated { items, .. } => items,
        }
    }
}

/// A population read the ROUTER has settled — a set that two independent sources agree on, and
/// the height they agree about.
///
/// There is no uncorroborated arm, deliberately: by the time a set reaches a caller of the router
/// it has either been agreed by [`CORROBORATION_FLOOR`](super::plurality::CORROBORATION_FLOOR)
/// peers or seconded by the coinset tier, or the call returned `Err`. A caller cannot be handed a
/// set nobody would second and be left to notice.
///
/// `as_of_height` travels WITH the set rather than beside it, because the set is a true statement
/// about the chain at that height and a false one about the tip. Dropping it is how a settled
/// answer becomes a stale claim nobody can date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorroboratedSet<T> {
    /// The agreed set, with the caller's projection applied.
    pub items: Vec<T>,
    /// The height the agreeing sources were held to.
    pub as_of_height: u32,
}

/// One source's RAW answer to a population read, together with the height it is a snapshot of.
///
/// "Raw" means: every coin the source knows about in range, spent ones included, with no client
/// side filtering applied at all. Filtering here would throw away the information normalisation
/// needs (see the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeightedSet<T> {
    /// Every record the source returned, unfiltered.
    pub items: Vec<T>,
    /// The block this source states its answer is a snapshot of.
    pub as_of_height: u32,
}

/// What the CALLER asked for, applied after agreement rather than before it.
///
/// A projection applied per source, before comparison, is how two honest truncations become a
/// false [`SourcesDisagree`](crate::types::ChiaQueryError::SourcesDisagree) and how a truncation on
/// one side alone becomes an omission nobody notices (chia-query#33).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SetProjection {
    /// Drop coins created below this height. Applied after agreement.
    pub start_height: Option<u32>,
    /// The caller's `end_height`, folded into `H` rather than applied separately — normalising to
    /// `H <= end_height` already drops everything created above it.
    pub end_height: Option<u32>,
    /// Whether the caller wants coins that are spent AT `H`. Applied after agreement.
    pub include_spent: bool,
}

// ---------------------------------------------------------------------------
// SetMember
// ---------------------------------------------------------------------------

/// A record that can be held to a height and compared as part of a set.
///
/// [`ChainClaim`] answers "what does this assert about the chain"; this answers "which coin is it,
/// and where does it sit" — the two questions normalisation and set comparison need.
pub trait SetMember: ChainClaim + Clone {
    /// The record's IDENTITY, independent of where it sits on the chain.
    ///
    /// Two records with this key equal are claims about the same coin, so a difference in their
    /// [`ChainClaim`] is a CONTRADICTION rather than two unrelated coins. Keying the comparison on
    /// identity rather than on the whole claim is what lets the error name the coin the sources
    /// disagree about instead of reporting one extra and one missing.
    fn identity(&self) -> String;

    /// The height the record says the coin was created at.
    fn created_height(&self) -> u32;

    /// The height the record says the coin was spent at, or `None` if it reports it unspent.
    fn spent_height(&self) -> Option<u32>;

    /// The same record with its spend removed — what it looked like before it was spent.
    fn with_spend_dropped(&self) -> Self;
}

impl SetMember for CoinRecord {
    fn identity(&self) -> String {
        format!(
            "{}:{}:{}",
            self.coin.parent_coin_info, self.coin.puzzle_hash, self.coin.amount
        )
    }

    fn created_height(&self) -> u32 {
        self.confirmed_block_index
    }

    fn spent_height(&self) -> Option<u32> {
        if self.spent {
            Some(self.spent_block_index)
        } else {
            None
        }
    }

    fn with_spend_dropped(&self) -> Self {
        Self {
            spent: false,
            spent_block_index: 0,
            ..self.clone()
        }
    }
}

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

/// The height every source in a round is held to, or `None` when no source answered.
///
/// `min` of the announced as-of heights, less [`SETTLED_LAG`], then clamped DOWN to the caller's
/// `end_height` when it asked for one.
///
/// **`min` is the security of it.** A hostile source can drag `H` DOWN — producing a stale but
/// TRUE answer whose staleness the caller can see on
/// [`SetAnswer::as_of_height`] — and cannot drag it UP, which is what would let it hide a coin
/// behind a height nobody else reached.
pub fn common_height(as_of: &[u32], end_height: Option<u32>) -> Option<u32> {
    let settled = as_of.iter().copied().min()?.checked_sub(SETTLED_LAG)?;
    Some(match end_height {
        Some(end) => settled.min(end),
        None => settled,
    })
}

/// Hold `items` to the chain as it stood at `h`.
///
/// Two moves, and they are not symmetric because the two facts are not:
///
/// - a coin CREATED above `h` did not exist yet, so it is dropped;
/// - a coin SPENT above `h` existed and was unspent, so it is kept and demoted to unspent.
///
/// Demoting rather than dropping is the half that matters: a coin spent at `h + 1` is a coin the
/// caller still owned at `h`, and dropping it would under-report exactly the way an omission does.
pub fn normalise_at<T: SetMember>(items: &[T], h: u32) -> Vec<T> {
    items
        .iter()
        .filter(|item| item.created_height() <= h)
        .map(|item| match item.spent_height() {
            Some(spent) if spent > h => item.with_spend_dropped(),
            _ => item.clone(),
        })
        .collect()
}

/// The comparable fingerprint of a normalised set: identity -> chain claim.
///
/// A `BTreeMap` rather than a set of claims so a disagreement can NAME the coin. Two sources that
/// each return the same coin id with a different `created_height` produce one key with two values,
/// which reads as "these sources disagree about coin X" instead of "one extra, one missing".
pub fn fingerprint<T: SetMember>(items: &[T]) -> BTreeMap<String, String> {
    items
        .iter()
        .map(|item| (item.identity(), item.chain_claim()))
        .collect()
}

/// The FIRST difference between two normalised fingerprints, rendered, or `None` if they are equal.
///
/// Any difference is a contradiction — an extra coin, a missing coin, or the same coin with a
/// different claim. There is no tolerance and no vote: nothing in two contradictory sets says which
/// to believe, so counting them would invent a fact.
pub fn contradiction(
    asked: &str,
    asked_set: &BTreeMap<String, String>,
    other: &str,
    other_set: &BTreeMap<String, String>,
) -> Option<String> {
    for (identity, claim) in asked_set {
        match other_set.get(identity) {
            Some(other_claim) if other_claim == claim => {}
            Some(other_claim) => {
                return Some(format!(
                    "{asked} claims `{claim}` for coin {identity}, {other} claims `{other_claim}`"
                ))
            }
            None => {
                return Some(format!(
                    "{asked} reports coin {identity} (`{claim}`), {other} omits it"
                ))
            }
        }
    }

    // The other direction is not implied by the first: a source that ADDS a coin agrees about
    // every coin the asked peer returned, and a one-way walk would call that agreement.
    for (identity, claim) in other_set {
        if !asked_set.contains_key(identity) {
            return Some(format!(
                "{other} reports coin {identity} (`{claim}`), {asked} omits it"
            ));
        }
    }

    None
}

/// Apply what the CALLER asked for to an already-agreed set.
///
/// Runs after agreement, never before it, and never on one side of a comparison alone.
pub fn project<T: SetMember>(items: Vec<T>, projection: SetProjection) -> Vec<T> {
    items
        .into_iter()
        .filter(|item| {
            let above_start = projection
                .start_height
                .is_none_or(|start| item.created_height() >= start);
            let wanted = projection.include_spent || item.spent_height().is_none();
            above_start && wanted
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Coin;

    /// A coin record with only the fields the rule reads. `n` names the coin.
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

    /// `H` is the LOWEST as-of height less the lag, never an average and never the highest.
    ///
    /// Pinned from both sides: the value is `min - SETTLED_LAG`, and it is that *because* it is
    /// the min — a fixture whose lowest source is neither first nor last would pass an
    /// implementation that read one fixed position otherwise.
    #[test]
    fn the_common_height_is_the_lowest_as_of_less_the_settled_lag() {
        assert_eq!(common_height(&[900, 880, 895], None), Some(880 - SETTLED_LAG));
        assert_eq!(common_height(&[880], None), Some(880 - SETTLED_LAG));
    }

    /// A hostile source can only drag `H` DOWN. Worth pinning explicitly, because the obvious
    /// alternative — the highest as-of, so the answer is as fresh as possible — is the aggregate
    /// one inflated claim wins outright.
    #[test]
    fn one_source_claiming_a_high_as_of_cannot_raise_the_common_height() {
        let honest = common_height(&[900, 901], None).expect("two honest sources");
        let with_liar = common_height(&[900, 901, 9_000_000], None).expect("plus one inflated");
        assert_eq!(honest, with_liar, "the maximum must not move H");
    }

    /// The caller's `end_height` clamps `H` DOWN and never up: a caller asking about the settled
    /// past must not be answered about the tip, and one asking about the future must not be told
    /// `H` is further along than the sources are.
    #[test]
    fn the_callers_end_height_clamps_the_common_height_downwards_only() {
        assert_eq!(common_height(&[900], Some(500)), Some(500));
        assert_eq!(
            common_height(&[900], Some(50_000)),
            Some(900 - SETTLED_LAG),
            "an end_height above the sources cannot invent chain they have not seen"
        );
    }

    /// Nothing settled can be said about a chain shorter than the lag, and `0` is not the honest
    /// answer to that — a set normalised at height 0 is empty, which is the exact under-report
    /// this module exists to refuse.
    #[test]
    fn there_is_no_common_height_below_the_lag_and_no_source_means_no_height() {
        assert_eq!(common_height(&[1], None), None);
        assert_eq!(common_height(&[], None), None);
    }

    /// A coin created above `H` did not exist yet, so it is not in the answer.
    #[test]
    fn normalising_drops_a_coin_created_above_the_common_height() {
        let items = [coin(1, 100, None), coin(2, 105, None)];

        let at = normalise_at(&items, 102);

        assert_eq!(at.len(), 1, "the coin created at 105 did not exist at 102");
        assert_eq!(at[0].identity(), coin(1, 100, None).identity());
    }

    /// **The half that decides whether this module under-reports.**
    ///
    /// A coin SPENT above `H` existed and was unspent at `H`. Dropping it — the naive reading of
    /// "hold the set to `H`" — removes a coin the caller still owned, which is indistinguishable
    /// from the omission attack the rule exists to catch. It is DEMOTED, not dropped.
    #[test]
    fn normalising_demotes_a_coin_spent_above_the_common_height_rather_than_dropping_it() {
        let items = [coin(1, 100, Some(105))];

        let at = normalise_at(&items, 102);

        assert_eq!(at.len(), 1, "the coin was still owned at 102");
        assert_eq!(at[0].spent_height(), None, "and it was unspent at 102");
    }

    /// A spend at or below `H` is a fact about the chain at `H` and survives normalisation.
    ///
    /// Without this, an implementation that demoted EVERY spend would pass the test above while
    /// erasing every settled spend in the set.
    #[test]
    fn normalising_keeps_a_spend_that_had_already_happened_at_the_common_height() {
        let items = [coin(1, 100, Some(101))];

        let at = normalise_at(&items, 102);

        assert_eq!(at[0].spent_height(), Some(101));
    }

    /// **The honest-lag case, which any workable rule must not call a disagreement.**
    ///
    /// Two peers one block apart: the faster one has seen a coin created at `P+1` and a spend at
    /// `P+1` the slower one has not. Normalised to a common height below both, their sets are
    /// identical — so ordinary lag stops being a source of disagreement at all.
    #[test]
    fn two_honest_peers_one_block_apart_agree_once_normalised() {
        let slow = [coin(1, 100, None), coin(2, 100, None)];
        let fast = [
            coin(1, 100, None),
            coin(2, 100, Some(105)),
            coin(3, 105, None),
        ];

        let h = common_height(&[104, 105], None).expect("both sources answered");
        let slow = fingerprint(&normalise_at(&slow, h));
        let fast = fingerprint(&normalise_at(&fast, h));

        assert_eq!(
            contradiction("slow", &slow, "fast", &fast),
            None,
            "a one-block lead is not a contradiction"
        );
    }

    /// **The attack, stated as a test.** A source that hides a coin is caught, not averaged away.
    #[test]
    fn a_source_that_omits_a_coin_contradicts_one_that_reports_it() {
        let honest = fingerprint(&normalise_at(&[coin(1, 10, None), coin(2, 10, None)], 100));
        let hiding = fingerprint(&normalise_at(&[coin(1, 10, None)], 100));

        let detail = contradiction("honest", &honest, "hiding", &hiding)
            .expect("an omission is a contradiction");
        assert!(
            detail.contains(&coin(2, 10, None).identity()),
            "the disagreement must name the coin: {detail}"
        );
    }

    /// The mirror: a source that ADDS a coin is caught too. Addition is the expensive direction
    /// for an attacker, but the rule is equality, so it is refused just the same.
    #[test]
    fn a_source_that_adds_a_coin_contradicts_one_that_does_not_have_it() {
        let honest = fingerprint(&normalise_at(&[coin(1, 10, None)], 100));
        let adding = fingerprint(&normalise_at(&[coin(1, 10, None), coin(9, 10, None)], 100));

        assert!(contradiction("honest", &honest, "adding", &adding).is_some());
    }

    /// The same coin with a different chain claim is a contradiction about THAT coin, and the
    /// error says so — the reason the fingerprint is keyed on identity rather than on the claim.
    #[test]
    fn the_same_coin_with_a_different_height_is_a_contradiction_naming_that_coin() {
        let a = fingerprint(&normalise_at(&[coin(1, 10, None)], 100));
        let b = fingerprint(&normalise_at(&[coin(1, 11, None)], 100));

        let detail = contradiction("a", &a, "b", &b)
            .expect("a fabricated creation height is a contradiction");
        assert!(detail.contains(&coin(1, 10, None).identity()), "{detail}");
    }

    /// Equal sets in a different ORDER agree: the wire order is not a claim about the chain.
    #[test]
    fn set_comparison_ignores_the_order_the_source_returned_coins_in() {
        let forwards = fingerprint(&[coin(1, 10, None), coin(2, 10, None)]);
        let backwards = fingerprint(&[coin(2, 10, None), coin(1, 10, None)]);

        assert_eq!(contradiction("f", &forwards, "b", &backwards), None);
    }

    /// The caller's projection is applied to an ALREADY-AGREED set, and applies both halves.
    #[test]
    fn the_projection_applies_the_callers_start_height_and_spent_filter() {
        let agreed = vec![coin(1, 10, None), coin(2, 50, Some(60)), coin(3, 50, None)];

        let unspent_only = project(
            agreed.clone(),
            SetProjection {
                start_height: None,
                end_height: None,
                include_spent: false,
            },
        );
        assert_eq!(unspent_only.len(), 2, "the spent coin is projected out");

        let recent = project(
            agreed,
            SetProjection {
                start_height: Some(20),
                end_height: None,
                include_spent: true,
            },
        );
        assert_eq!(recent.len(), 2, "the coin created at 10 is below the start");
    }
}
