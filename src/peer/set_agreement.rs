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
    unimplemented!("common_height")
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
    unimplemented!("normalise_at")
}

/// The comparable fingerprint of a normalised set: identity -> chain claim.
///
/// A `BTreeMap` rather than a set of claims so a disagreement can NAME the coin. Two sources that
/// each return the same coin id with a different `created_height` produce one key with two values,
/// which reads as "these sources disagree about coin X" instead of "one extra, one missing".
pub fn fingerprint<T: SetMember>(items: &[T]) -> BTreeMap<String, String> {
    unimplemented!("fingerprint")
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
    unimplemented!("contradiction")
}

/// Apply what the CALLER asked for to an already-agreed set.
///
/// Runs after agreement, never before it, and never on one side of a comparison alone.
pub fn project<T: SetMember>(items: Vec<T>, projection: SetProjection) -> Vec<T> {
    unimplemented!("project")
}
