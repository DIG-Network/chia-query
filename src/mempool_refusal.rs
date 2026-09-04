//! Which mempool refusals are properties of the BUNDLE, and which are properties of one node.
//!
//! A full node that will not admit a spend bundle names a reason. Some of those reasons are facts
//! about the bytes — every honest node reaches the same verdict from the same bundle — and the rest
//! are facts about the node that was asked: its mempool, its fee floor, how far behind the tip it
//! is. Telling the two apart is what lets a caller decide whether a refusal is worth a second
//! opinion, and it is the classification [`QueryRouter::push_tx`](crate::QueryRouter::push_tx)
//! keys its one retry on (chia-query#50).
//!
//! # This list has ONE home, and this is it
//!
//! It was written in `dig-wallet` (`sage::chain`, dig-node#460) and is moved DOWN here unchanged so
//! that the two crates cannot drift into rival definitions of the same split (CLAUDE.md §2.0,
//! centralize rivals). Two lists with different membership would not merely duplicate work: they
//! would disagree about whether a given refusal is final, and one of them would be wrong on the
//! money path. `dig-wallet` adopts these by re-export.
//!
//! # The two crates apply the SAME list with OPPOSITE defaults, deliberately
//!
//! `dig-wallet` asks "may I release this bundle's reserved inputs?" and chia-query asks "should I
//! try one more peer?". The safe side of *free the inputs* is to HOLD, and the safe side of *send
//! again once* is to RETRY, so an unrecognised reason falls to hold there and to retry here. That
//! is not an inconsistency to be tidied away by a later reader: unifying the defaults would make one
//! of the two callers fail in its dangerous direction.

use crate::types::TxStatus;

/// Refusal reasons that are properties of the BUNDLE rather than of one node's view.
///
/// # This is an ALLOWLIST, and that is the whole design (dig-node#460)
///
/// The reason text is supplied by an untrusted source (§13 / NC-12), so the guard cannot trust it
/// to make a POSITIVE safety claim. It does not have to. Freeing early is the dangerous direction
/// and holding is the safe one, so the default is HOLD and this list is the only exception to it.
/// Three consequences follow, and each is a property the code would lose as a denylist:
///
/// - **An incomplete list is safe.** A Chia error name added after this was written, a source with
///   its own vocabulary, a peer inventing text — all land in the hold class and cost at most one
///   bounded `RESERVATION_TTL_MS`. The same names written as "free unless one of these" would free
///   on every string nobody foresaw.
/// - **The free set strictly SHRANK**, which removes the ACCIDENTAL free: a source that denies a
///   relay it performed, or answers with its own conflict, no longer frees. It does NOT raise the bar
///   against a DELIBERATE attacker in the answering position — these names are public constants, so
///   emitting one is a lookup rather than a feat. This guard fixes the honest-race defect; it is not
///   a defence against a hostile last destination, and must not be described as one.
/// - **The other direction is unchanged.** A source wanting the inputs HELD could already achieve
///   that by stating no reason at all, which dig-node#348 made a hold. This adds no new lockout
///   capability, and the TTL that bounds it MUST NOT be shortened to compensate.
///
/// # What is deliberately absent
///
/// Everything whose answer depends on WHO was asked. `DOUBLE_SPEND`, `MEMPOOL_CONFLICT` and
/// `ALREADY_INCLUDING_TRANSACTION` are a node's report of its OWN mempool, and on the multi-
/// destination push path they are what a peer says when it has already seen the bundle another
/// destination admitted — the exact refusal that must never free. `UNKNOWN_UNSPENT` is a node that
/// has not caught up. The fee names (`INVALID_FEE_LOW_FEE`, `INVALID_FEE_TOO_CLOSE_TO_ZERO`) are
/// per-node relay POLICY. The timelock assertions (`ASSERT_HEIGHT_*`, `ASSERT_SECONDS_*`,
/// `ASSERT_BEFORE_*`) are evaluated against the asked node's PEAK, so a node behind the tip refuses
/// what a node at the tip admits.
///
/// `TOO_MANY_ANNOUNCEMENTS` is the subtle one and the reason this paragraph names it explicitly. It
/// reads as a pure property of the bundle — a bundle either carries too many announcements or it does
/// not — and it is NOT: `chia_consensus::conditions` decrements the per-spend announcement countdown
/// only `if (flags & COST_CONDITIONS) == 0`, and `COST_CONDITIONS` is derived from the answering
/// node's height. So a node below `hard_fork2_height` refuses an announcement-heavy bundle that a node
/// above it admits. It is absent, so it holds, and it is written down HERE because it is the entry a
/// future reader is most likely to add believing it intrinsic.
///
/// **The CLVM-EXECUTION names are absent for the SAME reason, which is not obvious and was got
/// wrong once.** `GENERATOR_RUNTIME_ERROR`, `BLOCK_COST_EXCEEDS_MAX`, `INVALID_BLOCK_COST` and
/// `INVALID_SPEND_BUNDLE` look like pure properties of the bytes and are not. Bundle validation is
/// parameterised by the answering node's HEIGHT and by a caller-supplied cost budget:
/// `chia_consensus::spendbundle_validation::get_flags_for_height_and_constants` derives
/// `COST_CONDITIONS` / `ENABLE_KECCAK_OPS_OUTSIDE_GUARD` / `SIMPLE_GENERATOR` from `prev_tx_height`,
/// and `run_spendbundle(.., max_cost, flags, ..)` runs under both. So a node above a hard fork and a
/// node below it can reach DIFFERENT verdicts on identical bytes — the same property that excludes
/// the timelocks. Do not re-add them.
///
/// The list is also kept SHORT on purpose: a name is added only when every node is certain to
/// refuse it identically. The announcement-consumption names are omitted for that reason, not
/// because they are believed view-dependent. Omission costs a bounded hold; a wrong inclusion costs
/// a double-select window.
///
/// **The one acknowledged residue.** `BAD_AGGREGATE_SIGNATURE` is verified against messages built
/// with the node's own `AGG_SIG_ME_ADDITIONAL_DATA`, so it is a property of the bundle only for
/// nodes on the same network. The peer handshake's `network_id` check is what makes that hold in
/// practice; it is stated rather than left implicit, because it is the assumption this entry rests
/// on.
pub const BUNDLE_INTRINSIC_REFUSALS: &[&str] = &[
    "BAD_AGGREGATE_SIGNATURE",
    "COIN_AMOUNT_NEGATIVE",
    "COIN_AMOUNT_EXCEEDS_MAXIMUM",
    "DUPLICATE_OUTPUT",
    "MINTING_COIN",
    "RESERVE_FEE_CONDITION_FAILED",
    "WRONG_PUZZLE_HASH",
    "ASSERT_MY_COIN_ID_FAILED",
    "ASSERT_MY_PARENT_ID_FAILED",
    "ASSERT_MY_PUZZLEHASH_FAILED",
    "ASSERT_MY_AMOUNT_FAILED",
];

/// Whether a stated refusal reason is a property of the BUNDLE rather than of one node's view.
///
/// The match is EXACT against [`BUNDLE_INTRINSIC_REFUSALS`], case-insensitively and after trimming
/// — never a substring or prefix test. A source that embeds an allowlisted name in wider text
/// (`"MEMPOOL_CONFLICT (see BAD_AGGREGATE_SIGNATURE)"`) does not match, and lands in the
/// view-dependent class, which is the direction an unparseable answer belongs in.
///
/// `reason` is the BARE reason. A caller whose transport composes it with a verdict
/// (`"{verdict}: {reason}"`, as `dig-wallet`'s `ChainTransport` does) splits it first; that split
/// belongs to whoever built the composition, and re-deriving it here would let the two drift apart
/// in silence.
pub fn is_bundle_intrinsic_refusal(reason: &str) -> bool {
    let reason = reason.trim();
    BUNDLE_INTRINSIC_REFUSALS
        .iter()
        .any(|intrinsic| reason.eq_ignore_ascii_case(intrinsic))
}

/// Whether a push verdict is FINAL — nothing another peer could say would change it.
///
/// Two ways to be final, for two different reasons:
///
/// - **Admitted.** The bundle is in a mempool; there is nothing left to retry.
/// - **Refused for a bundle-intrinsic reason.** Every honest node reaches the same verdict from the
///   same bytes, so a second transmission buys a round trip and the identical answer.
///
/// Everything else — `NotAdmitted`, `Failed` or `Unknown` carrying a view-dependent name, an
/// unrecognised name, an empty reason, or no reason at all — is NOT final, because the refusal may
/// be a fact about the peer rather than about the spend. A caller that treats those as final lets
/// one lagging, fee-strict or hostile peer veto a transaction every other peer would admit, which
/// under NC-12 is exactly the authority a single dialled peer must not have.
pub fn is_final(status: &TxStatus) -> bool {
    status.inclusion.is_admitted()
        || status
            .error
            .as_deref()
            .is_some_and(is_bundle_intrinsic_refusal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MempoolInclusion;

    fn refused(inclusion: MempoolInclusion, error: Option<&str>) -> TxStatus {
        TxStatus {
            status: "PENDING".into(),
            success: inclusion.is_admitted(),
            inclusion,
            error: error.map(str::to_string),
        }
    }

    /// Every allowlisted name is intrinsic, however the peer cased or padded it.
    ///
    /// A node's own vocabulary is upper-case, but the reason travels as free text through more than
    /// one transport, so an exact byte comparison would silently reclassify `"minting_coin"` as
    /// view-dependent and retry a bundle no node will ever accept.
    #[test]
    fn every_allowlisted_name_is_intrinsic_however_it_is_cased_or_padded() {
        for name in BUNDLE_INTRINSIC_REFUSALS {
            assert!(is_bundle_intrinsic_refusal(name), "{name}");
            assert!(is_bundle_intrinsic_refusal(&name.to_lowercase()), "{name}");
            assert!(
                is_bundle_intrinsic_refusal(&format!("  {name}  ")),
                "{name}"
            );
        }
    }

    /// **The match is EXACT, so an allowlisted name embedded in wider text does not count.**
    ///
    /// This is the assertion that stops a `contains` implementation passing. A peer that wants its
    /// refusal to stick can already emit a bare allowlisted name — that costs it nothing and is
    /// unchanged. What a substring test would additionally hand it is the ability to make ANY
    /// refusal final by mentioning an intrinsic name inside it, including one whose leading words
    /// say the opposite.
    #[test]
    fn an_allowlisted_name_inside_wider_text_is_not_a_match() {
        for wider in [
            "MEMPOOL_CONFLICT (see BAD_AGGREGATE_SIGNATURE)",
            "BAD_AGGREGATE_SIGNATURE_EXTRA",
            "not BAD_AGGREGATE_SIGNATURE",
            "DOUBLE_SPEND",
        ] {
            assert!(
                !is_bundle_intrinsic_refusal(wider),
                "{wider} must not match an allowlist entry"
            );
        }
    }

    /// The view-dependent names are ABSENT, and their absence is the classification.
    ///
    /// Named individually rather than counted, because the failure this guards is a future reader
    /// ADDING one — and a length assertion would pass a swap.
    #[test]
    fn the_view_dependent_names_are_not_intrinsic() {
        for view_dependent in [
            "MEMPOOL_CONFLICT",
            "DOUBLE_SPEND",
            "ALREADY_INCLUDING_TRANSACTION",
            "UNKNOWN_UNSPENT",
            "INVALID_FEE_LOW_FEE",
            "INVALID_FEE_TOO_CLOSE_TO_ZERO",
            "ASSERT_HEIGHT_ABSOLUTE_FAILED",
            "ASSERT_HEIGHT_RELATIVE_FAILED",
            "ASSERT_SECONDS_ABSOLUTE_FAILED",
            "ASSERT_BEFORE_HEIGHT_ABSOLUTE_FAILED",
            "TOO_MANY_ANNOUNCEMENTS",
            "GENERATOR_RUNTIME_ERROR",
            "BLOCK_COST_EXCEEDS_MAX",
            "INVALID_BLOCK_COST",
            "INVALID_SPEND_BUNDLE",
        ] {
            assert!(
                !is_bundle_intrinsic_refusal(view_dependent),
                "{view_dependent} depends on which node was asked and must NOT be final"
            );
        }
    }

    /// An admission is final because there is nothing left to retry.
    #[test]
    fn an_admission_is_final() {
        assert!(is_final(&refused(MempoolInclusion::Admitted, None)));
        assert!(is_final(&refused(
            MempoolInclusion::Admitted,
            Some("MEMPOOL_CONFLICT")
        )));
    }

    /// **A refusal with no reason at all is NOT final.**
    ///
    /// The cheapest thing a peer can do is say nothing, so reading silence as a definitive verdict
    /// would make the veto free. `None` and `Some("")` are both silence and both retry.
    #[test]
    fn a_refusal_that_states_no_reason_is_not_final() {
        for error in [None, Some(""), Some("   ")] {
            assert!(
                !is_final(&refused(MempoolInclusion::NotAdmitted, error)),
                "{error:?} states nothing about the bundle and must not be final"
            );
            assert!(!is_final(&refused(MempoolInclusion::Unknown, error)));
        }
    }

    /// The two refusal classes, side by side on the same inclusion state.
    ///
    /// A single-case fixture cannot tell a working classifier from one that answers a constant.
    #[test]
    fn an_intrinsic_refusal_is_final_and_a_view_dependent_one_is_not() {
        assert!(is_final(&refused(
            MempoolInclusion::Failed,
            Some("BAD_AGGREGATE_SIGNATURE")
        )));
        assert!(!is_final(&refused(
            MempoolInclusion::Failed,
            Some("INVALID_FEE_TOO_CLOSE_TO_ZERO")
        )));
    }
}
