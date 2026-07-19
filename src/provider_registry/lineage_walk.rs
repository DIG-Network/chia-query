//! The forward singleton-lineage walk that authenticates a singleton from its launcher to its
//! current unspent tip.
//!
//! [`walk_singleton_lineage`] follows the singleton hop-by-hop — each coin the singleton recreation
//! of the previous — collecting every member coin id into a
//! [`SingletonLineage`](dig_chainsource_interface::SingletonLineage). It is a GENUINE forward walk
//! launcher -> tip, never an echo of a caller-supplied coin, so lineage MEMBERSHIP is meaningful
//! (SPEC §4, the money-critical requirement).
//!
//! Each hop is proven from the parent's actual spend: [`singleton_child_from_spend`] verifies the
//! spent coin's puzzle reveal hashes to its committed puzzle hash, then extracts the singleton's
//! odd-amount recreation output. This per-hop CLVM check is NECESSARY but NOT SUFFICIENT for
//! custody — total fabrication is defeated by the registry trust model layered above, not here.
//!
//! The walk is `async` and generic over a spend-fetcher so it composes directly with the async
//! [`QueryRouter`](crate::router::QueryRouter) inside a single blocking bridge (avoiding the nested
//! `block_on` a synchronous `ChainSource`-driven walk would hit), while the child-extractor stays
//! injectable for isolated testing of the control flow.

use std::collections::BTreeSet;
use std::future::Future;

use chia_protocol::{Bytes32, Coin, CoinSpend};
use clvmr::{Allocator, NodePtr, SExp};
use dig_chainsource_interface::{ChainSourceError, SingletonLineage};

/// The CLVM opcode for a `CREATE_COIN` condition.
const CREATE_COIN: u8 = 51;

/// A generous CLVM evaluation budget for running a single puzzle+solution — the standard maximum
/// block cost, comfortably above any one singleton spend.
const MAX_CLVM_COST: u64 = 11_000_000_000;

/// Walks the singleton launched at `launcher_id` forward to its current unspent tip.
///
/// - `fetch_spend(coin_id)` reads the spend that SPENT `coin_id` (`Ok(None)` = unspent/unknown).
/// - `extract_child(spend)` derives the singleton recreation child from a spend (`Ok(None)` = the
///   spend recreates no singleton, i.e. a melt). Real callers pass [`singleton_child_from_spend`];
///   tests inject a table-driven extractor to exercise the control flow in isolation.
///
/// Returns `Ok(Some(lineage))` for a launched singleton at a live tip, `Ok(None)` when the launcher
/// was never spent or the singleton was fully melted, and `Err(_)` when a read failed (the caller
/// must fail closed and never treat the error as absence).
pub(crate) async fn walk_singleton_lineage<F, Fut, X>(
    launcher_id: Bytes32,
    fetch_spend: F,
    extract_child: X,
) -> Result<Option<SingletonLineage>, ChainSourceError>
where
    F: Fn(Bytes32) -> Fut,
    Fut: Future<Output = Result<Option<CoinSpend>, ChainSourceError>>,
    X: Fn(&CoinSpend) -> Result<Option<Bytes32>, ChainSourceError>,
{
    // The launcher must have been spent to launch the singleton (its spend creates the eve coin).
    let Some(launch_spend) = fetch_spend(launcher_id).await? else {
        return Ok(None); // launcher never spent => never launched
    };
    let Some(mut current) = extract_child(&launch_spend)? else {
        return Ok(None); // the launcher spend created no singleton child => not a singleton launch
    };

    let mut members: BTreeSet<Bytes32> = BTreeSet::new();
    loop {
        // A coin appearing twice would mean a malformed/cyclic lineage — fail closed.
        if !members.insert(current) {
            return Err(ChainSourceError::Malformed(
                "singleton lineage revisited a coin (cycle)".to_string(),
            ));
        }

        match fetch_spend(current).await? {
            // `current` is unspent — it is the live tip.
            None => return Ok(Some(SingletonLineage::new(current, members))),
            Some(spend) => match extract_child(&spend)? {
                Some(child) => current = child,
                // Spent with no singleton recreation output — the singleton was melted; there is
                // no live tip, so the lineage is genuinely gone.
                None => return Ok(None),
            },
        }
    }
}

/// Derives a singleton's recreation child from the spend that spent it, or `Ok(None)` when the
/// spend creates no odd-amount coin (a melt / not a singleton recreation).
///
/// Fails closed with [`ChainSourceError::Malformed`] when the puzzle reveal does not hash to the
/// spent coin's committed puzzle hash (an unauthenticated reveal) or the CLVM cannot be parsed/run.
/// The odd-amount `CREATE_COIN` output is the singleton continuation (singleton amounts are odd by
/// construction); its coin id is the next lineage member.
pub(crate) fn singleton_child_from_spend(
    spend: &CoinSpend,
) -> Result<Option<Bytes32>, ChainSourceError> {
    let mut allocator = Allocator::new();

    let puzzle =
        clvmr::serde::node_from_bytes_backrefs(&mut allocator, spend.puzzle_reveal.as_ref())
            .map_err(|e| ChainSourceError::Malformed(format!("undecodable puzzle reveal: {e}")))?;

    // Authenticate the reveal against the coin it claims to spend.
    let reveal_hash: [u8; 32] = chia::clvm_utils::tree_hash(&allocator, puzzle).into();
    if Bytes32::new(reveal_hash) != spend.coin.puzzle_hash {
        return Err(ChainSourceError::Malformed(
            "puzzle reveal does not hash to the spent coin's puzzle hash".to_string(),
        ));
    }

    let solution = clvmr::serde::node_from_bytes_backrefs(&mut allocator, spend.solution.as_ref())
        .map_err(|e| ChainSourceError::Malformed(format!("undecodable solution: {e}")))?;

    let dialect = clvmr::ChiaDialect::new(0);
    let output = clvmr::run_program(&mut allocator, &dialect, puzzle, solution, MAX_CLVM_COST)
        .map_err(|e| ChainSourceError::Malformed(format!("puzzle evaluation failed: {e:?}")))?
        .1;

    let parent_id = spend.coin.coin_id();
    for condition in list_iter(&allocator, output) {
        if let Some(child) = create_coin_child(&allocator, condition, parent_id) {
            return Ok(Some(child));
        }
    }
    Ok(None)
}

/// If `condition` is an odd-amount `CREATE_COIN`, returns the created coin's id (the singleton
/// recreation child); otherwise `None`.
fn create_coin_child(a: &Allocator, condition: NodePtr, parent_id: Bytes32) -> Option<Bytes32> {
    let mut args = list_iter(a, condition);
    let opcode = args.next()?;
    if atom_bytes(a, opcode)? != [CREATE_COIN] {
        return None;
    }
    let puzzle_hash_node = args.next()?;
    let amount_node = args.next()?;

    let puzzle_hash: [u8; 32] = atom_bytes(a, puzzle_hash_node)?.try_into().ok()?;
    let amount = atom_to_u64(&atom_bytes(a, amount_node)?);

    // Only the odd-amount output continues the singleton.
    if amount.is_multiple_of(2) {
        return None;
    }
    Some(Coin::new(parent_id, Bytes32::new(puzzle_hash), amount).coin_id())
}

/// The atom bytes of `node`, or `None` when it is a pair rather than an atom.
fn atom_bytes(a: &Allocator, node: NodePtr) -> Option<Vec<u8>> {
    match a.sexp(node) {
        SExp::Atom => Some(a.atom(node).as_ref().to_vec()),
        SExp::Pair(..) => None,
    }
}

/// Decodes a minimal big-endian CLVM atom into a `u64` (values above `u64` saturate, which no valid
/// coin amount reaches).
fn atom_to_u64(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |acc, &b| acc.wrapping_shl(8).wrapping_add(b as u64))
}

/// Iterates the elements of a proper CLVM list, stopping at the nil terminator.
fn list_iter(a: &Allocator, node: NodePtr) -> impl Iterator<Item = NodePtr> + '_ {
    let mut cursor = node;
    std::iter::from_fn(move || match a.sexp(cursor) {
        SExp::Pair(first, rest) => {
            cursor = rest;
            Some(first)
        }
        SExp::Atom => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use chia_protocol::{Coin, Program};

    fn coin(parent: Bytes32, ph: u8) -> Coin {
        Coin::new(parent, Bytes32::new([ph; 32]), 1)
    }

    /// A dummy spend whose *coin* is `coin`; the child linkage is resolved by the injected
    /// extractor keyed on the coin id, so the CLVM body is irrelevant here.
    fn spend_of(coin: Coin) -> CoinSpend {
        CoinSpend::new(coin, Program::from(vec![0x80]), Program::from(vec![0x80]))
    }

    /// A spend fetcher over a fixed `coin_id -> spend` table.
    fn fetcher(
        spends: HashMap<Bytes32, CoinSpend>,
    ) -> impl Fn(Bytes32) -> std::future::Ready<Result<Option<CoinSpend>, ChainSourceError>> {
        move |id| std::future::ready(Ok(spends.get(&id).cloned()))
    }

    /// A child extractor over a fixed `spent-coin-id -> child-coin-id` table.
    fn table_extractor(
        children: HashMap<Bytes32, Bytes32>,
    ) -> impl Fn(&CoinSpend) -> Result<Option<Bytes32>, ChainSourceError> {
        move |spend: &CoinSpend| Ok(children.get(&spend.coin.coin_id()).copied())
    }

    /// Builds a genuine launcher -> eve -> c1 -> tip lineage: the spend table (launcher, eve, c1 are
    /// spent; tip is unspent), the ordered member ids (eve, c1, tip), and the child table.
    #[allow(clippy::type_complexity)]
    fn genuine_lineage() -> (
        HashMap<Bytes32, CoinSpend>,
        Bytes32,
        Vec<Bytes32>,
        HashMap<Bytes32, Bytes32>,
    ) {
        let launcher = Coin::new(Bytes32::new([0x01; 32]), Bytes32::new([0x02; 32]), 1);
        let launcher_id = launcher.coin_id();
        let eve = coin(launcher_id, 0x10);
        let c1 = coin(eve.coin_id(), 0x11);
        let tip = coin(c1.coin_id(), 0x12);

        let mut spends = HashMap::new();
        spends.insert(launcher_id, spend_of(launcher));
        spends.insert(eve.coin_id(), spend_of(eve));
        spends.insert(c1.coin_id(), spend_of(c1));
        // tip is unspent -> no spend entry.

        let mut children = HashMap::new();
        children.insert(launcher_id, eve.coin_id());
        children.insert(eve.coin_id(), c1.coin_id());
        children.insert(c1.coin_id(), tip.coin_id());

        let members = vec![eve.coin_id(), c1.coin_id(), tip.coin_id()];
        (spends, launcher_id, members, children)
    }

    #[tokio::test]
    async fn walks_launcher_to_tip_and_collects_every_member() {
        let (spends, launcher_id, members, children) = genuine_lineage();
        let lineage =
            walk_singleton_lineage(launcher_id, fetcher(spends), table_extractor(children))
                .await
                .unwrap()
                .expect("a launched singleton");

        assert_eq!(lineage.tip(), *members.last().unwrap());
        for member in &members {
            assert!(
                lineage.contains(*member),
                "genuine member must be in lineage"
            );
        }
        assert_eq!(lineage.len(), members.len());
    }

    /// Test #6 — a fabricated / echoed / forked coin is NOT a member of the genuine lineage. The
    /// authority check is `contains` membership, never tip/puzzle-hash equality.
    #[tokio::test]
    async fn fabricated_coin_is_not_a_lineage_member() {
        let (spends, launcher_id, members, children) = genuine_lineage();
        let lineage =
            walk_singleton_lineage(launcher_id, fetcher(spends), table_extractor(children))
                .await
                .unwrap()
                .unwrap();

        // An attacker's look-alike coin (never a genuine recreation) is not a member.
        let fabricated = coin(Bytes32::new([0xEE; 32]), 0x12).coin_id();
        assert!(!lineage.contains(fabricated));

        // The launcher itself is not a lineage member (members start at the eve coin).
        assert!(!lineage.contains(launcher_id));

        // A structurally-consistent FORK: the tip's puzzle hash but a different parent -> a
        // different coin id -> still not a member (shape equality is not membership).
        let real_tip = *members.last().unwrap();
        let forked_tip = Coin::new(Bytes32::new([0x99; 32]), Bytes32::new([0x12; 32]), 1).coin_id();
        assert_ne!(forked_tip, real_tip);
        assert!(!lineage.contains(forked_tip));
    }

    #[tokio::test]
    async fn unlaunched_launcher_returns_none() {
        let result = walk_singleton_lineage(
            Bytes32::new([0x07; 32]),
            fetcher(HashMap::new()),
            table_extractor(HashMap::new()),
        )
        .await
        .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn melted_singleton_returns_none() {
        // launcher -> eve, then eve spent recreating nothing (melt).
        let launcher = Coin::new(Bytes32::new([0x01; 32]), Bytes32::new([0x02; 32]), 1);
        let launcher_id = launcher.coin_id();
        let eve = coin(launcher_id, 0x10);

        let mut spends = HashMap::new();
        spends.insert(launcher_id, spend_of(launcher));
        spends.insert(eve.coin_id(), spend_of(eve));

        let mut children = HashMap::new();
        children.insert(launcher_id, eve.coin_id());
        // eve spent, no child.

        let result =
            walk_singleton_lineage(launcher_id, fetcher(spends), table_extractor(children))
                .await
                .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn source_error_fails_closed_not_none() {
        let fetch = |_id: Bytes32| std::future::ready(Err(ChainSourceError::Timeout));
        let result = walk_singleton_lineage(
            Bytes32::new([0x07; 32]),
            fetch,
            table_extractor(HashMap::new()),
        )
        .await;
        assert_eq!(result, Err(ChainSourceError::Timeout));
    }

    /// Builds a spend whose puzzle is `(q . ((51 puzzle_hash amount)))` — a quoted single
    /// `CREATE_COIN` — with the spent coin's puzzle hash set to the reveal's tree hash so the
    /// authentication check passes. Returns the spend and the parent coin id.
    fn create_coin_spend(child_ph: [u8; 32], amount: u8) -> (CoinSpend, Bytes32) {
        let mut a = Allocator::new();
        let op = a.new_atom(&[CREATE_COIN]).unwrap();
        let ph_atom = a.new_atom(&child_ph).unwrap();
        let amt_atom = a.new_atom(&[amount]).unwrap();
        let nil = a.nil();
        let arg2 = a.new_pair(amt_atom, nil).unwrap();
        let arg1 = a.new_pair(ph_atom, arg2).unwrap();
        let condition = a.new_pair(op, arg1).unwrap();
        let conditions = a.new_pair(condition, nil).unwrap();
        let quote = a.new_atom(&[1]).unwrap();
        let puzzle = a.new_pair(quote, conditions).unwrap();

        let puzzle_bytes = clvmr::serde::node_to_bytes(&a, puzzle).unwrap();
        let solution_bytes = clvmr::serde::node_to_bytes(&a, nil).unwrap();
        let puzzle_hash: [u8; 32] = chia::clvm_utils::tree_hash(&a, puzzle).into();

        let coin = Coin::new(Bytes32::new([0xAB; 32]), Bytes32::new(puzzle_hash), 1);
        let parent_id = coin.coin_id();
        let spend = CoinSpend::new(
            coin,
            Program::from(puzzle_bytes),
            Program::from(solution_bytes),
        );
        (spend, parent_id)
    }

    #[test]
    fn extractor_returns_odd_create_coin_child() {
        let child_ph = [0x55u8; 32];
        let (spend, parent_id) = create_coin_spend(child_ph, 3); // odd
        let child = singleton_child_from_spend(&spend).unwrap();
        let expected = Coin::new(parent_id, Bytes32::new(child_ph), 3).coin_id();
        assert_eq!(child, Some(expected));
    }

    #[test]
    fn extractor_ignores_even_amount_create_coin() {
        // An even-amount CREATE_COIN is not a singleton recreation -> no child (a melt).
        let (spend, _) = create_coin_spend([0x55u8; 32], 2); // even
        assert_eq!(singleton_child_from_spend(&spend).unwrap(), None);
    }

    #[test]
    fn extractor_rejects_unauthenticated_reveal() {
        // Tamper with the committed puzzle hash so the reveal no longer authenticates -> Malformed.
        let (mut spend, _) = create_coin_spend([0x55u8; 32], 3);
        spend.coin = Coin::new(
            spend.coin.parent_coin_info,
            Bytes32::new([0x00; 32]), // wrong puzzle hash
            spend.coin.amount,
        );
        assert!(matches!(
            singleton_child_from_spend(&spend),
            Err(ChainSourceError::Malformed(_))
        ));
    }

    #[test]
    fn extractor_rejects_undecodable_reveal() {
        let coin = Coin::new(Bytes32::new([0x01; 32]), Bytes32::new([0x02; 32]), 1);
        // 0xff is not a valid serialized CLVM program.
        let spend = CoinSpend::new(coin, Program::from(vec![0xff]), Program::from(vec![0x80]));
        assert!(matches!(
            singleton_child_from_spend(&spend),
            Err(ChainSourceError::Malformed(_))
        ));
    }

    #[tokio::test]
    async fn cyclic_lineage_fails_closed() {
        // a -> b -> a : the back-edge must be rejected, not looped forever.
        let a_coin = Coin::new(Bytes32::new([0x01; 32]), Bytes32::new([0x02; 32]), 1);
        let launcher_id = a_coin.coin_id();
        let b = coin(launcher_id, 0x10);

        let mut spends = HashMap::new();
        spends.insert(launcher_id, spend_of(a_coin));
        spends.insert(b.coin_id(), spend_of(b));

        let mut children = HashMap::new();
        children.insert(launcher_id, b.coin_id());
        children.insert(b.coin_id(), launcher_id); // back-edge

        let result =
            walk_singleton_lineage(launcher_id, fetcher(spends), table_extractor(children)).await;
        assert!(matches!(result, Err(ChainSourceError::Malformed(_))));
    }
}
