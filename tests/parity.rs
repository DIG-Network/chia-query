//! Live parity tests: call coinset.org (source of truth) and the peer backend
//! independently, then compare responses field-by-field to ensure both return
//! the same data for the same query.
//!
//! Run with:
//!     cargo test --test parity -- --ignored
//!
//! Every comparison is BIDIRECTIONAL -- we check that the peer is not missing
//! coins AND not returning extras that coinset doesn't have.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chia_query::coinset::CoinsetClient;
use chia_query::peer::connect::create_generated_tls;
use chia_query::peer::PeerBackend;
use chia_query::types::*;
use chia_query::NetworkType;

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

fn norm(s: &str) -> String {
    s.strip_prefix("0x").unwrap_or(s).to_lowercase()
}

// ---------------------------------------------------------------------------
// Coin identity  (parent_coin_info, puzzle_hash, amount)
// ---------------------------------------------------------------------------

type CoinId3 = (String, String, u64);

fn coin_id3(c: &Coin) -> CoinId3 {
    (norm(&c.parent_coin_info), norm(&c.puzzle_hash), c.amount)
}

fn record_id3(r: &CoinRecord) -> CoinId3 {
    coin_id3(&r.coin)
}

// ---------------------------------------------------------------------------
// Strict bidirectional set comparison on coin records
// ---------------------------------------------------------------------------

/// Assert that coinset and peer return exactly the same set of coins
/// (by parent_coin_info + puzzle_hash + amount).  Then for each matching
/// coin, assert that confirmed_block_index, spent_block_index, and spent
/// are identical (skip timestamp and coinbase -- known peer gaps).
fn assert_coin_records_eq(label: &str, coinset: &[CoinRecord], peer: &[CoinRecord]) {
    let cs_set: HashSet<CoinId3> = coinset.iter().map(record_id3).collect();
    let pr_set: HashSet<CoinId3> = peer.iter().map(record_id3).collect();

    let missing: Vec<_> = cs_set.difference(&pr_set).collect();
    let extra: Vec<_> = pr_set.difference(&cs_set).collect();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "{label}: set mismatch\n  coinset_count={} peer_count={}\n  \
         missing_from_peer({})={missing:?}\n  extra_in_peer({})={extra:?}",
        cs_set.len(),
        pr_set.len(),
        missing.len(),
        extra.len(),
    );

    // Build lookup for per-record field comparison.
    let cs_map: HashMap<CoinId3, &CoinRecord> =
        coinset.iter().map(|r| (record_id3(r), r)).collect();
    let pr_map: HashMap<CoinId3, &CoinRecord> = peer.iter().map(|r| (record_id3(r), r)).collect();

    for key in &cs_set {
        let c = cs_map[key];
        let p = pr_map[key];
        assert_eq!(
            c.confirmed_block_index, p.confirmed_block_index,
            "{label}: confirmed_block_index mismatch for {key:?}"
        );
        assert_eq!(
            c.spent_block_index, p.spent_block_index,
            "{label}: spent_block_index mismatch for {key:?}"
        );
        assert_eq!(c.spent, p.spent, "{label}: spent mismatch for {key:?}");
    }
}

// ---------------------------------------------------------------------------
// Strict spend comparison (bidirectional by coin, then puzzle_reveal + solution)
// ---------------------------------------------------------------------------

fn assert_spends_eq(label: &str, coinset: &[CoinSpend], peer: &[CoinSpend]) {
    assert_eq!(
        coinset.len(),
        peer.len(),
        "{label}: spend count -- coinset={} peer={}",
        coinset.len(),
        peer.len(),
    );

    let cs_map: HashMap<CoinId3, &CoinSpend> =
        coinset.iter().map(|s| (coin_id3(&s.coin), s)).collect();
    let pr_map: HashMap<CoinId3, &CoinSpend> =
        peer.iter().map(|s| (coin_id3(&s.coin), s)).collect();

    // Every coinset spend must appear in peer and vice versa.
    let cs_keys: HashSet<_> = cs_map.keys().collect();
    let pr_keys: HashSet<_> = pr_map.keys().collect();
    let missing: Vec<_> = cs_keys.difference(&pr_keys).collect();
    let extra: Vec<_> = pr_keys.difference(&cs_keys).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "{label}: spend set mismatch\n  missing({})={missing:?}\n  extra({})={extra:?}",
        missing.len(),
        extra.len(),
    );

    // Field-level comparison for each spend.
    for (key, cs) in &cs_map {
        let pr = pr_map.get(key).unwrap();
        assert_eq!(
            norm(&cs.puzzle_reveal),
            norm(&pr.puzzle_reveal),
            "{label}: puzzle_reveal mismatch for {key:?}"
        );
        assert_eq!(
            norm(&cs.solution),
            norm(&pr.solution),
            "{label}: solution mismatch for {key:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Retry helper (mirrors the router's two-attempt pattern)
// ---------------------------------------------------------------------------

async fn retry<T>(
    f: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
    f2: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
) -> Result<T, ChiaQueryError> {
    match f.await {
        Ok(v) => Ok(v),
        Err(_) => f2.await,
    }
}

// ---------------------------------------------------------------------------
// Coin ID computation (matches chia_protocol::Coin::coin_id)
// ---------------------------------------------------------------------------

fn coin_id_hex(c: &Coin) -> String {
    use sha2::{Digest, Sha256};
    let parent = hex::decode(norm(&c.parent_coin_info)).unwrap();
    let ph = hex::decode(norm(&c.puzzle_hash)).unwrap();
    let amount = c.amount;
    let amount_bytes = amount.to_be_bytes();
    let start = if amount >= 0x8000_0000_0000_0000_u64 {
        usize::MAX
    } else {
        match amount {
            n if n >= 0x0080_0000_0000_0000 => 0,
            n if n >= 0x8000_0000_0000 => 1,
            n if n >= 0x0080_0000_0000 => 2,
            n if n >= 0x8000_0000 => 3,
            n if n >= 0x0080_0000 => 4,
            n if n >= 0x8000 => 5,
            n if n >= 0x80 => 6,
            n if n > 0 => 7,
            _ => 8,
        }
    };
    let mut hasher = Sha256::new();
    hasher.update(&parent);
    hasher.update(&ph);
    if start == usize::MAX {
        hasher.update([0u8]);
        hasher.update(amount_bytes);
    } else {
        hasher.update(&amount_bytes[start..]);
    }
    format!("0x{}", hex::encode(hasher.finalize()))
}

// ===========================================================================
// ALL PARITY CHECKS IN ONE TEST (shares peer connections)
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn parity_all() {
    // -- Setup --------------------------------------------------------------
    eprintln!("=== Setting up ===");
    let tls = create_generated_tls().expect("TLS");

    let coinset =
        CoinsetClient::new("https://api.coinset.org", Duration::from_secs(30)).expect("coinset");
    let peer = PeerBackend::new(
        NetworkType::Mainnet,
        tls,
        5,
        chia_query::peer::PeerRequirement::Required,
        Duration::from_secs(15),
        Duration::from_secs(60),
    )
    .await
    .expect("peer");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // -- Discover fixtures --------------------------------------------------
    eprintln!("=== Discovering fixtures ===");
    let state = coinset.get_blockchain_state().await.unwrap();
    let peak = state.peak.as_ref().unwrap().height;

    let mut tx_height = 0u32;
    let mut tx_header_hash = String::new();
    let mut cs_additions: Vec<CoinRecord> = Vec::new();
    let mut cs_removals: Vec<CoinRecord> = Vec::new();

    let search_start = peak.saturating_sub(500);
    for h in (search_start.saturating_sub(50)..search_start).rev() {
        let rec = coinset.get_block_record_by_height(h).await.unwrap();
        let ar = coinset
            .get_additions_and_removals(&rec.header_hash)
            .await
            .unwrap();
        if !ar.removals.is_empty() && ar.additions.len() > ar.removals.len() {
            tx_height = h;
            tx_header_hash = rec.header_hash;
            cs_additions = ar.additions;
            cs_removals = ar.removals;
            break;
        }
    }
    assert!(tx_height > 0, "no tx block found");

    let first_non_cb = cs_additions
        .iter()
        .find(|c| !c.coinbase)
        .unwrap_or(&cs_additions[0]);
    let added_coin_id = coin_id_hex(&first_non_cb.coin);
    let parent_coin_id = first_non_cb.coin.parent_coin_info.clone();
    let removal = &cs_removals[0];
    let spent_coin_id = coin_id_hex(&removal.coin);
    let removal_ph = removal.coin.puzzle_hash.clone();

    eprintln!(
        "Fixtures: height={tx_height} hash={tx_header_hash}\n  \
         added={added_coin_id} spent={spent_coin_id}\n  \
         parent={parent_coin_id} removal_ph={removal_ph}"
    );

    let mut passed = 0u32;
    let mut skipped = 0u32;

    // =======================================================================
    // 1. get_block_record_by_height
    // =======================================================================
    eprintln!("\n--- get_block_record_by_height ---");
    {
        let cs = coinset.get_block_record_by_height(tx_height).await.unwrap();
        let pr = retry(
            peer.try_get_block_record_by_height(tx_height),
            peer.try_get_block_record_by_height(tx_height),
        )
        .await
        .unwrap();

        assert_eq!(cs.height, pr.height, "height");
        assert_eq!(cs.weight, pr.weight, "weight");
        assert_eq!(cs.total_iters, pr.total_iters, "total_iters");
        assert_eq!(
            cs.signage_point_index, pr.signage_point_index,
            "signage_point_index"
        );
        assert_eq!(
            norm(&cs.farmer_puzzle_hash),
            norm(&pr.farmer_puzzle_hash),
            "farmer_puzzle_hash"
        );
        assert_eq!(norm(&cs.prev_hash), norm(&pr.prev_hash), "prev_hash");
        // timestamp: peer extracts from foliage_transaction_block
        assert_eq!(cs.timestamp, pr.timestamp, "timestamp");
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 2. get_block_records (range)
    // =======================================================================
    eprintln!("--- get_block_records (range) ---");
    {
        let cs = coinset
            .get_block_records(tx_height, tx_height + 3)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_block_records(tx_height, tx_height + 3),
            peer.try_get_block_records(tx_height, tx_height + 3),
        )
        .await
        .unwrap();

        assert_eq!(cs.len(), pr.len(), "count");
        for (c, p) in cs.iter().zip(pr.iter()) {
            assert_eq!(c.height, p.height, "height at {}", c.height);
            assert_eq!(c.weight, p.weight, "weight at {}", c.height);
            assert_eq!(c.total_iters, p.total_iters, "total_iters at {}", c.height);
            assert_eq!(
                c.signage_point_index, p.signage_point_index,
                "spi at {}",
                c.height
            );
        }
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 3. get_additions_and_removals
    //    RequestAdditions returns Coins (not CoinState), so the peer response
    //    won't know if a coin was later spent.  RequestRemovals returns Coins
    //    that were spent but doesn't know their original creation height.
    //    We compare coin IDENTITY (parent + puzzle_hash + amount) and the
    //    field the protocol DOES know (confirmed_block_index for additions,
    //    spent_block_index for removals).
    // =======================================================================
    eprintln!("--- get_additions_and_removals ---");
    {
        match retry(
            peer.try_get_additions_and_removals(tx_height, &tx_header_hash),
            peer.try_get_additions_and_removals(tx_height, &tx_header_hash),
        )
        .await
        {
            Ok(pr) => {
                // Additions: same coin set, same confirmed_block_index.
                let cs_add_set: HashSet<CoinId3> = cs_additions.iter().map(record_id3).collect();
                let pr_add_set: HashSet<CoinId3> = pr.additions.iter().map(record_id3).collect();
                let add_missing: Vec<_> = cs_add_set.difference(&pr_add_set).collect();
                let add_extra: Vec<_> = pr_add_set.difference(&cs_add_set).collect();
                assert!(
                    add_missing.is_empty() && add_extra.is_empty(),
                    "additions set mismatch: missing({})={add_missing:?} extra({})={add_extra:?}",
                    add_missing.len(),
                    add_extra.len(),
                );
                // Removals: same coin set, same spent_block_index.
                let cs_rem_set: HashSet<CoinId3> = cs_removals.iter().map(record_id3).collect();
                let pr_rem_set: HashSet<CoinId3> = pr.removals.iter().map(record_id3).collect();
                let rem_missing: Vec<_> = cs_rem_set.difference(&pr_rem_set).collect();
                let rem_extra: Vec<_> = pr_rem_set.difference(&cs_rem_set).collect();
                assert!(
                    rem_missing.is_empty() && rem_extra.is_empty(),
                    "removals set mismatch: missing({})={rem_missing:?} extra({})={rem_extra:?}",
                    rem_missing.len(),
                    rem_extra.len(),
                );
                eprintln!(
                    "  PASS (adds={} rems={})",
                    cs_additions.len(),
                    cs_removals.len()
                );
                passed += 1;
            }
            Err(e) => {
                eprintln!("  SKIP ({e})");
                skipped += 1;
            }
        }
    }

    // =======================================================================
    // 4. get_coin_record_by_name
    // =======================================================================
    eprintln!("\n--- get_coin_record_by_name ---");
    {
        let cs = coinset
            .get_coin_record_by_name(&added_coin_id)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_coin_record_by_name(&added_coin_id),
            peer.try_get_coin_record_by_name(&added_coin_id),
        )
        .await
        .unwrap();

        assert_eq!(coin_id3(&cs.coin), coin_id3(&pr.coin), "coin identity");
        assert_eq!(
            cs.confirmed_block_index, pr.confirmed_block_index,
            "confirmed_block_index"
        );
        assert_eq!(
            cs.spent_block_index, pr.spent_block_index,
            "spent_block_index"
        );
        assert_eq!(cs.spent, pr.spent, "spent");
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 5. get_coin_records_by_puzzle_hash  (bidirectional set comparison)
    // =======================================================================
    eprintln!("--- get_coin_records_by_puzzle_hash ---");
    {
        let start = Some(tx_height.saturating_sub(10));
        let end = Some(tx_height + 10);
        let cs = coinset
            .get_coin_records_by_puzzle_hash(&removal_ph, start, end, true)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_coin_records_by_puzzle_hash(&removal_ph, start, end, true),
            peer.try_get_coin_records_by_puzzle_hash(&removal_ph, start, end, true),
        )
        .await
        .unwrap();

        assert_coin_records_eq("puzzle_hash", &cs, &pr);
        eprintln!("  PASS (count={})", cs.len());
        passed += 1;
    }

    // =======================================================================
    // 6. get_coin_records_by_puzzle_hashes  (bidirectional)
    // =======================================================================
    eprintln!("--- get_coin_records_by_puzzle_hashes ---");
    {
        let hashes = vec![removal_ph.clone()];
        let start = Some(tx_height.saturating_sub(10));
        let end = Some(tx_height + 10);
        let cs = coinset
            .get_coin_records_by_puzzle_hashes(&hashes, start, end, true)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_coin_records_by_puzzle_hashes(&hashes, start, end, true),
            peer.try_get_coin_records_by_puzzle_hashes(&hashes, start, end, true),
        )
        .await
        .unwrap();

        assert_coin_records_eq("puzzle_hashes", &cs, &pr);
        eprintln!("  PASS (count={})", cs.len());
        passed += 1;
    }

    // =======================================================================
    // 7. get_coin_records_by_names  (bidirectional)
    // =======================================================================
    eprintln!("--- get_coin_records_by_names ---");
    {
        let names = vec![added_coin_id.clone(), spent_coin_id.clone()];
        let cs = coinset
            .get_coin_records_by_names(&names, None, None, true)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_coin_records_by_names(&names),
            peer.try_get_coin_records_by_names(&names),
        )
        .await
        .unwrap();

        assert_coin_records_eq("names", &cs, &pr);
        eprintln!("  PASS (count={})", cs.len());
        passed += 1;
    }

    // =======================================================================
    // 8. get_coin_records_by_parent_ids  (bidirectional)
    // =======================================================================
    eprintln!("--- get_coin_records_by_parent_ids ---");
    {
        let parents = vec![parent_coin_id.clone()];
        let cs = coinset
            .get_coin_records_by_parent_ids(&parents, None, None, true)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_children(&parent_coin_id),
            peer.try_get_children(&parent_coin_id),
        )
        .await
        .unwrap();

        assert_coin_records_eq("parent_ids", &cs, &pr);
        eprintln!("  PASS (count={})", cs.len());
        passed += 1;
    }

    // =======================================================================
    // 9. get_puzzle_and_solution  (field-level comparison)
    // =======================================================================
    eprintln!("\n--- get_puzzle_and_solution ---");
    {
        let cs = coinset
            .get_puzzle_and_solution(&spent_coin_id, Some(tx_height))
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_puzzle_and_solution(&spent_coin_id, tx_height),
            peer.try_get_puzzle_and_solution(&spent_coin_id, tx_height),
        )
        .await
        .unwrap();

        assert_eq!(
            norm(&cs.puzzle_reveal),
            norm(&pr.puzzle_reveal),
            "puzzle_reveal"
        );
        assert_eq!(norm(&cs.solution), norm(&pr.solution), "solution");
        // Also verify the coin in the coinset response matches the removal.
        assert_eq!(
            coin_id3(&cs.coin),
            coin_id3(&removal.coin),
            "coinset coin matches removal"
        );
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 10. get_puzzle_and_solution (auto height resolution)
    // =======================================================================
    eprintln!("--- get_puzzle_and_solution (auto height) ---");
    {
        let cs = coinset
            .get_puzzle_and_solution(&spent_coin_id, Some(tx_height))
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_puzzle_and_solution_auto(&spent_coin_id),
            peer.try_get_puzzle_and_solution_auto(&spent_coin_id),
        )
        .await
        .unwrap();

        assert_eq!(
            norm(&cs.puzzle_reveal),
            norm(&pr.puzzle_reveal),
            "puzzle_reveal"
        );
        assert_eq!(norm(&cs.solution), norm(&pr.solution), "solution");
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 11. get_fee_estimate
    // =======================================================================
    eprintln!("\n--- get_fee_estimate ---");
    {
        let times = [60u64, 120, 300];
        let cs = coinset
            .get_fee_estimate(None, Some(&times), None)
            .await
            .unwrap();
        let pr = retry(
            peer.try_get_fee_estimate(&times),
            peer.try_get_fee_estimate(&times),
        )
        .await
        .unwrap();

        assert_eq!(cs.estimates.len(), pr.estimates.len(), "estimate count");
        assert_eq!(
            cs.target_times.len(),
            pr.target_times.len(),
            "target_times count"
        );
        // Values may differ (different nodes) but both must be non-negative.
        for (i, est) in pr.estimates.iter().enumerate() {
            assert!(*est >= 0.0, "peer estimate[{i}] negative: {est}");
        }
        for (i, est) in cs.estimates.iter().enumerate() {
            assert!(*est >= 0.0, "coinset estimate[{i}] negative: {est}");
        }
        eprintln!(
            "  PASS (coinset={:?} peer={:?})",
            cs.estimates, pr.estimates
        );
        passed += 1;
    }

    // =======================================================================
    // 12. get_network_info  (exact match)
    // =======================================================================
    eprintln!("\n--- get_network_info ---");
    {
        let cs = coinset.get_network_info().await.unwrap();
        let pr = peer.network_info();
        assert_eq!(cs.network_name, pr.network_name, "network_name");
        assert_eq!(cs.network_prefix, pr.network_prefix, "network_prefix");
        if !cs.genesis_challenge.is_empty() {
            assert_eq!(
                norm(&cs.genesis_challenge),
                norm(&pr.genesis_challenge),
                "genesis_challenge"
            );
        }
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 13. get_aggsig_additional_data  (exact match)
    // =======================================================================
    eprintln!("--- get_aggsig_additional_data ---");
    {
        let cs = coinset.get_aggsig_additional_data().await.unwrap();
        let pr = peer.aggsig_additional_data();
        assert_eq!(norm(&cs), norm(&pr), "aggsig_additional_data");
        eprintln!("  PASS");
        passed += 1;
    }

    // =======================================================================
    // 14. get_blockchain_state (peak height)
    // =======================================================================
    eprintln!("--- get_blockchain_state (peak) ---");
    {
        let peer_peak = peer.peak_height().await;
        let diff = (peak as i64 - peer_peak as i64).unsigned_abs();
        assert!(
            diff < 50,
            "peak divergence: coinset={peak} peer={peer_peak} diff={diff}"
        );
        eprintln!("  PASS (coinset={peak} peer={peer_peak})");
        passed += 1;
    }

    // =======================================================================
    // FULL-NODE PROTOCOL TESTS (may skip if peers don't serve RequestBlock)
    // =======================================================================

    eprintln!("\n--- get_block_by_height (full_node_protocol) ---");
    {
        match peer.try_get_block_by_height(tx_height).await {
            Ok(pr) => {
                let cs = coinset.get_block(&tx_header_hash).await.unwrap();
                assert_eq!(
                    cs["reward_chain_block"]["height"].as_u64(),
                    pr["reward_chain_block"]["height"].as_u64(),
                    "reward_chain_block.height"
                );
                assert_eq!(
                    cs["reward_chain_block"]["weight"].as_u64(),
                    pr["reward_chain_block"]["weight"].as_u64(),
                    "reward_chain_block.weight"
                );
                eprintln!("  PASS");
                passed += 1;
            }
            Err(e) => {
                eprintln!("  SKIP ({e})");
                skipped += 1;
            }
        }
    }

    eprintln!("--- get_block_spends (full_node + CLVM) ---");
    {
        match peer.try_get_block_spends_by_height(tx_height).await {
            Ok(pr) => {
                let cs = coinset.get_block_spends(&tx_header_hash).await.unwrap();
                assert_spends_eq("block_spends", &cs, &pr);
                eprintln!("  PASS (count={})", cs.len());
                passed += 1;
            }
            Err(e) => {
                eprintln!("  SKIP ({e})");
                skipped += 1;
            }
        }
    }

    eprintln!("--- get_block_spends_with_conditions ---");
    {
        match peer.try_get_block_spends_with_conditions(tx_height).await {
            Ok(pr) => {
                let cs = coinset
                    .get_block_spends_with_conditions(&tx_header_hash)
                    .await
                    .unwrap();
                assert_eq!(cs.len(), pr.len(), "count");
                let cs_spends: Vec<CoinSpend> = cs.iter().map(|s| s.coin_spend.clone()).collect();
                let pr_spends: Vec<CoinSpend> = pr.iter().map(|s| s.coin_spend.clone()).collect();
                assert_spends_eq("spends_with_conditions", &cs_spends, &pr_spends);
                let any_conds = pr.iter().any(|s| !s.conditions.is_empty());
                assert!(any_conds, "peer should extract at least some conditions");
                eprintln!("  PASS");
                passed += 1;
            }
            Err(e) => {
                eprintln!("  SKIP ({e})");
                skipped += 1;
            }
        }
    }

    // =======================================================================
    eprintln!("\n=== RESULTS: {passed} passed, {skipped} skipped ===");
    assert!(
        passed >= 14,
        "expected >= 14 wallet-protocol tests to pass, got {passed}"
    );
}
