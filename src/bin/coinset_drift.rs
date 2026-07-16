//! coinset.org API drift monitor.
//!
//! Probes a fixed, representative set of coinset.org endpoints, reduces each
//! response to its type-shape (see [`chia_query::drift`]), and compares the
//! result against the committed snapshot `tests/fixtures/coinset-api-snapshot.json`.
//!
//! Usage:
//!   coinset_drift check    # diff live shapes vs the snapshot; exit 1 on drift (default)
//!   coinset_drift update   # regenerate the snapshot from live responses
//!
//! Run by `.github/workflows/coinset-drift.yml` on a daily cron; it is NOT a
//! merge gate — a coinset outage must never block PRs.

use std::process::ExitCode;
use std::time::Duration;

use serde_json::{json, Map, Value};

use chia_query::coinset::CoinsetClient;

const SNAPSHOT_PATH: &str = "tests/fixtures/coinset-api-snapshot.json";
const BASE_URL: &str = "https://api.coinset.org";

/// A representative endpoint probe: a display name, the endpoint, and its body.
struct Probe {
    /// Snapshot key — the endpoint, suffixed to distinguish multiple probes of
    /// the same endpoint (e.g. the error case).
    name: &'static str,
    endpoint: &'static str,
    body: Value,
}

/// The fixed probe set: chain state, network identity, a stable block record,
/// and an error envelope (which guards the `structuredError`/`traceback`
/// fields the client now parses).
fn probes() -> Vec<Probe> {
    vec![
        Probe {
            name: "get_blockchain_state",
            endpoint: "get_blockchain_state",
            body: json!({}),
        },
        Probe {
            name: "get_network_info",
            endpoint: "get_network_info",
            body: json!({}),
        },
        Probe {
            name: "get_block_record_by_height",
            endpoint: "get_block_record_by_height",
            body: json!({ "height": 1 }),
        },
        Probe {
            // A well-formed but absent coin id — returns the error envelope so
            // its shape (error / structuredError / traceback / success) is
            // snapshotted and watched.
            name: "get_coin_record_by_name__error",
            endpoint: "get_coin_record_by_name",
            body: json!({ "name": format!("0x{}", "0".repeat(64)) }),
        },
    ]
}

#[tokio::main]
async fn main() -> ExitCode {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "check".into());
    match run(&mode).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("coinset_drift: {err}");
            ExitCode::from(2)
        }
    }
}

async fn run(mode: &str) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let live = collect_shapes().await?;

    match mode {
        "update" => {
            let serialized = format!("{}\n", serde_json::to_string_pretty(&live)?);
            std::fs::write(SNAPSHOT_PATH, serialized)?;
            println!("wrote {SNAPSHOT_PATH}");
            Ok(ExitCode::SUCCESS)
        }
        "check" => {
            let baseline: Value = serde_json::from_str(&std::fs::read_to_string(SNAPSHOT_PATH)?)?;
            let drifts = chia_query::drift::diff_shapes("", &baseline, &live);
            if drifts.is_empty() {
                println!("no drift: coinset.org matches the committed snapshot");
                Ok(ExitCode::SUCCESS)
            } else {
                println!("DRIFT DETECTED against {SNAPSHOT_PATH}:");
                for line in &drifts {
                    println!("- {line}");
                }
                Ok(ExitCode::FAILURE)
            }
        }
        other => Err(format!("unknown mode `{other}` (expected `check` or `update`)").into()),
    }
}

/// Probe every endpoint and return `{ probe_name: shape }`.
async fn collect_shapes() -> Result<Value, Box<dyn std::error::Error>> {
    let client = CoinsetClient::new(BASE_URL, Duration::from_secs(30))?;
    let mut shapes = Map::new();
    for probe in probes() {
        let response = client.post_raw(probe.endpoint, &probe.body).await?;
        shapes.insert(
            probe.name.to_string(),
            chia_query::drift::shape_of(&response),
        );
    }
    Ok(Value::Object(shapes))
}
