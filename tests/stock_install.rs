//! A STOCK install — no Chia installation, no `~/.chia`, no certificate anywhere on
//! disk — must be able to construct a [`ChiaQuery`] client.
//!
//! Regression for dig_ecosystem#2210: `dig-node` runs as a Windows service under
//! SYSTEM, whose home is `C:\Windows\system32\config\systemprofile`. That profile has
//! no `.chia` directory and no writable `.chia/mainnet/config/ssl/wallet` parent, so
//! resolving the peer TLS identity from the home directory failed the whole client and
//! every balance read answered `-32040 WALLET_NO_CHAIN_SOURCE`.
//!
//! Each test runs with the home directory pointed at an EMPTY temporary directory, so
//! it can never pass merely because the developer's machine happens to have `~/.chia`
//! — which is exactly the condition that hid the bug.

use std::path::{Path, PathBuf};

use chia_query::{ChiaQuery, ChiaQueryConfig, TlsIdentity};

/// Point the home directory at a fresh empty directory and return it.
///
/// Writes both `USERPROFILE` and `HOME` so the fixture is platform-independent.
/// Every test in this binary sets the SAME value, so the process-global mutation is
/// safe under the default parallel test harness.
fn empty_home(tag: &str) -> PathBuf {
    let home = std::env::temp_dir().join(format!("chia-query-2210-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).expect("create fake home");
    std::env::set_var("USERPROFILE", &home);
    std::env::set_var("HOME", &home);
    home
}

/// Every path under `root`, so a test can assert nothing was written there.
fn entries_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(read) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in read.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path.clone());
            }
            found.push(path);
        }
    }
    found
}

/// The default configuration must not depend on a certificate existing on disk.
#[test]
fn default_config_generates_its_tls_identity() {
    assert!(
        matches!(
            ChiaQueryConfig::default().tls_identity,
            TlsIdentity::Generated
        ),
        "the default TLS identity must be generated, not read from the home directory"
    );
}

/// Generating the identity must need no filesystem at all — not even a writable one.
///
/// The nearest wrong implementation generates a certificate and then PERSISTS it under
/// the home directory (which is what `chia_wallet_sdk::client::load_ssl_cert` does, and
/// why #2210 failed: the `.chia/.../wallet` parent does not exist under SYSTEM). The
/// emptiness assertion below is what distinguishes this fix from that one.
#[test]
fn generated_identity_writes_nothing_to_the_home_directory() {
    let home = empty_home("tls");

    chia_query::peer::connect::create_generated_tls().expect("generate a TLS identity");

    assert_eq!(
        entries_under(&home),
        Vec::<PathBuf>::new(),
        "generating a TLS identity must not write to the home directory"
    );
}

/// A GENERATED certificate is accepted by real mainnet full nodes.
///
/// This is the premise the whole fix rests on — that Chia peer TLS does not treat the
/// client certificate as a credential. [`client_constructs_with_no_chia_installation`]
/// cannot prove it, because with the coinset fallback enabled construction succeeds
/// even when zero peers connect. Here the requirement is [`PeerRequirement::Required`],
/// so the pool must genuinely complete a TLS handshake with a live node.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires network access"]
async fn generated_identity_is_accepted_by_real_peers() {
    empty_home("peers");

    let tls = chia_query::peer::connect::create_generated_tls().expect("generate a TLS identity");
    let peers = chia_query::peer::PeerBackend::new(
        chia_query::NetworkType::Mainnet,
        tls,
        3,
        chia_query::peer::PeerRequirement::Required,
        std::time::Duration::from_secs(15),
        std::time::Duration::from_secs(30),
    )
    .await
    .expect("a generated certificate must be accepted by mainnet full nodes");

    assert!(
        peers.has_peers().await,
        "at least one mainnet peer must be connected with a generated certificate"
    );
}

/// The end-to-end link that was broken: constructing the client on a machine with no
/// Chia installation. Requires network (peer discovery + coinset), hence `#[ignore]`;
/// run with `cargo test --features native --test stock_install -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires network access"]
async fn client_constructs_with_no_chia_installation() {
    let home = empty_home("client");

    ChiaQuery::new(ChiaQueryConfig::default())
        .await
        .expect("construct a client on a machine with no Chia installation");

    assert_eq!(
        entries_under(&home),
        Vec::<PathBuf>::new(),
        "constructing a client must not write to the home directory"
    );
}
