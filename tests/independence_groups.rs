//! A consumer can pin its OWN independence classification with no network (chia-query#38).
//!
//! `ProviderRegistry` decides custody by independence group: a pure-public quorum keeps one
//! representative answer per group and refuses below the threshold. The group id is therefore a
//! security-relevant input supplied by the consumer -- and until these accessors existed, no
//! consumer could check the id it passed except by watching quorum behaviour against a live
//! network, which is the one condition CI cannot reproduce.
//!
//! # The defect these tests exist to catch
//!
//! On DIG-Network/dig-node#354 a `ChiaQueryProvider` was registered under a literal `"chia-peers"`
//! while the `ChiaQuery` behind it was built with `coinset_fallback_enabled: true`, whose router
//! asks `api.coinset.org` FIRST. Both "independent" groups then answered from one endpoint, and a
//! client configured with `max_peers: 0` -- holding no peers whatsoever -- satisfied a two-of-two
//! independent-group custody quorum.
//!
//! So the bar is not "an accessor exists". It is that **hard-coding the group at the registration
//! site now FAILS an offline test**, which is what
//! [`a_coinset_backed_fabric_cannot_be_registered_as_a_pure_peer_group`] pins.

use std::sync::Arc;

use chia_query::provider_registry::interface::{ProviderId, ProviderInfo, ProviderKind};
use chia_query::provider_registry::{
    independence_group_for, ChiaQueryProvider, ProviderRegistry, TrustLevel,
    CHIA_PEERS_INDEPENDENCE_GROUP, COINSET_INDEPENDENCE_GROUP,
};
use chia_query::{ChiaQuery, ChiaQueryConfig, NetworkType, TlsIdentity};

/// A client that reaches coinset.org and NOTHING else, built with no network access.
///
/// `max_peers: 0` attempts no dial at all, and `CoinsetClient` construction is a local HTTP client
/// builder that contacts nothing. So this is the exact fabric from dig-node#354 -- a client holding
/// zero peers whose only reachable source is coinset.org -- constructible on an isolated runner.
async fn coinset_backed_query() -> ChiaQuery {
    ChiaQuery::new(ChiaQueryConfig {
        network: NetworkType::Mainnet,
        max_peers: 0,
        coinset_fallback_enabled: true,
        tls_identity: TlsIdentity::Generated,
        ..Default::default()
    })
    .await
    .expect("a coinset-backed client constructs without dialling a peer")
}

fn info(id: &'static str) -> ProviderInfo {
    ProviderInfo {
        id: ProviderId(id.into()),
        kind: ProviderKind::Custom,
        priority: 20,
        trustless: false,
    }
}

/// **The mutation this catches: replacing `provider.independence_group()` at the `register` call
/// with the literal `"chia-peers"`.**
///
/// That is precisely the dig-node#354 registration, and today it passes every network-free test in
/// the ecosystem. Here the first assertion fails, because the registry reports the group it was
/// GIVEN while the provider reports the group its fabric can actually REACH, and a stale literal
/// makes those two disagree.
///
/// The assertions are ordered deliberately. The registry-vs-provider comparison comes FIRST so the
/// mutation fires on the wiring rather than on the constant, which is the property under test; the
/// later assertion then pins WHICH group, so a derivation returning the peer group for both
/// branches could not satisfy it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coinset_backed_fabric_cannot_be_registered_as_a_pure_peer_group() {
    let query = Arc::new(coinset_backed_query().await);
    let provider = ChiaQueryProvider::new(
        query.clone(),
        tokio::runtime::Handle::current(),
        info("chia-query"),
    );

    let group = provider.independence_group();
    let registry = ProviderRegistry::new().register(Box::new(provider), None, group);

    let registered = registry.registrations();
    assert_eq!(registered.len(), 1, "one provider was registered");

    assert_eq!(
        registered[0].independence_group,
        query.independence_group(),
        "the group the registry holds must match the group the fabric derives -- a literal typed \
         at the registration site is exactly the dig-node#354 defect",
    );
    assert_eq!(
        registered[0].independence_group, COINSET_INDEPENDENCE_GROUP,
        "a client whose router falls back to coinset.org shares the coinset group, however many \
         peers it also holds",
    );
    assert_ne!(
        registered[0].independence_group, CHIA_PEERS_INDEPENDENCE_GROUP,
        "reporting the peer group here is what let one endpoint satisfy a two-group quorum alone",
    );
}

/// The control that keeps the test above from passing on a constant.
///
/// Without it, an `independence_group_for` that ignored its argument and always returned the
/// coinset group would satisfy every assertion there while classifying a genuine pure-peer fabric
/// as coinset-backed -- collapsing two real groups into one and making a legitimate quorum
/// unsatisfiable.
#[test]
fn the_derivation_reads_its_input_rather_than_returning_a_constant() {
    assert_eq!(
        independence_group_for(true),
        COINSET_INDEPENDENCE_GROUP,
        "a coinset-reachable fabric shares the coinset group",
    );
    assert_eq!(
        independence_group_for(false),
        CHIA_PEERS_INDEPENDENCE_GROUP,
        "a fabric that cannot reach coinset is a pure peer fabric",
    );
    assert_ne!(
        independence_group_for(true),
        independence_group_for(false),
        "the two branches must be distinguishable, or the classification carries no information",
    );
}

/// **Two registrations sharing a group must be VISIBLE as sharing it.**
///
/// An accessor that deduplicated -- or that returned one entry per distinct group -- would report
/// two groups here and hide the exact state in which a two-of-two public quorum cannot be
/// satisfied, which is the state a consumer most needs to detect offline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registrations_expose_a_shared_group_rather_than_collapsing_it() {
    let query = Arc::new(coinset_backed_query().await);
    let handle = tokio::runtime::Handle::current();

    let router_provider = ChiaQueryProvider::new(query.clone(), handle.clone(), info("chia-query"));
    let router_group = router_provider.independence_group();

    let direct_coinset = ChiaQueryProvider::new(query.clone(), handle, info("coinset-direct"));

    let registry = ProviderRegistry::new()
        .register(Box::new(router_provider), None, router_group)
        .register(Box::new(direct_coinset), None, COINSET_INDEPENDENCE_GROUP);

    assert_eq!(
        registry.independence_groups(),
        vec![COINSET_INDEPENDENCE_GROUP, COINSET_INDEPENDENCE_GROUP],
        "both registrations resolve to coinset.org, and the duplicate is the finding -- not noise \
         to be deduplicated away",
    );
}

/// The registration view reports the TRUST actually recorded, not one it recomputed.
///
/// A `Custom` provider defaults to `Untrusted`; an explicit override is honoured. Asserting both in
/// one test is what separates "the view reads the stored field" from "the view recomputes the
/// default", which would silently discard an operator override.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_view_reports_the_stored_trust_not_a_recomputed_default() {
    let query = Arc::new(coinset_backed_query().await);
    let handle = tokio::runtime::Handle::current();

    let registry = ProviderRegistry::new()
        .register(
            Box::new(ChiaQueryProvider::new(
                query.clone(),
                handle.clone(),
                info("defaulted"),
            )),
            None,
            CHIA_PEERS_INDEPENDENCE_GROUP,
        )
        .register(
            Box::new(ChiaQueryProvider::new(query, handle, info("vouched"))),
            Some(TrustLevel::Trusted),
            COINSET_INDEPENDENCE_GROUP,
        );

    let registered = registry.registrations();
    assert_eq!(
        registered[0].trust,
        TrustLevel::Untrusted,
        "a Custom provider defaults to Untrusted",
    );
    assert_eq!(
        registered[1].trust,
        TrustLevel::Trusted,
        "an explicit operator override must survive into the view",
    );
    assert_eq!(registered[0].kind, ProviderKind::Custom);
}
