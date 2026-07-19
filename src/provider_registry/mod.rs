//! `provider_registry` — chia-query as the aggregating [`ChainSource`] registry.
//!
//! This module makes chia-query the canonical [`dig_chainsource_interface`] registry: it wraps the
//! async [`QueryRouter`](crate::router::QueryRouter) in a blocking-facade [`ChainSource`] provider
//! ([`ChiaQueryProvider`]) and composes providers (coinset.org, a local node, DIG peers, an
//! operator override) under an operator-assigned trust model whose custody view
//! ([`ProviderRegistry::trusted`]) FAILS CLOSED.
//!
//! ## Money-critical stance (SPEC §3–§5)
//!
//! - Every read distinguishes `Ok(None)` (provable absence — safe to act on) from `Err`
//!   (could-not-answer — the consumer MUST fail closed). A transport/timeout/parse failure is NEVER
//!   reported as absence, and absence is NEVER reported as an error.
//! - The CUSTODY view answers only from an operator-[`Trusted`](TrustLevel::Trusted) source or a
//!   qualifying quorum; a pure-public quorum fails closed unless the operator opts in
//!   ([`ProviderRegistry::allow_public_quorum_custody`]) to a two-independent-group agreement.
//! - A provider's self-declared `trustless` flag is ADVISORY only — it never grants custody trust.
//! - Singleton lineage authority is MEMBERSHIP
//!   ([`SingletonLineage::contains`](dig_chainsource_interface::SingletonLineage::contains)), never
//!   tip/puzzle-hash equality; the lineage is built by a genuine forward walk launcher -> tip.
//!
//! This module is native-only: it never reaches the wasm coinset-only build.

mod bridge;
mod chia_query_provider;
mod convert;
mod lineage_walk;
mod providers;
mod registry;

/// The canonical chain-source interface chia-query implements + aggregates, re-exported so
/// consumers of this registry can name its trait/types without a separate dependency line.
pub use dig_chainsource_interface as interface;

pub use chia_query_provider::ChiaQueryProvider;
pub use providers::{CoinsetProvider, CustomProvider, DigPeersProvider, LocalNodeProvider};
pub use registry::{DiscoveryView, ProviderRegistry, TrustLevel, TrustedView};
