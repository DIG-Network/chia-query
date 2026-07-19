//! `provider_registry` — chia-query as the aggregating `ChainSource` registry.
//!
//! This module makes chia-query the canonical `dig-chainsource-interface` registry: it wraps the
//! async [`QueryRouter`](crate::router::QueryRouter) in a blocking-facade `ChainSource` provider and
//! composes providers (coinset.org, a local node, DIG peers, an operator override) under an
//! operator-assigned trust model whose custody view fails closed.
//!
//! Implementation tracked in EPIC #1240 (dig-chainsource-interface — canonical ChainSource provider
//! interface + registry).

// Anchor stub — real implementation lands in the same PR (TDD, #1240).
