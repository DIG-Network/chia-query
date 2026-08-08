# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org) and
[Conventional Commits](https://www.conventionalcommits.org).

## [0.7.0] - 2026-08-08

### Features
- **deps:** Migrate to the chia-0.36 family for dig-chainsource-interface 0.3 (#16)

## [0.6.0] - 2026-08-06

### Features
- Generate the peer TLS identity so no Chia installation is required (#15)

## [0.5.2] - 2026-07-21

### Refactor
- **provider-registry:** Adopt ChainSourceError::TooManyRecords for over-cap record counts (#14)

## [0.5.1] - 2026-07-21

### Bug Fixes
- **coinset:** Bound untrusted discovery + coinset responses (#1351, #1363) (#13)

## [0.5.0] - 2026-07-21

### Features
- **provider:** Lightweight no-handshake coinset ChainSource (CoinsetProvider::from_url) (#11)

## [0.4.4] - 2026-07-21

### Bug Fixes
- **coinset:** Refresh peak snapshot for tolerated coinset drift (#12)

## [0.4.3] - 2026-07-21

### Bug Fixes
- **provider-registry:** Launcher_id identity binding + quorum perf/bound hardening (#10)

## [0.4.2] - 2026-07-20

### Bug Fixes
- **provider-registry:** Order-insensitive quorum, atom overflow error, singleton-shape child check (#9)

## [0.4.1] - 2026-07-20

### Bug Fixes
- **lineage:** Max-generation depth cap + peer-coin liveness (#8)

## [0.4.0] - 2026-07-19

### Features
- **provider-registry:** ChainSource registry + trust model over the async router (#7)

## [0.3.2] - 2026-07-16

### CI
- **publish-wasm:** Pin npm to 11.x so the OIDC publish runs on Node 20 (#5)

## [0.3.1] - 2026-07-16

### CI
- **publish-wasm:** Publish chia-query-wasm via npm OIDC trusted publishing (#4)

## [0.3.0] - 2026-07-16

### Features
- **coinset:** StructuredError capture + drift-monitor + wasm coinset build (#3)

## [0.2.2] - 2026-07-12

### CI
- Add flaky-test management (#489) (#2)

## [0.2.1] - 2026-07-04

### CI
- Enforce version increment in PRs (package.json / Cargo.toml)- Enforce Conventional Commits with commitlint on PRs- Enforce Conventional Commits with commitlint on PRs- Release automation (git-cliff changelog + tag on merge); publish is manual workflow_dispatch (#230)- Re-arm crates.io auto-publish on version tag (token in org secrets; auto-publish-everything #230)- Add PR quality gates (fmt/clippy/test/build) [#230] (#1)

### Chores
- **changelog:** Add git-cliff config for Conventional-Commit changelog

## [0.2.0] - 2026-04-12

### Documentation
- Comprehensive API reference in README

### Chores
- Bump version to 0.2.0

## [0.1.0] - 2026-04-12

### Features
- Add query api

### Bug Fixes
- Resolve all fmt and clippy warnings, add CI and publishing

### Chores
- Remove target/ from tracking, add .gitignore


