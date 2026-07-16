# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org) and
[Conventional Commits](https://www.conventionalcommits.org).

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
