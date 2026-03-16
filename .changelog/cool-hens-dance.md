---
reth-engine-tree: minor
reth-node-builder: minor
---

Added `EngineSharedCaches` struct that bundles `PayloadExecutionCache`, `SharedPreservedSparseTrie`, and `PrecompileCacheMap` into a single launcher-owned handle. Updated `PayloadProcessor::new()` and `EngineValidatorBuilder` to accept `EngineSharedCaches`, and added `build_tree_validator_with_caches` to the builder trait with a default fallback to the existing method.
