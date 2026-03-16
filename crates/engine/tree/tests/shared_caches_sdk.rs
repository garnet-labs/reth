//! SDK smoke tests for `EngineSharedCaches`.

use alloy_primitives::B256;
use reth_engine_tree::tree::{
    EngineSharedCaches, PayloadSparseTrieKind, PayloadSparseTrieStoreOutcome,
};
use reth_evm_ethereum::EthEvmConfig;

#[test]
fn engine_shared_caches_exposes_public_sparse_trie_sdk() {
    let caches =
        EngineSharedCaches::<EthEvmConfig>::with_sparse_trie_kind(PayloadSparseTrieKind::Arena);

    let _precompile_cache_map = caches.precompile_cache_map();

    let sparse_trie_cache = caches.sparse_trie_cache();
    assert_eq!(sparse_trie_cache.kind(), PayloadSparseTrieKind::Arena);
    let state_root = B256::with_last_byte(1);

    assert_eq!(
        sparse_trie_cache.take_or_create_for(state_root).store_anchored(state_root),
        PayloadSparseTrieStoreOutcome::Stored
    );

    let checkout = sparse_trie_cache.take_or_create_for(state_root);
    assert!(checkout.memory_size() > 0 || checkout.retained_storage_tries_count() == 0);
}
