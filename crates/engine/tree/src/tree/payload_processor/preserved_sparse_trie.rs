//! Preserved sparse trie for reuse across payload validations.

use super::{
    PARALLEL_SPARSE_TRIE_PARALLELISM_THRESHOLDS, SPARSE_TRIE_MAX_NODES_SHRINK_CAPACITY,
    SPARSE_TRIE_MAX_VALUES_SHRINK_CAPACITY,
};
use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_trie_sparse::{
    ArenaParallelSparseTrie, ConfigurableSparseTrie, ParallelSparseTrie, RevealableSparseTrie,
    SparseStateTrie,
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::debug;

/// Type alias for the sparse trie type used in preservation.
type SparseTrie = SparseStateTrie<ConfigurableSparseTrie, ConfigurableSparseTrie>;

/// Sparse trie implementation used by [`PayloadSparseTrieCache`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSparseTrieKind {
    /// Back sparse trie storage with hash maps.
    #[default]
    HashMap,
    /// Back sparse trie storage with arena allocations.
    Arena,
}

impl From<bool> for PayloadSparseTrieKind {
    fn from(enable_arena_sparse_trie: bool) -> Self {
        if enable_arena_sparse_trie {
            Self::Arena
        } else {
            Self::HashMap
        }
    }
}

#[derive(Debug, Default)]
struct PayloadSparseTrieState {
    latest_checkout_id: u64,
    preserved: Option<PreservedSparseTrie>,
}

/// Outcome of storing a checked-out sparse trie back into the shared cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSparseTrieStoreOutcome {
    /// The checkout was the most recent lease and the trie was stored.
    Stored,
    /// A newer checkout had already been issued, so this stale lease was ignored.
    IgnoredStaleCheckout,
}

/// Shared sparse trie cache that can be reused across payload validations.
///
/// This is the public sparse-trie SDK surface exposed through
/// [`EngineSharedCaches`](super::EngineSharedCaches). Callers take or create a trie, use it for
/// payload work, then store it back either anchored to the resulting state root or cleared for
/// allocation reuse.
#[derive(Debug, Clone)]
pub struct PayloadSparseTrieCache {
    kind: PayloadSparseTrieKind,
    state: Arc<Mutex<PayloadSparseTrieState>>,
}

impl Default for PayloadSparseTrieCache {
    fn default() -> Self {
        Self::new(PayloadSparseTrieKind::default())
    }
}

impl PayloadSparseTrieCache {
    /// Creates a sparse trie cache backed by the requested trie implementation.
    pub fn new(kind: PayloadSparseTrieKind) -> Self {
        Self { kind, state: Arc::new(Mutex::new(PayloadSparseTrieState::default())) }
    }

    /// Returns the sparse trie implementation used when the cache needs to create a new trie.
    pub const fn kind(&self) -> PayloadSparseTrieKind {
        self.kind
    }

    /// Takes a preserved trie for `parent_state_root` or creates a new trie if the cache is empty.
    pub fn take_or_create_for(&self, parent_state_root: B256) -> SparseTrieCheckout {
        let start = Instant::now();
        let mut state = self.state.lock();
        state.latest_checkout_id += 1;
        let checkout_id = state.latest_checkout_id;
        let trie = state
            .preserved
            .take()
            .map(|preserved| preserved.into_trie_for(parent_state_root))
            .unwrap_or_else(|| {
                debug!(
                    target: "engine::tree::payload_processor",
                    %parent_state_root,
                    kind = ?self.kind,
                    "Creating new sparse trie - no preserved trie available"
                );
                new_sparse_trie(self.kind)
            });
        drop(state);

        let elapsed = start.elapsed();
        if elapsed.as_millis() > 5 {
            debug!(
                target: "engine::tree::payload_processor",
                blocked_for=?elapsed,
                "Waited for preserved sparse trie checkout"
            );
        }

        SparseTrieCheckout { trie: Some(trie), cache: self.clone(), checkout_id }
    }

    /// Waits until the sparse trie lock becomes available.
    ///
    /// Returns the time spent waiting for the lock.
    pub fn wait_for_availability(&self) -> Duration {
        let start = Instant::now();
        let _guard = self.state.lock();
        let elapsed = start.elapsed();
        if elapsed.as_millis() > 5 {
            debug!(
                target: "engine::tree::payload_processor",
                blocked_for=?elapsed,
                "Waited for preserved sparse trie to become available"
            );
        }
        elapsed
    }

    /// Acquires a guard that blocks cache mutation until dropped.
    ///
    /// Engine-internal code uses this before making the state-root result visible so the next
    /// payload cannot observe an empty cache between send and store.
    pub(super) fn lock(&self) -> PreservedTrieGuard<'_> {
        PreservedTrieGuard { state: self.state.lock() }
    }
}

/// A checked-out sparse trie lease.
///
/// This dereferences to [`SparseStateTrie`] so callers can reuse the trie directly. If the lease is
/// dropped without being stored back, a cleared trie is returned to the shared cache unless a newer
/// checkout has already superseded it.
#[derive(Debug)]
pub struct SparseTrieCheckout {
    trie: Option<SparseTrie>,
    cache: PayloadSparseTrieCache,
    checkout_id: u64,
}

impl SparseTrieCheckout {
    /// Stores the trie back into the shared cache anchored to the given state root.
    pub fn store_anchored(self, state_root: B256) -> PayloadSparseTrieStoreOutcome {
        let cache = self.cache.clone();
        let mut guard = cache.lock();
        self.store_anchored_with_guard(&mut guard, state_root)
    }

    /// Stores the trie back into the shared cache in a cleared state.
    pub fn store_cleared(mut self) -> PayloadSparseTrieStoreOutcome {
        let cache = self.cache.clone();
        let mut trie = self.take_trie();
        prepare_cleared_trie(&mut trie);
        let deferred = trie.take_deferred_drops();
        let mut guard = cache.lock();
        let outcome = guard.store(self.checkout_id, PreservedSparseTrie::cleared(trie));
        drop(guard);
        drop(deferred);
        outcome
    }

    /// Stores the trie back into the shared cache anchored to the given state root while the
    /// caller is already holding the preservation lock.
    pub(super) fn store_anchored_with_guard(
        mut self,
        guard: &mut PreservedTrieGuard<'_>,
        state_root: B256,
    ) -> PayloadSparseTrieStoreOutcome {
        guard.store(self.checkout_id, PreservedSparseTrie::anchored(self.take_trie(), state_root))
    }

    /// Stores an already-cleared trie back into the shared cache while the caller is already
    /// holding the preservation lock.
    pub(super) fn store_prepared_cleared_with_guard(
        mut self,
        guard: &mut PreservedTrieGuard<'_>,
    ) -> PayloadSparseTrieStoreOutcome {
        guard.store(self.checkout_id, PreservedSparseTrie::cleared(self.take_trie()))
    }

    fn take_trie(&mut self) -> SparseTrie {
        self.trie.take().expect("sparse trie checkout must hold a trie until it is stored")
    }
}

impl Deref for SparseTrieCheckout {
    type Target = SparseTrie;

    fn deref(&self) -> &Self::Target {
        self.trie.as_ref().expect("sparse trie checkout must hold a trie until it is stored")
    }
}

impl DerefMut for SparseTrieCheckout {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.trie.as_mut().expect("sparse trie checkout must hold a trie until it is stored")
    }
}

impl Drop for SparseTrieCheckout {
    fn drop(&mut self) {
        let Some(mut trie) = self.trie.take() else { return };

        debug!(
            target: "engine::tree::payload_processor",
            checkout_id = self.checkout_id,
            "Sparse trie checkout dropped before store, returning cleared trie to cache"
        );

        prepare_cleared_trie(&mut trie);
        let deferred = trie.take_deferred_drops();
        let mut guard = self.cache.lock();
        let _ = guard.store(self.checkout_id, PreservedSparseTrie::cleared(trie));
        drop(guard);
        drop(deferred);
    }
}

/// Guard that holds the lock on the preserved trie.
/// While held, take-or-create calls will block. Call `store()` to save the trie before dropping.
pub(super) struct PreservedTrieGuard<'a> {
    state: parking_lot::MutexGuard<'a, PayloadSparseTrieState>,
}

impl PreservedTrieGuard<'_> {
    /// Stores a preserved trie for later reuse if the checkout is still current.
    fn store(
        &mut self,
        checkout_id: u64,
        trie: PreservedSparseTrie,
    ) -> PayloadSparseTrieStoreOutcome {
        if checkout_id != self.state.latest_checkout_id {
            debug!(
                target: "engine::tree::payload_processor",
                checkout_id,
                latest_checkout_id = self.state.latest_checkout_id,
                "Ignoring stale sparse trie checkout"
            );
            return PayloadSparseTrieStoreOutcome::IgnoredStaleCheckout;
        }

        self.state.preserved.replace(trie);
        PayloadSparseTrieStoreOutcome::Stored
    }
}

/// A preserved sparse trie that can be reused across payload validations.
///
/// The trie exists in one of two states:
/// - **Anchored**: Has a computed state root and can be reused for payloads whose parent state
///   root matches the anchor.
/// - **Cleared**: Trie data has been cleared but allocations are preserved for reuse.
#[derive(Debug)]
enum PreservedSparseTrie {
    /// Trie with a computed state root that can be reused for continuation payloads.
    Anchored {
        /// The sparse state trie (pruned after root computation).
        trie: SparseTrie,
        /// The state root this trie represents (computed from the previous block).
        /// Used to verify continuity: new payload's `parent_state_root` must match this.
        state_root: B256,
    },
    /// Cleared trie with preserved allocations, ready for fresh use.
    Cleared {
        /// The sparse state trie with cleared data but preserved allocations.
        trie: SparseTrie,
    },
}

impl PreservedSparseTrie {
    /// Creates a new anchored preserved trie.
    const fn anchored(trie: SparseTrie, state_root: B256) -> Self {
        Self::Anchored { trie, state_root }
    }

    /// Creates a cleared preserved trie (allocations preserved, data cleared).
    const fn cleared(trie: SparseTrie) -> Self {
        Self::Cleared { trie }
    }

    /// Consumes self and returns the trie for reuse.
    fn into_trie_for(self, parent_state_root: B256) -> SparseTrie {
        match self {
            Self::Anchored { trie, state_root } if state_root == parent_state_root => {
                debug!(
                    target: "engine::tree::payload_processor",
                    %state_root,
                    "Reusing anchored sparse trie for continuation payload"
                );
                trie
            }
            Self::Anchored { mut trie, state_root } => {
                debug!(
                    target: "engine::tree::payload_processor",
                    anchor_root = %state_root,
                    %parent_state_root,
                    "Clearing anchored sparse trie - parent state root mismatch"
                );
                trie.clear();
                trie
            }
            Self::Cleared { trie } => {
                debug!(
                    target: "engine::tree::payload_processor",
                    %parent_state_root,
                    "Using cleared sparse trie with preserved allocations"
                );
                trie
            }
        }
    }
}

fn new_sparse_trie(kind: PayloadSparseTrieKind) -> SparseTrie {
    let default_trie = match kind {
        PayloadSparseTrieKind::HashMap => {
            RevealableSparseTrie::blind_from(ConfigurableSparseTrie::HashMap(
                ParallelSparseTrie::default()
                    .with_parallelism_thresholds(PARALLEL_SPARSE_TRIE_PARALLELISM_THRESHOLDS),
            ))
        }
        PayloadSparseTrieKind::Arena => RevealableSparseTrie::blind_from(
            ConfigurableSparseTrie::Arena(ArenaParallelSparseTrie::default()),
        ),
    };

    SparseStateTrie::default()
        .with_accounts_trie(default_trie.clone())
        .with_default_storage_trie(default_trie)
        .with_updates(true)
}

fn prepare_cleared_trie(trie: &mut SparseTrie) {
    trie.clear();
    trie.shrink_to(SPARSE_TRIE_MAX_NODES_SHRINK_CAPACITY, SPARSE_TRIE_MAX_VALUES_SHRINK_CAPACITY);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_or_create_reuses_matching_anchor() {
        let cache = PayloadSparseTrieCache::default();
        let state_root = B256::with_last_byte(1);

        assert_eq!(
            cache.take_or_create_for(state_root).store_anchored(state_root),
            PayloadSparseTrieStoreOutcome::Stored
        );

        match cache.state.lock().preserved.as_ref() {
            Some(PreservedSparseTrie::Anchored { state_root: anchored, .. }) => {
                assert_eq!(*anchored, state_root);
            }
            other => panic!("expected anchored trie, got {other:?}"),
        }
    }

    #[test]
    fn drop_restores_cleared_trie() {
        let cache = PayloadSparseTrieCache::default();
        let state_root = B256::with_last_byte(2);

        let mut checkout = cache.take_or_create_for(state_root);
        checkout.set_updates(true);
        drop(checkout);

        match cache.state.lock().preserved.as_ref() {
            Some(PreservedSparseTrie::Cleared { .. }) => {}
            other => panic!("expected cleared trie, got {other:?}"),
        }
    }

    #[test]
    fn stale_checkout_does_not_overwrite_newer_store() {
        let cache = PayloadSparseTrieCache::default();
        let parent_state_root = B256::with_last_byte(3);
        let anchored_state_root = B256::with_last_byte(4);

        let stale = cache.take_or_create_for(parent_state_root);
        let fresh = cache.take_or_create_for(parent_state_root);

        assert_eq!(
            fresh.store_anchored(anchored_state_root),
            PayloadSparseTrieStoreOutcome::Stored
        );
        assert_eq!(stale.store_cleared(), PayloadSparseTrieStoreOutcome::IgnoredStaleCheckout);

        match cache.state.lock().preserved.as_ref() {
            Some(PreservedSparseTrie::Anchored { state_root, .. }) => {
                assert_eq!(*state_root, anchored_state_root);
            }
            other => panic!("expected anchored trie to survive stale checkout, got {other:?}"),
        }
    }
}
