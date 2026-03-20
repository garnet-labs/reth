//! Shared history shard pruning logic.
//!
//! Both MDBX and RocksDB backends implement the same algorithm for pruning
//! history shards (account history, storage history). This module extracts
//! the decision logic into a pure planner that returns a `ShardPrunePlan`
//! — a list of delete/put operations that backends apply with their own I/O.

use crate::BlockNumberList;
use alloy_primitives::BlockNumber;

/// A planned mutation to a history shard.
#[derive(Debug, Clone)]
pub enum ShardOp<K> {
    /// Delete the shard at this key.
    Delete(K),
    /// Write (upsert) the shard at this key with the given block list.
    Put(K, BlockNumberList),
}

/// Outcome of planning a prune for one logical key's shard group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneShardOutcome {
    /// At least one shard was deleted.
    Deleted,
    /// At least one shard was updated (but none deleted).
    Updated,
    /// All shards were unchanged.
    Unchanged,
}

/// Result of planning a prune across one logical key's shard group.
#[derive(Debug, Clone)]
pub struct ShardPrunePlan<K> {
    /// Operations to apply.
    pub ops: Vec<ShardOp<K>>,
    /// Summary outcome.
    pub outcome: PruneShardOutcome,
}

/// Aggregated stats across multiple shard groups.
///
/// Each field counts the number of **logical key groups** (not individual shard
/// rows) that had the given outcome. For example, if a key has three shards and
/// two are deleted, `deleted` is incremented by one (for that key group).
#[derive(Debug, Default, Clone, Copy)]
pub struct PrunedShardStats {
    /// Number of shard groups where at least one shard was deleted.
    pub deleted: usize,
    /// Number of shard groups where shards were updated but not deleted.
    pub updated: usize,
    /// Number of shard groups that were unchanged.
    pub unchanged: usize,
}

impl PrunedShardStats {
    /// Record the outcome of one shard group.
    pub const fn record(&mut self, outcome: PruneShardOutcome) {
        match outcome {
            PruneShardOutcome::Deleted => self.deleted += 1,
            PruneShardOutcome::Updated => self.updated += 1,
            PruneShardOutcome::Unchanged => self.unchanged += 1,
        }
    }
}

/// Plans which shards to delete/update for one logical key's shard group.
///
/// This is backend-agnostic: it only inspects the shard data and returns
/// a plan of operations. The caller applies the plan using backend-specific
/// I/O (MDBX cursors, `RocksDB` batch, etc.).
///
/// The returned [`ShardPrunePlan::outcome`] represents the **per-group**
/// summary: `Deleted` if any shard in the group was deleted, `Updated` if
/// any was modified (but none deleted), or `Unchanged` otherwise.
///
/// If the input contains no sentinel shard (e.g. a partially-written state),
/// the planner will still promote the last surviving shard to the sentinel
/// position, effectively repairing the invariant.
///
/// # Arguments
///
/// * `shards` — All shards for one logical key, in DB order (ascending by `highest_block_number`).
///   The last shard should have the sentinel key (`u64::MAX`).
/// * `to_block` — Prune all block numbers `<= to_block`.
/// * `highest_block` — Extract the `highest_block_number` from a key.
/// * `is_sentinel` — Returns `true` if the key is the sentinel (`u64::MAX`).
/// * `make_sentinel` — Creates a sentinel key for this logical key.
pub fn plan_shard_prune<K: Clone>(
    shards: Vec<(K, BlockNumberList)>,
    to_block: BlockNumber,
    highest_block: impl Fn(&K) -> BlockNumber,
    is_sentinel: impl Fn(&K) -> bool,
    make_sentinel: impl Fn() -> K,
) -> ShardPrunePlan<K> {
    if shards.is_empty() {
        return ShardPrunePlan { ops: Vec::new(), outcome: PruneShardOutcome::Unchanged };
    }

    let mut ops = Vec::new();
    let mut deleted = false;
    let mut updated = false;
    let mut last_remaining: Option<(K, BlockNumberList)> = None;

    for (key, block_list) in shards {
        // Non-sentinel shard whose highest block is fully within the prune range —
        // delete it outright without inspecting individual block numbers.
        if !is_sentinel(&key) && highest_block(&key) <= to_block {
            ops.push(ShardOp::Delete(key));
            deleted = true;
            continue;
        }

        let original_len = block_list.len();
        let filtered =
            BlockNumberList::new_pre_sorted(block_list.iter().skip_while(|&b| b <= to_block));

        if filtered.is_empty() {
            ops.push(ShardOp::Delete(key));
            deleted = true;
        } else if filtered.len() < original_len {
            ops.push(ShardOp::Put(key.clone(), filtered.clone()));
            last_remaining = Some((key, filtered));
            updated = true;
        } else {
            // Unchanged — no op needed, but track as potential last surviving shard.
            last_remaining = Some((key, block_list));
        }
    }

    // If the last surviving shard is not the sentinel, promote it:
    // delete the old key and re-insert under the sentinel key.
    if let Some((last_key, last_value)) = last_remaining &&
        !is_sentinel(&last_key)
    {
        ops.push(ShardOp::Delete(last_key));
        ops.push(ShardOp::Put(make_sentinel(), last_value));
        updated = true;
    }

    let outcome = if deleted {
        PruneShardOutcome::Deleted
    } else if updated {
        PruneShardOutcome::Updated
    } else {
        PruneShardOutcome::Unchanged
    };

    ShardPrunePlan { ops, outcome }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_list(blocks: &[u64]) -> BlockNumberList {
        BlockNumberList::new_pre_sorted(blocks.iter().copied())
    }

    #[test]
    fn empty_shards_unchanged() {
        let plan = plan_shard_prune::<u64>(vec![], 10, |k| *k, |k| *k == u64::MAX, || u64::MAX);
        assert_eq!(plan.outcome, PruneShardOutcome::Unchanged);
        assert!(plan.ops.is_empty());
    }

    #[test]
    fn single_sentinel_shard_fully_pruned() {
        // Single shard with sentinel key, all blocks <= to_block
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[1, 2, 3]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0], ShardOp::Delete(u64::MAX)));
    }

    #[test]
    fn single_sentinel_shard_partially_pruned() {
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[5, 10, 15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Updated);
        // Should put the filtered list under the sentinel key
        assert_eq!(plan.ops.len(), 1);
        match &plan.ops[0] {
            ShardOp::Put(k, list) => {
                assert_eq!(*k, u64::MAX);
                assert_eq!(list.iter().collect::<Vec<_>>(), vec![15, 20]);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn single_sentinel_shard_unchanged() {
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Unchanged);
        assert!(plan.ops.is_empty());
    }

    #[test]
    fn non_sentinel_fully_below_to_block_deleted() {
        // Non-sentinel shard with highest_block_number <= to_block
        let plan = plan_shard_prune(
            vec![(5, make_list(&[1, 2, 3, 4, 5])), (u64::MAX, make_list(&[15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        // Should delete the first shard; sentinel unchanged
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0], ShardOp::Delete(5)));
    }

    #[test]
    fn all_shards_fully_pruned() {
        // Both shards are entirely within the prune range → both deleted.
        // - shard at key=8: highest 8 <= 10 → delete (non-sentinel fast path)
        // - sentinel: blocks [7,9] → both <= 10 → delete (empties)
        let plan = plan_shard_prune(
            vec![(8, make_list(&[5, 8])), (u64::MAX, make_list(&[7, 9]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        assert_eq!(plan.ops.len(), 2);
        assert!(matches!(plan.ops[0], ShardOp::Delete(8)));
        assert!(matches!(plan.ops[1], ShardOp::Delete(u64::MAX)));
    }

    #[test]
    fn sentinel_empties_previous_shard_promoted() {
        // Non-sentinel shard survives, sentinel empties → promote survivor to sentinel.
        // - shard at key=15: blocks [5, 12, 15] → skip_while <= 10 → [12, 15] (survives)
        // - sentinel: blocks [7, 9] → both <= 10 → delete (empties)
        // The surviving shard at key=15 must be promoted to sentinel (u64::MAX).
        let plan = plan_shard_prune(
            vec![(15, make_list(&[5, 12, 15])), (u64::MAX, make_list(&[7, 9]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        // Ops: Put(15, [12,15]), Delete(sentinel), Delete(15), Put(sentinel, [12,15])
        assert_eq!(plan.ops.len(), 4);
        // Last op should be the sentinel promotion
        match &plan.ops[plan.ops.len() - 1] {
            ShardOp::Put(k, list) => {
                assert_eq!(*k, u64::MAX);
                assert_eq!(list.iter().collect::<Vec<_>>(), vec![12, 15]);
            }
            _ => panic!("expected Put for sentinel promotion"),
        }
    }

    #[test]
    fn missing_sentinel_repaired_by_promotion() {
        // Input has no sentinel shard (e.g. partially-written state).
        // The planner should still promote the last surviving shard to sentinel.
        // shard at key=15: blocks [5, 12, 15] → skip_while <= 10 → [12, 15]
        let plan = plan_shard_prune(
            vec![(15, make_list(&[5, 12, 15]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Updated);
        // Ops: Put(15, [12,15]), Delete(15), Put(MAX, [12,15])
        assert_eq!(plan.ops.len(), 3);
        match &plan.ops[2] {
            ShardOp::Put(k, list) => {
                assert_eq!(*k, u64::MAX);
                assert_eq!(list.iter().collect::<Vec<_>>(), vec![12, 15]);
            }
            _ => panic!("expected Put for sentinel promotion"),
        }
    }

    #[test]
    fn multi_shard_mixed_outcomes() {
        // Three shards: first deleted, second partially pruned, sentinel unchanged.
        // - shard at key=5: highest 5 <= 10 → delete
        // - shard at key=15: blocks [8, 12, 15] → skip_while <= 10 → [12, 15]
        // - sentinel: blocks [20, 25] → all > 10 → unchanged
        let plan = plan_shard_prune(
            vec![
                (5, make_list(&[1, 3, 5])),
                (15, make_list(&[8, 12, 15])),
                (u64::MAX, make_list(&[20, 25])),
            ],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        // Ops: Delete(5), Put(15, [12,15])
        // Sentinel is unchanged (and is already sentinel), no promotion needed.
        assert_eq!(plan.ops.len(), 2);
        assert!(matches!(plan.ops[0], ShardOp::Delete(5)));
        match &plan.ops[1] {
            ShardOp::Put(k, list) => {
                assert_eq!(*k, 15);
                assert_eq!(list.iter().collect::<Vec<_>>(), vec![12, 15]);
            }
            _ => panic!("expected Put"),
        }
    }
}
