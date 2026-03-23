# Big Block Skipped Validation Analysis

This documents every consensus check in `validate_post_execution_env_switches` (in `crates/engine/tree/src/tree/payload_validator.rs`) and its current status.

## Root Cause

Some checks fail because validation operates on the **merged header** but execution produces **per-segment results**. Receipt cumulative gas counters reset at each segment boundary when `executor.finish()` is called.

## Checks

### 1. `validate_block_pre_execution_with_tx_root` — RESTORED

Re-enabled fully. `validate_cancun_gas` passes because the merged header's `blob_gas_used` is the sum across all constituent blocks, matching the concatenated body transactions. The `max_blob_count` limit (which would reject the summed blob gas) is handled by `--testing.max-blob-count` in `validate_header` (standalone validation), not in `validate_cancun_gas`.

### 2. `validate_header_against_parent` — RESTORED

Re-enabled fully. The big block generator now derives correct `parent_hash`, `block_number`, `base_fee_per_gas`, and `excess_blob_gas` for chained big blocks. The gas limit ramp check is skipped via `--testing.skip-gas-limit-ramp-check`.

### 3. `validate_block_post_execution` — `gas_used` — RESTORED

Re-enabled by checking `output.result.gas_used` directly against `header.gas_used()`, bypassing the receipt-based check (`receipts.last().cumulative_gas_used()`). The accumulated `gas_used` from all segments is correct; only per-receipt cumulative gas counters are wrong.

### 4. `validate_block_post_execution` — `receipts_root` + `logs_bloom` — SKIPPED

**Why skipped:** Receipt cumulative gas counters reset at each segment boundary. The validator's receipts have per-segment cumulative gas, so the computed `receipts_root` doesn't match the header. The header now has the *correct* merged `receipts_root` and `logs_bloom` (computed by the generator from RPC receipts with globally-corrected `cumulative_gas_used`), but the validator can't verify this without also fixing its own receipts.

**What would restore it:** After each `executor.finish()`, fix up the segment's receipts by adding the prior segments' total gas to each receipt's `cumulative_gas_used`. This requires adding a `set_cumulative_gas_used` method to the `TxReceipt` trait (currently only has a getter) or working with the concrete `Receipt` type. Once receipts are globally correct, the standard `calculate_receipt_root` and bloom OR would match the header.

### 5. `validate_block_post_execution` — `requests_hash` (EIP-7685) — SKIPPED

**Why skipped:** Execution layer requests (EIP-7002/7251 system calls) are applied at each `executor.finish()` boundary, so the merged block accumulates requests from all segments. The header's `requests_hash` (from the last constituent block) doesn't match the hash of all accumulated requests.

**What would restore it:** The generator could compute the correct aggregate `requests_hash` by concatenating all constituent blocks' requests (fetchable via RPC `eth_getBlockReceipts` or similar). Then the validator would hash all accumulated requests and compare. Alternatively, store per-segment expected `requests_hash` in `BigBlockData` and validate each segment individually.
