//! Command for replaying a reorg between two existing block hashes.
//!
//! This command takes two block hashes (competing chain tips), discovers their
//! common ancestor, fetches the payloads for both forks, and replays them
//! through the Engine API to trigger a reorg.
//!
//! The default flow is:
//! 1. **Phase 1**: Send fork A payloads and make them canonical via FCU.
//! 2. **Phase 2**: Send fork B payloads (imported as side-chain blocks).
//! 3. **Phase 3**: Send a single FCU switching the head to fork B (triggers the reorg).
//!
//! Use `--order b-then-a` to reverse which fork is built first.
//!
//! Blocks can be sourced from:
//! - An EL RPC endpoint (`--rpc-url`)
//! - Pre-downloaded JSON files (`--payload-a-file`, `--payload-b-file`)
//! - A Dora beacon explorer (`--dora-url` + `--beacon-root-a` / `--beacon-root-b`)
//!
//! File inputs accept both `cast block --full --json` (RPC block) and Dora
//! `?download=block-json` (beacon block) formats.

use crate::{
    authenticated_transport::AuthenticatedTransportConnect,
    valid_payload::{
        block_to_new_payload, call_forkchoice_updated_with_reth, call_new_payload_with_reth,
        payload_to_new_payload,
    },
};
use alloy_eips::eip4844::kzg_to_versioned_hash;
use alloy_primitives::{hex, B256};
use alloy_provider::{
    network::{AnyNetwork, AnyRpcBlock},
    Provider, RootProvider,
};
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionPayload, ExecutionPayloadSidecar, ForkchoiceState, JwtSecret,
};
use alloy_transport::layers::{RateLimitRetryPolicy, RetryBackoffLayer};
use clap::{Parser, ValueEnum};
use eyre::Context;
use reth_cli_runner::CliContext;
use reth_node_api::EngineApiMessageVersion;
use serde::Deserialize;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::info;
use url::Url;

/// Maximum ancestor search depth to prevent unbounded RPC walks.
const DEFAULT_MAX_DEPTH: u64 = 4096;

/// `reth bench replay-reorg` command
///
/// Replays a reorg between two block hashes by discovering the common ancestor,
/// building one fork as canonical, then importing the competing fork and switching
/// the head via forkchoiceUpdated.
///
/// Example (RPC source):
///
/// `reth-bench replay-reorg --rpc-url http://localhost:8545 --engine-rpc-url
/// http://localhost:8551 --jwt-secret ~/.local/share/reth/mainnet/jwt.hex
/// --tip-a 0xaaa... --tip-b 0xbbb...`
///
/// Example (file source for orphaned blocks):
///
/// `reth-bench replay-reorg --rpc-url http://localhost:8545 --engine-rpc-url
/// http://localhost:8551 --jwt-secret ~/.local/share/reth/mainnet/jwt.hex
/// --payload-a-file orphaned.json --payload-b-file canonical.json`
#[derive(Debug, Parser)]
pub struct Command {
    /// Block hash of the first fork tip (initially made canonical).
    /// Required when fetching from RPC; optional when using --payload-a-file.
    #[arg(long, value_name = "HASH")]
    tip_a: Option<B256>,

    /// Block hash of the second fork tip (triggers the reorg).
    /// Required when fetching from RPC; optional when using --payload-b-file.
    #[arg(long, value_name = "HASH")]
    tip_b: Option<B256>,

    /// Which fork to make canonical first.
    #[arg(long, value_enum, default_value = "a-then-b")]
    order: ReorgOrder,

    /// Whether to send an FCU after every newPayload in the canonical phase,
    /// or only once at the tip.
    #[arg(long, value_enum, default_value = "tip-only")]
    fcu_mode: FcuMode,

    /// Maximum number of blocks to walk back when searching for the common ancestor.
    #[arg(long, default_value_t = DEFAULT_MAX_DEPTH)]
    max_depth: u64,

    /// The RPC URL to use for fetching block data.
    /// Required unless both --payload-a-file and --payload-b-file are provided.
    #[arg(long, value_name = "RPC_URL")]
    rpc_url: Option<String>,

    /// The engine RPC URL (with JWT authentication).
    #[arg(long, value_name = "ENGINE_RPC_URL", default_value = "http://localhost:8551")]
    engine_rpc_url: String,

    /// Path to the JWT secret file for engine API authentication.
    #[arg(long, value_name = "JWT_SECRET")]
    jwt_secret: String,

    /// Use `reth_newPayload` endpoint instead of `engine_newPayload*`.
    #[arg(long, default_value = "false")]
    reth_new_payload: bool,

    /// Path to a pre-downloaded block JSON file for tip A.
    ///
    /// Accepts either an RPC block (`cast block --full --json`) or a beacon
    /// block from Dora (`?download=block-json`).
    #[arg(long, value_name = "PATH")]
    payload_a_file: Option<PathBuf>,

    /// Path to a pre-downloaded block JSON file for tip B.
    ///
    /// Accepts either an RPC block (`cast block --full --json`) or a beacon
    /// block from Dora (`?download=block-json`).
    #[arg(long, value_name = "PATH")]
    payload_b_file: Option<PathBuf>,

    /// Base URL of a Dora beacon explorer for fetching orphaned blocks.
    ///
    /// Used as fallback when a block is not found via --rpc-url.
    /// Requires the corresponding --beacon-root-a / --beacon-root-b.
    /// Example: <https://light-mainnet.beaconcha.in>
    #[arg(long, value_name = "URL")]
    dora_url: Option<String>,

    /// Beacon block root for tip A (used with --dora-url).
    #[arg(long, value_name = "HASH")]
    beacon_root_a: Option<B256>,

    /// Beacon block root for tip B (used with --dora-url).
    #[arg(long, value_name = "HASH")]
    beacon_root_b: Option<B256>,
}

/// Which fork to build as canonical first.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(super) enum ReorgOrder {
    /// Build fork A canonical first, then reorg to fork B.
    #[value(name = "a-then-b")]
    AThenB,
    /// Build fork B canonical first, then reorg to fork A.
    #[value(name = "b-then-a")]
    BThenA,
}

/// FCU strategy during the canonical build phase.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(super) enum FcuMode {
    /// Send a single FCU at the tip of the canonical fork.
    #[value(name = "tip-only")]
    TipOnly,
    /// Send an FCU after every newPayload in the canonical phase.
    #[value(name = "per-block")]
    PerBlock,
}

/// A fetched block with its hash and number.
struct FetchedBlock {
    /// The RPC block (when fetched from an EL RPC).
    block: Option<AnyRpcBlock>,
    hash: B256,
    number: u64,
    parent_hash: B256,
    /// Pre-computed engine API params for blocks loaded from beacon JSON or file.
    precomputed_payload: Option<(Option<EngineApiMessageVersion>, serde_json::Value)>,
}

impl Command {
    /// Execute the `replay-reorg` command.
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        info!(
            target: "reth-bench",
            tip_a = ?self.tip_a,
            tip_b = ?self.tip_b,
            payload_a_file = ?self.payload_a_file,
            payload_b_file = ?self.payload_b_file,
            dora_url = ?self.dora_url,
            ?self.order,
            ?self.fcu_mode,
            "Starting reorg replay"
        );

        // Set up optional block provider (RPC for fetching block data)
        let block_provider = if let Some(ref rpc_url) = self.rpc_url {
            let retry_policy = RateLimitRetryPolicy::default().or(
                |err: &alloy_transport::TransportError| -> bool {
                    err.as_transport_err()
                        .and_then(|t| t.as_http_error())
                        .is_some_and(|e| e.status == 502)
                },
            );
            let client = ClientBuilder::default()
                .layer(RetryBackoffLayer::new_with_policy(10, 800, u64::MAX, retry_policy))
                .http(rpc_url.parse()?);
            Some(RootProvider::<AnyNetwork>::new(client))
        } else {
            None
        };

        // Set up authenticated engine provider
        let jwt =
            std::fs::read_to_string(&self.jwt_secret).wrap_err("Failed to read JWT secret file")?;
        let jwt = JwtSecret::from_hex(jwt.trim())?;
        let auth_url = Url::parse(&self.engine_rpc_url)?;
        info!(target: "reth-bench", "Connecting to Engine RPC at {}", auth_url);
        let auth_transport = AuthenticatedTransportConnect::new(auth_url, jwt);
        let auth_client = ClientBuilder::default().connect_with(auth_transport).await?;
        let auth_provider = RootProvider::<AnyNetwork>::new(auth_client);

        // Detect optimism by checking predeploy code (need RPC for this)
        let is_optimism = if let Some(ref provider) = block_provider {
            !provider
                .get_code_at(alloy_primitives::address!(
                    "0x420000000000000000000000000000000000000F"
                ))
                .await?
                .is_empty()
        } else {
            false
        };

        // Resolve blocks for each tip from the configured sources
        let block_a = self
            .resolve_block(
                "A",
                self.tip_a,
                &self.payload_a_file,
                self.beacon_root_a,
                &block_provider,
                is_optimism,
            )
            .await?;
        let block_b = self
            .resolve_block(
                "B",
                self.tip_b,
                &self.payload_b_file,
                self.beacon_root_b,
                &block_provider,
                is_optimism,
            )
            .await?;

        info!(
            target: "reth-bench",
            tip_a_hash = %block_a.hash,
            tip_a_number = block_a.number,
            tip_b_hash = %block_b.hash,
            tip_b_number = block_b.number,
            "Resolved both tips"
        );

        // Build fork segments
        let (ancestor, fork_a_blocks, fork_b_blocks) =
            self.build_fork_segments(block_a, block_b, &block_provider).await?;

        info!(
            target: "reth-bench",
            ancestor_hash = %ancestor.hash,
            ancestor_number = ancestor.number,
            fork_a_length = fork_a_blocks.len(),
            fork_b_length = fork_b_blocks.len(),
            "Discovered common ancestor"
        );

        // Determine order
        let (canonical_blocks, competing_blocks, canonical_label, competing_label) =
            match self.order {
                ReorgOrder::AThenB => (fork_a_blocks, fork_b_blocks, "A", "B"),
                ReorgOrder::BThenA => (fork_b_blocks, fork_a_blocks, "B", "A"),
            };

        let canonical_tip = canonical_blocks.last().map(|b| b.hash).unwrap_or(ancestor.hash);
        let competing_tip = competing_blocks.last().map(|b| b.hash).unwrap_or(ancestor.hash);

        // Phase 1: Build canonical fork
        info!(
            target: "reth-bench",
            fork = canonical_label,
            blocks = canonical_blocks.len(),
            "Phase 1: Building canonical fork"
        );

        let phase1_start = Instant::now();
        let mut parent_hash = ancestor.hash;

        for (i, fetched) in canonical_blocks.iter().enumerate() {
            let (version, params) = fetched.engine_params(is_optimism, self.reth_new_payload)?;
            let start = Instant::now();
            call_new_payload_with_reth(&auth_provider, version, params).await?;
            let np_latency = start.elapsed();

            info!(
                target: "reth-bench",
                phase = 1,
                fork = canonical_label,
                progress = format_args!("{}/{}", i + 1, canonical_blocks.len()),
                block_number = fetched.number,
                block_hash = %fetched.hash,
                new_payload_latency = ?np_latency,
                "Sent newPayload"
            );

            // Send FCU per block if configured
            if matches!(self.fcu_mode, FcuMode::PerBlock) {
                let fcu_state = ForkchoiceState {
                    head_block_hash: fetched.hash,
                    safe_block_hash: parent_hash,
                    finalized_block_hash: ancestor.hash,
                };
                let fcu_start = Instant::now();
                call_forkchoice_updated_with_reth(&auth_provider, version, fcu_state).await?;
                info!(
                    target: "reth-bench",
                    fcu_latency = ?fcu_start.elapsed(),
                    "Sent per-block FCU"
                );
            }

            parent_hash = fetched.hash;
        }

        // Send canonical tip FCU
        let canonical_fcu_state = ForkchoiceState {
            head_block_hash: canonical_tip,
            safe_block_hash: ancestor.hash,
            finalized_block_hash: ancestor.hash,
        };

        let canonical_version = canonical_blocks
            .last()
            .map(|last| last.engine_params(is_optimism, self.reth_new_payload).map(|(v, _)| v))
            .transpose()?
            .flatten();

        let fcu_start = Instant::now();
        call_forkchoice_updated_with_reth(&auth_provider, canonical_version, canonical_fcu_state)
            .await?;
        let phase1_fcu_latency = fcu_start.elapsed();
        let phase1_total = phase1_start.elapsed();

        info!(
            target: "reth-bench",
            fork = canonical_label,
            canonical_tip = %canonical_tip,
            fcu_latency = ?phase1_fcu_latency,
            total_phase_time = ?phase1_total,
            "Phase 1 complete: canonical fork built"
        );

        // Phase 2: Import competing fork blocks (newPayload only, no FCU)
        info!(
            target: "reth-bench",
            fork = competing_label,
            blocks = competing_blocks.len(),
            "Phase 2: Importing competing fork blocks"
        );

        let phase2_start = Instant::now();
        let mut phase2_np_latencies = Vec::with_capacity(competing_blocks.len());

        for (i, fetched) in competing_blocks.iter().enumerate() {
            let (version, params) = fetched.engine_params(is_optimism, self.reth_new_payload)?;
            let start = Instant::now();
            call_new_payload_with_reth(&auth_provider, version, params).await?;
            let np_latency = start.elapsed();
            phase2_np_latencies.push(np_latency);

            info!(
                target: "reth-bench",
                phase = 2,
                fork = competing_label,
                progress = format_args!("{}/{}", i + 1, competing_blocks.len()),
                block_number = fetched.number,
                block_hash = %fetched.hash,
                new_payload_latency = ?np_latency,
                "Sent newPayload (side chain)"
            );
        }

        let phase2_total = phase2_start.elapsed();
        info!(
            target: "reth-bench",
            fork = competing_label,
            total_phase_time = ?phase2_total,
            "Phase 2 complete: competing fork imported"
        );

        // Phase 3: Trigger reorg via FCU to competing tip
        info!(
            target: "reth-bench",
            competing_tip = %competing_tip,
            "Phase 3: Triggering reorg"
        );

        let reorg_fcu_state = ForkchoiceState {
            head_block_hash: competing_tip,
            safe_block_hash: ancestor.hash,
            finalized_block_hash: ancestor.hash,
        };

        let competing_version = competing_blocks
            .last()
            .map(|last| last.engine_params(is_optimism, self.reth_new_payload).map(|(v, _)| v))
            .transpose()?
            .flatten();

        let reorg_fcu_start = Instant::now();
        call_forkchoice_updated_with_reth(&auth_provider, competing_version, reorg_fcu_state)
            .await?;
        let reorg_fcu_latency = reorg_fcu_start.elapsed();

        // Summary
        let phase2_np_total: Duration = phase2_np_latencies.iter().sum();

        info!(
            target: "reth-bench",
            ancestor_number = ancestor.number,
            ancestor_hash = %ancestor.hash,
            canonical_fork = canonical_label,
            canonical_fork_length = canonical_blocks.len(),
            competing_fork = competing_label,
            competing_fork_length = competing_blocks.len(),
            phase1_canonical_build = ?phase1_total,
            phase2_competing_import = ?phase2_total,
            phase2_np_total = ?phase2_np_total,
            reorg_fcu_latency = ?reorg_fcu_latency,
            total_time = ?(phase1_total + phase2_total + reorg_fcu_latency),
            "Reorg replay complete"
        );

        Ok(())
    }

    /// Resolve a block from the configured source: file, Dora, or RPC.
    async fn resolve_block(
        &self,
        label: &str,
        tip_hash: Option<B256>,
        payload_file: &Option<PathBuf>,
        beacon_root: Option<B256>,
        block_provider: &Option<RootProvider<AnyNetwork>>,
        is_optimism: bool,
    ) -> eyre::Result<FetchedBlock> {
        // Source 1: Pre-downloaded file
        if let Some(path) = payload_file {
            info!(target: "reth-bench", fork = label, path = %path.display(), "Loading block from file");
            let json_str = reth_fs_util::read_to_string(path)
                .wrap_err_with(|| format!("Failed to read payload file: {}", path.display()))?;
            return load_block_from_json(&json_str, is_optimism);
        }

        // Source 2: Dora beacon explorer
        if let (Some(dora_url), Some(root)) = (&self.dora_url, beacon_root) {
            info!(
                target: "reth-bench",
                fork = label,
                dora_url = %dora_url,
                beacon_root = %root,
                "Fetching block from Dora"
            );
            return fetch_from_dora(dora_url, root, is_optimism).await;
        }

        // Source 3: EL RPC
        let hash = tip_hash.ok_or_else(|| {
            eyre::eyre!(
                "No source for tip {label}: provide --tip-{l} (with --rpc-url), \
                 --payload-{l}-file, or --dora-url with --beacon-root-{l}",
                l = label.to_lowercase()
            )
        })?;

        let provider = block_provider.as_ref().ok_or_else(|| {
            eyre::eyre!("--rpc-url is required when fetching tip {label} from RPC")
        })?;

        info!(target: "reth-bench", fork = label, hash = %hash, "Fetching block from RPC");
        let block = provider.get_block_by_hash(hash).full().await?.ok_or_else(|| {
            let mut msg = format!("Block not found via RPC: {hash}");
            if self.dora_url.is_some() {
                msg.push_str(" (tip: provide --beacon-root-{} to fetch from Dora)");
            } else {
                msg.push_str(
                    " (tip: the block may be orphaned — use --payload-{}-file with a \
                         Dora download, or --dora-url with --beacon-root-{})",
                );
            }
            eyre::eyre!(msg)
        })?;

        let number = block.header.number;
        let parent_hash = block.header.parent_hash;
        Ok(FetchedBlock {
            block: Some(block),
            hash,
            number,
            parent_hash,
            precomputed_payload: None,
        })
    }

    /// Build fork segments from two resolved tips.
    ///
    /// When both blocks share the same parent (typical 1-block reorg), the ancestor
    /// is fetched directly. Otherwise, falls back to walking both chains via RPC.
    async fn build_fork_segments(
        &self,
        block_a: FetchedBlock,
        block_b: FetchedBlock,
        block_provider: &Option<RootProvider<AnyNetwork>>,
    ) -> eyre::Result<(FetchedBlock, Vec<FetchedBlock>, Vec<FetchedBlock>)> {
        // Fast path: both blocks share the same parent (1-block reorg)
        if block_a.parent_hash == block_b.parent_hash {
            let provider = block_provider.as_ref().ok_or_else(|| {
                eyre::eyre!("--rpc-url is required to fetch the common ancestor block")
            })?;

            let ancestor_hash = block_a.parent_hash;
            let ancestor_block = provider
                .get_block_by_hash(ancestor_hash)
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Common ancestor not found: {ancestor_hash}"))?;

            let ancestor = FetchedBlock {
                number: ancestor_block.header.number,
                parent_hash: ancestor_block.header.parent_hash,
                block: Some(ancestor_block),
                hash: ancestor_hash,
                precomputed_payload: None,
            };

            return Ok((ancestor, vec![block_a], vec![block_b]));
        }

        // General case: walk both tips back to find common ancestor via RPC
        let provider = block_provider.as_ref().ok_or_else(|| {
            eyre::eyre!(
                "--rpc-url is required when tips have different parents \
                 (need to walk back to find common ancestor)"
            )
        })?;

        self.find_forks(provider, block_a, block_b).await
    }

    /// Walk both tips back to their common ancestor and return the ancestor
    /// block plus the two fork segments in chronological order (ancestor-child → tip).
    async fn find_forks(
        &self,
        provider: &RootProvider<AnyNetwork>,
        initial_a: FetchedBlock,
        initial_b: FetchedBlock,
    ) -> eyre::Result<(FetchedBlock, Vec<FetchedBlock>, Vec<FetchedBlock>)> {
        let fetch_block = |hash: B256| async move {
            let block = provider
                .get_block_by_hash(hash)
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Block not found: {hash}"))?;
            let number = block.header.number;
            let parent_hash = block.header.parent_hash;
            Ok::<_, eyre::Error>(FetchedBlock {
                block: Some(block),
                hash,
                number,
                parent_hash,
                precomputed_payload: None,
            })
        };

        let mut a = initial_a;
        let mut b = initial_b;

        let mut fork_a_blocks = vec![];
        let mut fork_b_blocks = vec![];
        let mut depth = 0u64;

        // Align heights: walk the higher tip down
        while a.number > b.number {
            if depth >= self.max_depth {
                return Err(eyre::eyre!(
                    "Exceeded max depth ({}) while aligning heights",
                    self.max_depth
                ));
            }
            let parent = a.parent_hash;
            fork_a_blocks.push(a);
            a = fetch_block(parent).await?;
            depth += 1;
        }

        while b.number > a.number {
            if depth >= self.max_depth {
                return Err(eyre::eyre!(
                    "Exceeded max depth ({}) while aligning heights",
                    self.max_depth
                ));
            }
            let parent = b.parent_hash;
            fork_b_blocks.push(b);
            b = fetch_block(parent).await?;
            depth += 1;
        }

        // Walk both back together until hashes match
        while a.hash != b.hash {
            if depth >= self.max_depth {
                return Err(eyre::eyre!(
                    "Exceeded max depth ({}) while searching for common ancestor",
                    self.max_depth
                ));
            }
            let a_parent = a.parent_hash;
            let b_parent = b.parent_hash;
            fork_a_blocks.push(a);
            fork_b_blocks.push(b);
            let (new_a, new_b) = tokio::try_join!(fetch_block(a_parent), fetch_block(b_parent))?;
            a = new_a;
            b = new_b;
            depth += 2;
        }

        // `a` and `b` are the same block (the ancestor).
        // Reverse fork segments to get chronological order (ancestor-child → tip).
        fork_a_blocks.reverse();
        fork_b_blocks.reverse();

        Ok((a, fork_a_blocks, fork_b_blocks))
    }
}

impl FetchedBlock {
    /// Get engine API params, either from pre-computed payload or by converting the RPC block.
    fn engine_params(
        &self,
        is_optimism: bool,
        reth_new_payload: bool,
    ) -> eyre::Result<(Option<EngineApiMessageVersion>, serde_json::Value)> {
        if let Some((version, params)) = &self.precomputed_payload {
            return Ok((*version, params.clone()));
        }

        let block = self
            .block
            .clone()
            .ok_or_else(|| eyre::eyre!("No block data available for {}", self.hash))?;

        block_to_new_payload(block, is_optimism, None, reth_new_payload)
    }
}

// ---------------------------------------------------------------------------
// Block loading from JSON files and Dora
// ---------------------------------------------------------------------------

/// Load a block from a JSON string, auto-detecting the format.
///
/// Supports:
/// - RPC block format (`cast block --full --json` / `eth_getBlockByHash`)
/// - Beacon block format (Dora `?download=block-json`)
fn load_block_from_json(json_str: &str, is_optimism: bool) -> eyre::Result<FetchedBlock> {
    let value: serde_json::Value =
        serde_json::from_str(json_str).wrap_err("Failed to parse JSON")?;

    // Beacon block format has a top-level "version" field
    if value.get("version").is_some() && value.get("data").is_some() {
        return parse_beacon_block_json(&value, is_optimism);
    }

    // Otherwise, try RPC block format
    let block: AnyRpcBlock =
        serde_json::from_value(value).wrap_err("Failed to parse as RPC block JSON")?;
    let hash = block.header.hash;
    let number = block.header.number;
    let parent_hash = block.header.parent_hash;

    Ok(FetchedBlock { block: Some(block), hash, number, parent_hash, precomputed_payload: None })
}

/// Fetch a block from a Dora beacon explorer instance.
async fn fetch_from_dora(
    base_url: &str,
    beacon_root: B256,
    is_optimism: bool,
) -> eyre::Result<FetchedBlock> {
    let url =
        format!("{}/slot/0x{:x}?download=block-json", base_url.trim_end_matches('/'), beacon_root);

    info!(target: "reth-bench", url = %url, "Downloading beacon block from Dora");

    let client = reqwest::Client::builder()
        .user_agent("reth-bench/1.0")
        .timeout(Duration::from_secs(30))
        .build()?;

    let resp = client.get(&url).send().await.wrap_err("Dora request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if body.contains("security") || body.contains("challenge") {
            return Err(eyre::eyre!(
                "Dora returned {status} (Cloudflare challenge). \
                 Download the block manually in a browser:\n  {url}\n\
                 Then pass it via --payload-a-file or --payload-b-file"
            ));
        }
        return Err(eyre::eyre!("Dora returned {status}: {body}"));
    }

    let json_str = resp.text().await?;
    load_block_from_json(&json_str, is_optimism)
        .wrap_err("Failed to parse Dora beacon block response")
}

// ---------------------------------------------------------------------------
// Beacon block JSON parsing
// ---------------------------------------------------------------------------

/// Minimal beacon block JSON structure for extracting the execution payload.
#[derive(Deserialize)]
struct BeaconBlockEnvelope {
    version: String,
    data: BeaconBlockSigned,
}

#[derive(Deserialize)]
struct BeaconBlockSigned {
    message: BeaconBlockMessage,
}

#[derive(Deserialize)]
struct BeaconBlockMessage {
    body: BeaconBlockBody,
}

#[derive(Deserialize)]
struct BeaconBlockBody {
    execution_payload: serde_json::Value,
    #[serde(default)]
    blob_kzg_commitments: Option<Vec<String>>,
}

/// Parse a beacon block JSON (from Dora `?download=block-json`) into a `FetchedBlock`.
///
/// Extracts the execution payload, converts numeric fields from decimal to hex
/// (beacon API format → engine API format), and builds the sidecar.
fn parse_beacon_block_json(
    value: &serde_json::Value,
    is_optimism: bool,
) -> eyre::Result<FetchedBlock> {
    let envelope: BeaconBlockEnvelope =
        serde_json::from_value(value.clone()).wrap_err("Failed to parse beacon block envelope")?;

    // Extract parent_beacon_block_root from the beacon block's parent_root
    let parent_beacon_root: Option<B256> = value
        .pointer("/data/message/parent_root")
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    // Convert the execution payload from beacon API format (decimal strings)
    // to engine API format (hex strings) so alloy can deserialize it.
    let engine_payload_json =
        beacon_to_engine_payload(&envelope.data.message.body.execution_payload);

    // Deserialize as ExecutionPayload
    let payload: ExecutionPayload = serde_json::from_value(engine_payload_json)
        .wrap_err("Failed to deserialize execution payload from beacon block")?;

    let block_hash = payload.block_hash();
    let block_number = payload.block_number();
    let parent_hash = payload.parent_hash();

    // Build sidecar from blob KZG commitments
    let sidecar = build_sidecar(
        &envelope.version,
        parent_beacon_root,
        envelope.data.message.body.blob_kzg_commitments.as_deref(),
    );

    // Compute engine API params
    let (version, params, _) = payload_to_new_payload(
        payload,
        sidecar,
        is_optimism,
        None, // withdrawals_root not available from beacon block
        None,
    )?;

    Ok(FetchedBlock {
        block: None,
        hash: block_hash,
        number: block_number,
        parent_hash,
        precomputed_payload: Some((Some(version), params)),
    })
}

/// Build an `ExecutionPayloadSidecar` from beacon block data.
fn build_sidecar(
    version: &str,
    parent_beacon_root: Option<B256>,
    blob_kzg_commitments: Option<&[String]>,
) -> ExecutionPayloadSidecar {
    let needs_cancun = matches!(version, "deneb" | "electra" | "fulu");

    if needs_cancun {
        let versioned_hashes = blob_kzg_commitments
            .unwrap_or_default()
            .iter()
            .filter_map(|hex_str| {
                let bytes = hex::decode(hex_str.strip_prefix("0x").unwrap_or(hex_str)).ok()?;
                Some(kzg_to_versioned_hash(&bytes))
            })
            .collect();

        ExecutionPayloadSidecar::v3(CancunPayloadFields {
            parent_beacon_block_root: parent_beacon_root.unwrap_or_default(),
            versioned_hashes,
        })
    } else {
        ExecutionPayloadSidecar::none()
    }
}

/// Convert a beacon API execution payload JSON to engine API format.
///
/// The beacon API uses decimal strings for numeric quantities, while the
/// engine API uses hex-encoded `"0x..."` strings. Field names are the same
/// (`snake_case`) in both formats.
fn beacon_to_engine_payload(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (key, val) in map {
                let new_key = if key == "validator_index" {
                    // alloy's Withdrawal type uses camelCase for this field
                    "validatorIndex".to_string()
                } else {
                    key.clone()
                };

                let new_val = if is_quantity_field(key) {
                    decimal_to_hex(val)
                } else if key == "withdrawals" || key == "deposit_requests" {
                    // Recurse into arrays of objects
                    if let serde_json::Value::Array(arr) = val {
                        serde_json::Value::Array(arr.iter().map(beacon_to_engine_payload).collect())
                    } else {
                        val.clone()
                    }
                } else {
                    val.clone()
                };

                new_map.insert(new_key, new_val);
            }
            serde_json::Value::Object(new_map)
        }
        other => other.clone(),
    }
}

/// Fields that use `alloy_serde::quantity` (hex-encoded u64/U256).
fn is_quantity_field(field: &str) -> bool {
    matches!(
        field,
        "block_number" |
            "gas_limit" |
            "gas_used" |
            "timestamp" |
            "blob_gas_used" |
            "excess_blob_gas" |
            "base_fee_per_gas" |
            "index" |
            "validator_index" |
            "amount" |
            "deposit_count"
    )
}

/// Convert a decimal string JSON value to a hex `"0x..."` string.
/// Passes through values that are already hex or not strings.
fn decimal_to_hex(val: &serde_json::Value) -> serde_json::Value {
    if let Some(s) = val.as_str() {
        // Already hex-encoded
        if s.starts_with("0x") || s.starts_with("0X") {
            return val.clone();
        }
        // Try parsing as u128 first (covers u64 and base_fee_per_gas)
        if let Ok(n) = s.parse::<u128>() {
            return serde_json::Value::String(format!("0x{n:x}"));
        }
    }
    val.clone()
}
