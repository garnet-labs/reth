#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

#[cfg(all(feature = "jemalloc-prof", unix))]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use clap::Parser;
use reth::cli::Cli;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
#[cfg(feature = "revmc")]
use reth_node_ethereum::EthereumAddOns;
use reth_node_ethereum::EthereumNode;
use tracing::info;

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) = Cli::<EthereumChainSpecParser>::parse().run(async move |builder, _| {
        info!(target: "reth::cli", "Launching node");

        #[cfg(not(feature = "revmc"))]
        let handle = builder.node(EthereumNode::default()).launch_with_debug_capabilities().await?;

        #[cfg(feature = "revmc")]
        let handle = builder
            .with_types::<EthereumNode>()
            .with_components(
                EthereumNode::components()
                    .executor(reth_node_ethereum::RevmcExecutorBuilder::default()),
            )
            .with_add_ons(EthereumAddOns::default())
            .launch_with_debug_capabilities()
            .await?;

        handle.wait_for_node_exit().await
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
