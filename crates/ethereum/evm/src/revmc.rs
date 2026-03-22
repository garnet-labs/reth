//! revmc JIT compiler integration for EVM execution.
//!
//! Re-exports types from [`revmc::alloy_evm`] and provides [`RevmcEvmFactory`], a newtype that
//! implements [`Debug`].

use alloy_evm::{Database, EvmEnv, EvmFactory};
use revm::{
    context::BlockEnv,
    context_interface::result::{EVMError, HaltReason},
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    Inspector,
};
use revmc::alloy_evm as jit;
use std::sync::{Arc, Mutex};

pub use jit::JitEvm;
pub use revmc::runtime::{
    JitCoordinator, JitCoordinatorHandle, RuntimeConfig, RuntimeStatsSnapshot,
};

/// Newtype around [`revmc::alloy_evm::JitEvmFactory`] that implements [`Debug`].
///
/// Optionally owns the [`JitCoordinator`] to keep it alive for the factory's lifetime.
#[derive(Clone)]
pub struct RevmcEvmFactory {
    inner: jit::JitEvmFactory,
    /// Keeps the coordinator alive. `Mutex` because `JitCoordinator` is `!Sync`.
    _coordinator: Arc<Mutex<JitCoordinator>>,
}

impl core::fmt::Debug for RevmcEvmFactory {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RevmcEvmFactory").finish_non_exhaustive()
    }
}

impl RevmcEvmFactory {
    /// Creates a new factory that owns the coordinator.
    pub fn new(coordinator: JitCoordinator) -> Self {
        let handle = coordinator.handle();
        Self {
            inner: jit::JitEvmFactory::new(handle),
            _coordinator: Arc::new(Mutex::new(coordinator)),
        }
    }

    /// Creates a [`RevmcEvmFactory`] with JIT disabled.
    ///
    /// Starts a coordinator with `enabled: false` so lookups always return `Interpret`.
    pub fn disabled() -> Self {
        let coordinator = JitCoordinator::start(RuntimeConfig::default())
            .expect("failed to start disabled revmc runtime");
        Self::new(coordinator)
    }
}

impl EvmFactory for RevmcEvmFactory {
    type Evm<DB: Database, I: Inspector<alloy_evm::eth::EthEvmContext<DB>>> =
        <jit::JitEvmFactory as EvmFactory>::Evm<DB, I>;
    type Context<DB: Database> = <jit::JitEvmFactory as EvmFactory>::Context<DB>;
    type Tx = <jit::JitEvmFactory as EvmFactory>::Tx;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = <jit::JitEvmFactory as EvmFactory>::Precompiles;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        self.inner.create_evm(db, input)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        self.inner.create_evm_with_inspector(db, input, inspector)
    }
}

/// Records revmc JIT runtime stats as Prometheus metrics.
pub fn record_revmc_metrics(stats: &RuntimeStatsSnapshot) {
    metrics::gauge!("revmc_jit_lookup_hits").set(stats.lookup_hits as f64);
    metrics::gauge!("revmc_jit_lookup_misses").set(stats.lookup_misses as f64);
    metrics::gauge!("revmc_jit_events_sent").set(stats.events_sent as f64);
    metrics::gauge!("revmc_jit_events_dropped").set(stats.events_dropped as f64);
    metrics::gauge!("revmc_jit_resident_entries").set(stats.resident_entries as f64);
}
