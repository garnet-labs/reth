#![allow(missing_docs, unreachable_pub)]

use alloy_consensus::Header;
use alloy_primitives::{
    map::{FbBuildHasher, HashMap},
    Address, B256, U256,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use reth_chain_state::{ComputedTrieData, ExecutedBlock};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use reth_provider::{
    test_utils::{create_test_provider_factory, MockNodeTypesWithDB},
    SaveBlocksMode, StorageSettings, StorageSettingsCache,
};
use reth_trie::{HashedPostState, KeccakKeyHasher};
use revm_database::BundleState;
use revm_state::AccountInfo;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const ACCOUNTS_PER_BLOCK: usize = 32;
const SLOTS_PER_ACCOUNT: usize = 8;

struct SaveBlocksFixture {
    factory: reth_provider::ProviderFactory<MockNodeTypesWithDB>,
    blocks: Vec<ExecutedBlock>,
}

fn initialize_genesis(factory: &reth_provider::ProviderFactory<MockNodeTypesWithDB>) {
    let genesis = SealedBlock::<reth_ethereum_primitives::Block>::from_sealed_parts(
        SealedHeader::new(
            Header { number: 0, difficulty: U256::from(1), ..Default::default() },
            B256::ZERO,
        ),
        Default::default(),
    );

    let executed_genesis = ExecutedBlock::new(
        Arc::new(genesis.try_recover().unwrap()),
        Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts: vec![],
                requests: Default::default(),
                gas_used: 0,
                blob_gas_used: 0,
            },
            state: Default::default(),
        }),
        ComputedTrieData::default(),
    );

    let provider_rw = factory.provider_rw().unwrap();
    provider_rw.save_blocks(vec![executed_genesis], SaveBlocksMode::Full).unwrap();
    provider_rw.commit().unwrap();
}

fn create_blocks(block_count: u64) -> Vec<ExecutedBlock> {
    let mut parent_hash = B256::ZERO;
    let mut blocks = Vec::with_capacity(block_count as usize);

    for block_num in 1..=block_count {
        let mut builder = BundleState::builder(block_num..=block_num);

        for account_idx in 0..ACCOUNTS_PER_BLOCK {
            let address = Address::with_last_byte((block_num * 10 + account_idx as u64) as u8);
            let info = AccountInfo {
                nonce: block_num,
                balance: U256::from(block_num * 100 + account_idx as u64),
                ..Default::default()
            };

            let storage: HashMap<U256, (U256, U256), FbBuildHasher<32>> = (1..=SLOTS_PER_ACCOUNT
                as u64)
                .map(|slot_idx| {
                    (
                        U256::from(slot_idx + account_idx as u64 * 100),
                        (U256::ZERO, U256::from(block_num * 1000 + slot_idx)),
                    )
                })
                .collect();

            let revert_storage = (1..=SLOTS_PER_ACCOUNT as u64)
                .map(|slot_idx| (U256::from(slot_idx + account_idx as u64 * 100), U256::ZERO))
                .collect::<Vec<_>>();

            builder = builder
                .state_present_account_info(address, info)
                .revert_account_info(block_num, address, Some(None))
                .state_storage(address, storage)
                .revert_storage(block_num, address, revert_storage);
        }

        let bundle = builder.build();
        let hashed_state =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle.state()).into_sorted();

        let block = SealedBlock::<reth_ethereum_primitives::Block>::seal_parts(
            Header {
                number: block_num,
                parent_hash,
                difficulty: U256::from(1),
                ..Default::default()
            },
            Default::default(),
        );
        parent_hash = block.hash();

        blocks.push(ExecutedBlock::new(
            Arc::new(RecoveredBlock::new_sealed(block, Vec::new())),
            Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts: vec![],
                    requests: Default::default(),
                    gas_used: 0,
                    blob_gas_used: 0,
                },
                state: bundle,
            }),
            ComputedTrieData { hashed_state: Arc::new(hashed_state), ..Default::default() },
        ));
    }

    blocks
}

fn setup_fixture(block_count: u64) -> SaveBlocksFixture {
    let factory = create_test_provider_factory();
    factory.set_storage_settings_cache(StorageSettings::v1());
    initialize_genesis(&factory);
    SaveBlocksFixture { factory, blocks: create_blocks(block_count) }
}

fn bench_save_blocks(c: &mut Criterion) {
    let mut group = c.benchmark_group("save_blocks");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for block_count in [1_u64, 3, 8] {
        group.bench_function(BenchmarkId::from_parameter(block_count), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;

                for _ in 0..iters {
                    let SaveBlocksFixture { factory, blocks } = setup_fixture(block_count);
                    let start = Instant::now();
                    let provider_rw = factory.provider_rw().unwrap();
                    provider_rw.save_blocks(blocks, SaveBlocksMode::Full).unwrap();
                    total += start.elapsed();
                }

                total
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_save_blocks);
criterion_main!(benches);
