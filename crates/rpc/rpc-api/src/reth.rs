use alloy_eips::BlockId;
use alloy_primitives::{map::AddressMap, Bytes, B256, U256, U64};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_trie_common::Nibbles;

// Required for the subscription attributes below
use reth_chain_state as _;

/// Response for `reth_getReceiptWithProof`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptWithProofResponse {
    /// The receipt encoded as EIP-2718.
    pub receipt: Bytes,
    /// The Merkle proof: an array of RLP-encoded trie nodes from the receipts root to the leaf.
    pub proof: Vec<Bytes>,
    /// The receipts trie root from the block header.
    pub receipts_root: B256,
    /// The block hash.
    pub block_hash: B256,
    /// The transaction index within the block.
    pub tx_index: u64,
}

impl ReceiptWithProofResponse {
    /// Verifies the Merkle proof against the `receipts_root`.
    ///
    /// Returns `Ok(())` if the proof is valid, proving that the receipt is included
    /// in the block's receipts trie at position `tx_index`.
    pub fn verify(&self) -> Result<(), alloy_trie::proof::ProofVerificationError> {
        let key = alloy_rlp::encode_fixed_size(&(self.tx_index as usize));
        alloy_trie::proof::verify_proof(
            self.receipts_root,
            Nibbles::unpack(&key),
            Some(self.receipt.to_vec()),
            &self.proof,
        )
    }
}

/// Reth API namespace for reth-specific methods
#[cfg_attr(not(feature = "client"), rpc(server, namespace = "reth"))]
#[cfg_attr(feature = "client", rpc(server, client, namespace = "reth"))]
pub trait RethApi {
    /// Returns the receipt for a transaction along with its Merkle proof against the block's
    /// receipts root.
    ///
    /// The proof allows verifying that a specific receipt (and its logs) is included in a block
    /// without downloading all receipts for that block.
    #[method(name = "getReceiptWithProof")]
    async fn reth_get_receipt_with_proof(
        &self,
        tx_hash: B256,
    ) -> RpcResult<Option<ReceiptWithProofResponse>>;

    /// Returns all ETH balance changes in a block
    #[method(name = "getBalanceChangesInBlock")]
    async fn reth_get_balance_changes_in_block(
        &self,
        block_id: BlockId,
    ) -> RpcResult<AddressMap<U256>>;

    /// Re-executes a block (or a range of blocks) and returns the execution outcome including
    /// receipts, state changes, and EIP-7685 requests.
    ///
    /// If `count` is provided, re-executes `count` consecutive blocks starting from `block_id`
    /// and returns the merged execution outcome.
    #[method(name = "getBlockExecutionOutcome")]
    async fn reth_get_block_execution_outcome(
        &self,
        block_id: BlockId,
        count: Option<U64>,
    ) -> RpcResult<Option<serde_json::Value>>;

    /// Subscribe to json `ChainNotifications`
    #[subscription(
        name = "subscribeChainNotifications",
        unsubscribe = "unsubscribeChainNotifications",
        item = reth_chain_state::CanonStateNotification
    )]
    async fn reth_subscribe_chain_notifications(&self) -> jsonrpsee::core::SubscriptionResult;

    /// Subscribe to persisted block notifications.
    ///
    /// Emits a notification with the block number and hash when a new block is persisted to disk.
    #[subscription(
        name = "subscribePersistedBlock",
        unsubscribe = "unsubscribePersistedBlock",
        item = alloy_eips::BlockNumHash
    )]
    async fn reth_subscribe_persisted_block(&self) -> jsonrpsee::core::SubscriptionResult;

    /// Subscribe to finalized chain notifications.
    ///
    /// Buffers committed chain notifications and emits them once a new finalized block is received.
    /// Each notification contains all committed chain segments up to the finalized block.
    #[subscription(
        name = "subscribeFinalizedChainNotifications",
        unsubscribe = "unsubscribeFinalizedChainNotifications",
        item = Vec<reth_chain_state::CanonStateNotification>
    )]
    async fn reth_subscribe_finalized_chain_notifications(
        &self,
    ) -> jsonrpsee::core::SubscriptionResult;
}
