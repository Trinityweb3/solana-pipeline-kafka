// File: solana_producer/src/block_fetcher.rs

//! Bounded concurrent RPC block poller with exponential backoff and consensus-aware error decoding.
//! 
//! This module decouples the slot discovery layer from the ingestion loop by processing slots 
//! concurrently across multiple tokio task actors using bounded MPSC channels. It explicitly handles
//! Solana-specific ledger gaps (skipped slots, ledger cleanups) via internal JSON-RPC error mapping 
//! to prevent data ingestion pipeline stalls.


use std::sync::Arc;

use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcTransactionDetails},
    rpc_response::RpcError,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedConfirmedBlock, UiTransactionEncoding};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::backoff::backoff_delay;
use crate::shutdown::{is_shutdown, sleep_with_shutdown, ShutdownSignal};
use crate::transfer::{extract_transfers, TransferEvent};

const BLOCK_NOT_AVAILABLE_CODE: i64 = -32004;
const SLOT_SKIPPED_CODE: i64 = -32005;

pub fn build_block_config(commitment: CommitmentConfig) -> RpcBlockConfig {
    RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: Some(RpcTransactionDetails::Full),
        rewards: Some(false),
        commitment: Some(commitment),
        max_supported_transaction_version: Some(0),
    }
}

pub async fn run_fetch_worker(
    worker_id: usize,
    client: Arc<RpcClient>,
    block_config: RpcBlockConfig,
    max_attempts: u32,
    mut slot_rx: mpsc::Receiver<u64>,
    event_tx: mpsc::Sender<TransferEvent>,
    shutdown: ShutdownSignal,
) {
    loop {
        let Some(slot) = slot_rx.recv().await else {
            break;
        };
        if is_shutdown(&shutdown) {
            break;
        }
        match fetch_block(&client, &block_config, slot, max_attempts, &shutdown).await {
            Ok(Some(block)) => {
                let transfers = extract_transfers(&block);
                debug!(worker_id, slot, transfers = transfers.len(), "block parsed");
                for event in transfers {
                    if event_tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
            Ok(None) => debug!(worker_id, slot, "slot skipped by consensus"),
            Err(error) => warn!(
                worker_id,
                slot,
                error = %error,
                "block fetch exhausted retries, slot abandoned"
            ),
        }
    }
    info!(worker_id, "fetch worker stopped");
}

async fn fetch_block(
    client: &RpcClient,
    block_config: &RpcBlockConfig,
    slot: u64,
    max_attempts: u32,
    shutdown: &ShutdownSignal,
) -> Result<Option<EncodedConfirmedBlock>, ClientError> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match client.get_block_with_config(slot, block_config.clone()).await {
            Ok(block) => return Ok(Some(block)),
            Err(error) if is_skipped_slot(&error) => return Ok(None),
            Err(error) if attempt >= max_attempts => return Err(error),
            Err(error) => {
                let delay = backoff_delay(attempt);
                warn!(
                    slot,
                    attempt,
                    max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "transient block fetch failure, retrying"
                );
                if sleep_with_shutdown(delay, shutdown).await {
                    return Err(error);
                }
            }
        }
    }
}

fn is_skipped_slot(error: &ClientError) -> bool {
    match error.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) => {
            code == &BLOCK_NOT_AVAILABLE_CODE
                || code == &SLOT_SKIPPED_CODE
                || message.contains("not available")
                || message.contains("skipped")
        }
        _ => false,
    }
}
