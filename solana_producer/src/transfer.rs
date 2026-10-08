use std::str::FromStr;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::Serialize;
use solana_sdk::{pubkey::Pubkey, system_program, transaction::VersionedTransaction};
use solana_transaction_status::{
    EncodedConfirmedBlock, EncodedTransaction, UiTransactionStatusMeta,
};

const TRANSFER_DISCRIMINANT: [u8; 4] = [2, 0, 0, 0];
const TRANSFER_DATA_LEN: usize = 12;
const TRANSFER_ACCOUNT_COUNT: usize = 2;

#[derive(Debug, Clone, Serialize)]
pub struct TransferEvent {
    pub source_account: String,
    pub lamports: u64,
    pub destination_account: String,
}

pub fn extract_transfers(block: &EncodedConfirmedBlock) -> Vec<TransferEvent> {
    let mut events = Vec::new();
    for entry in &block.transactions {
        let Some(meta) = entry.meta.as_ref() else {
            continue;
        };
        if meta.err.is_some() {
            continue;
        }
        let Ok(transaction) = decode_transaction(&entry.transaction) else {
            continue;
        };
        let account_keys = resolve_account_keys(&transaction, meta);
        for instruction in transaction.message.instructions() {
            let Some(program_id) = account_keys.get(instruction.program_id_index as usize) else {
                continue;
            };
            if *program_id != system_program::id() {
                continue;
            }
            if instruction.accounts.len() != TRANSFER_ACCOUNT_COUNT
                || instruction.data.len() != TRANSFER_DATA_LEN
            {
                continue;
            }
            if instruction.data[..4] != TRANSFER_DISCRIMINANT {
                continue;
            }
            let Ok(lamports_bytes) = <[u8; 8]>::try_from(&instruction.data[4..]) else {
                continue;
            };
            let Some(source_account) = account_keys.get(instruction.accounts[0] as usize) else {
                continue;
            };
            let Some(destination_account) = account_keys.get(instruction.accounts[1] as usize)
            else {
                continue;
            };
            events.push(TransferEvent {
                source_account: source_account.to_string(),
                lamports: u64::from_le_bytes(lamports_bytes),
                destination_account: destination_account.to_string(),
            });
        }
    }
    events
}

fn decode_transaction(encoded: &EncodedTransaction) -> Option<VersionedTransaction> {
    match encoded {
        EncodedTransaction::Binary(raw, _) => {
            let raw_bytes = BASE64_STANDARD.decode(raw).ok()?;
            bincode::deserialize::<VersionedTransaction>(&raw_bytes).ok()
        }
        _ => None,
    }
}

fn resolve_account_keys(
    transaction: &VersionedTransaction,
    meta: &UiTransactionStatusMeta,
) -> Vec<Pubkey> {
    let mut keys = Vec::with_capacity(
        transaction.message.static_account_keys().len()
            + meta.loaded_addresses.writable.len()
            + meta.loaded_addresses.readonly.len(),
    );
    keys.extend_from_slice(transaction.message.static_account_keys());
    keys.extend(
        meta.loaded_addresses
            .writable
            .iter()
            .filter_map(|key| Pubkey::from_str(key).ok()),
    );
    keys.extend(
        meta.loaded_addresses
            .readonly
            .iter()
            .filter_map(|key| Pubkey::from_str(key).ok()),
    );
    keys
}
