mod backoff;
mod block_fetcher;
mod config;
mod kafka;
mod shutdown;
mod transfer;

use std::sync::Arc;
use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use crate::block_fetcher::{build_block_config, run_fetch_worker};
use crate::config::Config;
use crate::kafka::{build_producer, run_kafka_writer};
use crate::shutdown::{is_shutdown, shutdown_channel, shutdown_signal, sleep_with_shutdown};
use crate::transfer::TransferEvent;

const SLOT_BACKLOG_WARN: u64 = 256;

#[tokio::main]
async fn main() {
    init_tracing();
    let config = Config::from_env();
    info!(
        rpc = %config.solana_http_url,
        brokers = %config.kafka_brokers,
        topic = %config.kafka_topic,
        commitment = ?config.commitment,
        "starting solana transfer producer"
    );

    let producer = match build_producer(&config) {
        Ok(producer) => producer,
        Err(error) => {
            error!(error = %error, "kafka producer bootstrap failed");
            std::process::exit(1);
        }
    };

    let (shutdown_tx, shutdown_rx) = shutdown_channel();
    let (slot_tx, slot_rx) = mpsc::channel::<u64>(config.slot_queue_depth);
    let (event_tx, event_rx) = mpsc::channel::<TransferEvent>(config.event_queue_depth);
    let block_config = build_block_config(config.commitment);
    let rpc_client = Arc::new(RpcClient::new_with_timeout(
        config.solana_http_url.clone(),
        Duration::from_secs(60),
    ));

    let orchestrator = tokio::spawn(run_orchestrator(
        Arc::clone(&rpc_client),
        config.commitment,
        config.start_slot,
        config.slot_poll_interval,
        slot_tx,
        shutdown_rx.clone(),
    ));

    let mut workers = Vec::with_capacity(config.fetch_workers);
    for worker_id in 0..config.fetch_workers {
        workers.push(tokio::spawn(run_fetch_worker(
            worker_id,
            Arc::clone(&rpc_client),
            block_config.clone(),
            config.max_fetch_attempts,
            slot_rx.clone(),
            event_tx.clone(),
            shutdown_rx.clone(),
        )));
    }
    drop(event_tx);

    let writer = tokio::spawn(run_kafka_writer(
        producer,
        config.kafka_topic.clone(),
        config.max_produce_attempts,
        event_rx,
        shutdown_rx.clone(),
    ));

    shutdown_signal().await;
    info!("shutdown signal received, draining pipeline");
    let _ = shutdown_tx.send(true);
    drop(shutdown_tx);

    let drained = tokio::time::timeout(Duration::from_secs(45), async {
        let _ = orchestrator.await;
        for worker in workers {
            let _ = worker.await;
        }
        let _ = writer.await;
    })
    .await;

    match drained {
        Ok(()) => info!("pipeline drained cleanly"),
        Err(_) => warn!("shutdown drain timed out, forcing exit"),
    }
}

async fn run_orchestrator(
    client: Arc<RpcClient>,
    commitment: CommitmentConfig,
    start_slot: Option<u64>,
    poll_interval: Duration,
    slot_tx: mpsc::Sender<u64>,
    shutdown: watch::Receiver<bool>,
) {
    let mut cursor = match start_slot {
        Some(slot) => slot,
        None => loop {
            match client.get_slot_with_commitment(commitment).await {
                Ok(head) => break head,
                Err(error) => {
                    warn!(error = %error, "initial head slot probe failed");
                    if sleep_with_shutdown(poll_interval, &shutdown).await {
                        return;
                    }
                }
            }
        },
    };
    info!(cursor, "slot orchestrator cursor initialized");

    loop {
        if is_shutdown(&shutdown) {
            break;
        }
        let head = match client.get_slot_with_commitment(commitment).await {
            Ok(head) => head,
            Err(error) => {
                warn!(error = %error, cursor, "head slot probe failed");
                if sleep_with_shutdown(poll_interval, &shutdown).await {
                    break;
                }
                continue;
            }
        };
        if head >= cursor {
            let lag = head - cursor;
            if lag > SLOT_BACKLOG_WARN {
                warn!(lag, cursor, head, "slot dispatch backlog above threshold");
            }
            for slot in cursor..=head {
                if slot_tx.send(slot).await.is_err() {
                    return;
                }
                if is_shutdown(&shutdown) {
                    return;
                }
            }
            cursor = head + 1;
            debug!(head, next_cursor = cursor, "slots dispatched");
        }
        if sleep_with_shutdown(poll_interval, &shutdown).await {
            break;
        }
    }
    info!("slot orchestrator stopped");
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
