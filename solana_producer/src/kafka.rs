use std::time::Duration;

use anyhow::Result;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::backoff::backoff_delay;
use crate::config::Config;
use crate::shutdown::{sleep_with_shutdown, ShutdownSignal};
use crate::transfer::TransferEvent;

const PRODUCE_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const PRODUCER_FLUSH_TIMEOUT: Duration = Duration::from_secs(15);
const FIXED_PARTITION: i32 = 0;

pub fn build_producer(config: &Config) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "60000")
        .set("compression.type", "lz4")
        .set("linger.ms", "5")
        .create()?;
    Ok(producer)
}

pub async fn run_kafka_writer(
    producer: FutureProducer,
    topic: String,
    max_attempts: u32,
    mut event_rx: mpsc::Receiver<TransferEvent>,
    shutdown: ShutdownSignal,
) {
    info!(%topic, "kafka writer started");
    while let Some(event) = event_rx.recv().await {
        if let Err(delivery_error) =
            deliver(&producer, &topic, &event, max_attempts, &shutdown).await
        {
            error!(
                error = %delivery_error,
                source = %event.source_account,
                destination = %event.destination_account,
                lamports = event.lamports,
                "delivery permanently failed, transfer logged and dropped"
            );
        }
    }
    match producer.flush(PRODUCER_FLUSH_TIMEOUT) {
        Ok(()) => info!("kafka producer flushed"),
        Err(flush_error) => warn!(error = %flush_error, "producer flush incomplete"),
    }
}
          
async fn deliver(
    producer: &FutureProducer,
    topic: &str,
    event: &TransferEvent,
    max_attempts: u32,
    shutdown: &ShutdownSignal,
) -> Result<()> {
    let payload = serde_json::to_string(event).unwrap();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        let record = FutureRecord::to(topic)
            .partition(FIXED_PARTITION)
            .key("")
            .payload(&payload);

        let send_result = async {
            let delivery_future = producer
                .send(record, Timeout::After(PRODUCE_ENQUEUE_TIMEOUT))
                .map_err(|(e, _)| e)?; 
            
            delivery_future.await.map_err(|(e, _)| e) 
        }.await;

        match send_result {
            Ok((_, _)) => return Ok(()),
            Err(err) => {
                if attempt >= max_attempts {
                    return Err(err.into());
                }

                warn!(attempt, max_attempts, error = %err, "kafka transfer deliver failed, retrying");

                if sleep_with_shutdown(backoff_delay(attempt), shutdown).await {
                    return Err(err.into());
                }
            }
        }
    }
}
