use std::env;
use std::time::Duration;

use solana_sdk::commitment_config::CommitmentConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub solana_http_url: String,
    pub commitment: CommitmentConfig,
    pub start_slot: Option<u64>,
    pub slot_poll_interval: Duration,
    pub slot_queue_depth: usize,
    pub event_queue_depth: usize,
    pub fetch_workers: usize,
    pub max_fetch_attempts: u32,
    pub max_produce_attempts: u32,
}

impl Config {
    pub fn from_env() -> Self {
        let commitment = match env::var("SOLANA_COMMITMENT")
            .unwrap_or_else(|_| "confirmed".to_string())
            .as_str()
        {
            "finalized" => CommitmentConfig::finalized(),
            "processed" => CommitmentConfig::processed(),
            _ => CommitmentConfig::confirmed(),
        };
        Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            kafka_topic: env_or("KAFKA_TOPIC", "solana.transfers.v1"),
            solana_http_url: env_or("SOLANA_HTTP_URL", "https://api.mainnet-beta.solana.com"),
            commitment,
            start_slot: env::var("SOLANA_START_SLOT").ok().and_then(|v| v.parse().ok()),
            slot_poll_interval: Duration::from_millis(parse_or("SLOT_POLL_INTERVAL_MS", 500)),
            slot_queue_depth: parse_or::<u64>("SLOT_QUEUE_DEPTH", 4096) as usize,
            event_queue_depth: parse_or::<u64>("EVENT_QUEUE_DEPTH", 8192) as usize,
            fetch_workers: parse_or::<u64>("FETCH_WORKERS", 4) as usize,
            max_fetch_attempts: parse_or("MAX_FETCH_ATTEMPTS", 8),
            max_produce_attempts: parse_or("MAX_PRODUCE_ATTEMPTS", 10),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
