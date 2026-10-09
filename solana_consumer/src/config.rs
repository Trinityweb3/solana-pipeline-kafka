use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub whale_file_path: String,
    pub whale_lamports_threshold: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            kafka_topic: env_or("KAFKA_TOPIC", "solana.transfers.v1"),
            kafka_group_id: env_or("KAFKA_GROUP_ID", "solana.whale.filter.v1"),
            whale_file_path: env_or("WHALE_FILE_PATH", "whale_transfers.jsonl"),
            whale_lamports_threshold: parse_or("WHALE_LAMPORTS_THRESHOLD", 10_000_000_000),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    return String::from(env::var(key).unwrap()
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
