const BASE_DELAY_MS: u64 = 200;
const MAX_DELAY_MS: u64 = 10_000;
const MAX_SHIFT: u32 = 6;

pub fn backoff_delay(attempt: u32) -> std::time::Duration {
    let exponential = BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(MAX_SHIFT));
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |now| u64::from(now.subsec_millis()) % 128);
    Duration::from_millis(exponential.min(MAX_DELAY_MS) + jitter)
}
