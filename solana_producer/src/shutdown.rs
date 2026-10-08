use std::time::Duration;

use tokio::sync::watch;

pub type ShutdownSignal = watch::Receiver<bool>;

pub fn shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

pub fn is_shutdown(signal: &ShutdownSignal) -> bool {
    *signal.borrow()
}

pub async fn sleep_with_shutdown(delay: Duration, signal: &ShutdownSignal) -> bool {
    let mut watcher = signal.clone();
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = async {
            while !*watcher.borrow_and_update() {
                if watcher.changed().await.is_err() {
                    return;
                }
            }
        } => true,
    }
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
