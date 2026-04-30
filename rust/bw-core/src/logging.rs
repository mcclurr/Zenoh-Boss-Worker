use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

pub fn init_logging(prefix: &str) -> Result<WorkerGuard, std::io::Error> {
    let log_dir = "out";
    std::fs::create_dir_all(log_dir)?;

    let ts = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let filename = format!("{}/{}-{}.log", log_dir, ts, prefix);

    let file = std::fs::File::create(&filename)?;
    let (non_blocking, guard) = tracing_appender::non_blocking(file);

    let filter = std::env::var("RUST_LOG")
        .map(EnvFilter::new)
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_ansi(false)
        .try_init();

    tracing::info!("Logging initialized -> {}", filename);

    Ok(guard)
}