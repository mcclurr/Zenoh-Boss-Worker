mod config;
mod coordination;
mod execution;
mod processing;
mod transport;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bw_core::{
    config::DynError,
    logging::init_logging,
};

use tracing::info;

use config::OrchestratorConfig;
use coordination::coordinator::BatchCoordinator;
use execution::batch_runner::BatchRunner;
use execution::executor::{
    build_window_executor,
    JobSubmissionMode,
};
use transport::handler::OrchestratorHandler;

const JOB_SUBMISSION_MODE: JobSubmissionMode = JobSubmissionMode::PerPerson;

fn main() -> Result<(), DynError> {
    let _guard = init_logging("orchestrator-rust")?;

    info!("[orchestrator] rust orchestrator starting");

    let config = OrchestratorConfig {
        num_threads: 4,
        person_gather_window_seconds: 0.25,
        person_last_success_ttl_seconds: 30.0,
    };

    info!(
        "[orchestrator] config loaded: num_threads={} person_gather_window_seconds={} person_last_success_ttl_seconds={} job_submission_mode={:?}",
        config.num_threads,
        config.person_gather_window_seconds,
        config.person_last_success_ttl_seconds,
        JOB_SUBMISSION_MODE,
    );

    let batch_runner = BatchRunner::new();

    let executor = build_window_executor(
        JOB_SUBMISSION_MODE,
        batch_runner,
    );

    let coordinator = Arc::new(Mutex::new(
        BatchCoordinator::new(
            executor,
            config,
        )
    ));

    let _handler = OrchestratorHandler::new(
        Arc::clone(&coordinator),
    );

    info!("[orchestrator] initialized");

    loop {
        thread::sleep(Duration::from_millis(100));

        let now = Instant::now();

        if let Ok(mut coordinator) = coordinator.lock() {
            coordinator.expire_stale_if_idle(now);
        }
    }
}