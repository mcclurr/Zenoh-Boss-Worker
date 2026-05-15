mod config;
mod coordination;
mod execution;
mod processing;
mod transport;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bw_core::{
    config::{
        jobs_batch_key,
        orchestrator_to_consumer_key,
        worker_status_key,
        zenoh_client_config,
        DynError,
    },
    logging::init_logging,
};

use tokio::time;
use tracing::info;

use config::load_orchestrator_config_from_env;
use coordination::coordinator::BatchCoordinator;
use execution::batch_runner::BatchRunner;
use execution::executor::{
    build_window_executor,
    JobSubmissionMode,
};
use transport::handler::OrchestratorHandler;
use tokio::sync::mpsc;

const JOB_SUBMISSION_MODE: JobSubmissionMode = JobSubmissionMode::PerPerson;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let _guard = init_logging("orchestrator-rust")?;

    info!("[orchestrator] rust orchestrator starting");

    let config = load_orchestrator_config_from_env()?;

    let chores_key = jobs_batch_key();
    let person_key = worker_status_key();
    let summary_key = orchestrator_to_consumer_key();

    let zenoh_config = zenoh_client_config()?;
    let zenoh_session = zenoh::open(zenoh_config).await?;

    info!("[orchestrator] connected to Zenoh");

    let summary_pub = zenoh_session
        .declare_publisher(summary_key.clone())
        .await?;

    let chores_sub = zenoh_session
        .declare_subscriber(chores_key.clone())
        .with(flume::bounded(1024))
        .await?;

    let person_sub = zenoh_session
        .declare_subscriber(person_key.clone())
        .with(flume::bounded(1024))
        .await?;

    let batch_runner = BatchRunner::new(summary_pub);

    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();

    let executor = build_window_executor(
        JOB_SUBMISSION_MODE,
        batch_runner,
        completion_tx,
    );

    let coordinator = Arc::new(Mutex::new(
        BatchCoordinator::new(
            executor,
            config.clone(),
        )
    ));

    let handler = OrchestratorHandler::new(
        Arc::clone(&coordinator),
    );

    info!(
        "[orchestrator] subscribed: chores_key={} person_key={} summary_key={} num_threads={} job_submission_mode={:?}",
        chores_key,
        person_key,
        summary_key,
        config.num_threads,
        JOB_SUBMISSION_MODE,
    );

    let mut tick = time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            sample = chores_sub.recv_async() => {
                match sample {
                    Ok(sample) => {
                        let payload = sample.payload().to_bytes();
                        handler.on_chores_bytes(payload.as_ref());
                    }
                    Err(err) => {
                        tracing::error!(
                            "[orchestrator] failed to receive chores sample: {}",
                            err,
                        );
                    }
                }
            }

            sample = person_sub.recv_async() => {
                match sample {
                    Ok(sample) => {
                        let payload = sample.payload().to_bytes();
                        handler.on_person_bytes(payload.as_ref());
                    }
                    Err(err) => {
                        tracing::error!(
                            "[orchestrator] failed to receive person sample: {}",
                            err,
                        );
                    }
                }
            }

            _ = tick.tick() => {
                let now = Instant::now();

                let mut coordinator = coordinator
                    .lock()
                    .expect("coordinator mutex poisoned");

                coordinator.expire_stale_if_idle(now);
            }

            completion = completion_rx.recv() => {
                if let Some(completion) = completion {
                    let mut coordinator = coordinator
                        .lock()
                        .expect("coordinator mutex poisoned");

                    coordinator.on_person_complete(completion);
                }
            }
        }
    }
}