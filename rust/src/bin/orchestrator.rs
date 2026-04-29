use std::{collections::HashSet, time::Duration};

use prost::Message;
use rbw_rust::{
    config::{
        DynError, JOBS_QUEUE, ORCHESTRATOR_TO_CONSUMER_KEY, PRODUCER_TO_ORCHESTRATOR_KEY,
        RESULTS_QUEUE,
    },
    logging::init_logging,
    proto::demo::example1::{BatchRequest, BatchSummary, JobResult},
    rabbitmq::{connect_with_retry, declare_queues, publish_bytes},
};
use tokio::time::sleep;
use tracing::{error, info, warn};

use lapin::{
    options::{BasicAckOptions, BasicGetOptions, BasicNackOptions},
};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let _guard = init_logging("orchestrator-rust")?;

    let rabbit_connection = connect_with_retry(30, 2).await?;
    let rabbit_channel = rabbit_connection.create_channel().await?;
    declare_queues(&rabbit_channel).await?;
    info!("[orchestrator] connected to RabbitMQ");

    let zenoh_config = rbw_rust::config::zenoh_client_config()?;
    let zenoh_session = zenoh::open(zenoh_config).await?;
    info!("[orchestrator] connected to Zenoh");

    let consumer_pub = zenoh_session
        .declare_publisher(ORCHESTRATOR_TO_CONSUMER_KEY)
        .await?;

    let subscriber = zenoh_session
        .declare_subscriber(PRODUCER_TO_ORCHESTRATOR_KEY)
        .with(flume::bounded(1024))
        .await?;

    info!(
        "[orchestrator] subscribed to {}",
        PRODUCER_TO_ORCHESTRATOR_KEY
    );

    loop {
        let sample = subscriber.recv_async().await?;
        let payload = sample.payload().to_bytes();

        match BatchRequest::decode(payload.as_ref()) {
            Ok(batch) => {
                info!(
                    "[orchestrator] received batch request: batch_id={} total_jobs={}",
                    batch.batch_id, batch.total_jobs
                );

                if let Err(err) =
                    process_batch(batch, &rabbit_channel, &consumer_pub).await
                {
                    error!("[orchestrator] failed to process batch: {}", err);
                }
            }
            Err(err) => {
                error!(
                    "[orchestrator] failed to decode zenoh batch request: {}",
                    err
                );
            }
        }
    }
}

async fn process_batch(
    batch: BatchRequest,
    rabbit_channel: &lapin::Channel,
    consumer_pub: &zenoh::pubsub::Publisher<'_>,
) -> Result<(), DynError> {
    let batch_id = batch.batch_id.clone();
    let total_jobs = batch.total_jobs;

    info!(
        "[orchestrator] starting batch_id={} total_jobs={}",
        batch_id, total_jobs
    );

    for job in &batch.jobs {
        let mut buf = Vec::with_capacity(job.encoded_len());
        job.encode(&mut buf)?;
        publish_bytes(rabbit_channel, JOBS_QUEUE, &buf).await?;

        info!(
            "[orchestrator] queued job: batch_id={} job_id={}",
            job.batch_id, job.job_id
        );
    }

    let mut received_results = 0u32;
    let mut seen_job_ids: HashSet<u32> = HashSet::new();
    let mut results: Vec<JobResult> = Vec::new();

    info!(
        "[orchestrator] waiting for {} results for batch_id={}",
        total_jobs, batch_id
    );

    while received_results < total_jobs {
        let delivery = rabbit_channel
            .basic_get(RESULTS_QUEUE, BasicGetOptions { no_ack: false })
            .await?;

        let Some(delivery) = delivery else {
            sleep(Duration::from_millis(250)).await;
            continue;
        };

        let delivery_tag = delivery.delivery_tag;
        let body = delivery.data.clone();

        match JobResult::decode(body.as_ref()) {
            Ok(result) => {
                if result.batch_id != batch_id {
                    warn!(
                        "[orchestrator] ignoring stale/non-matching result: batch_id={} job_id={}",
                        result.batch_id, result.job_id
                    );
                    rabbit_channel
                        .basic_ack(delivery_tag, BasicAckOptions::default())
                        .await?;
                    continue;
                }

                let job_id = result.job_id;

                if seen_job_ids.insert(job_id) {
                    received_results += 1;
                    results.push(result.clone());

                    info!(
                        "[orchestrator] received result {}/{}: batch_id={} job_id={} worker={} result={}",
                        received_results,
                        total_jobs,
                        result.batch_id,
                        result.job_id,
                        result.worker,
                        result.result
                    );
                } else {
                    warn!(
                        "[orchestrator] duplicate result ignored: batch_id={} job_id={}",
                        result.batch_id, result.job_id
                    );
                }

                rabbit_channel
                    .basic_ack(delivery_tag, BasicAckOptions::default())
                    .await?;
            }
            Err(err) => {
                error!("[orchestrator] error processing result: {}", err);
                rabbit_channel
                    .basic_nack(
                        delivery_tag,
                        BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        },
                    )
                    .await?;
            }
        }
    }

    let summary = BatchSummary {
        batch_id: batch_id.clone(),
        total_jobs,
        results_received: results.len() as u32,
        results,
        context: batch.context,
    };

    let mut buf = Vec::with_capacity(summary.encoded_len());
    summary.encode(&mut buf)?;

    consumer_pub.put(buf).await?;

    info!(
        "[orchestrator] published batch summary to zenoh: batch_id={} total_jobs={} results_received={}",
        batch_id, total_jobs, summary.results_received
    );

    Ok(())
}