use std::time::Duration;

use futures_util::stream::StreamExt;
use prost::Message;
use rand::Rng;
use rbw_rust::{
    config::{worker_name, DynError, JOBS_QUEUE, RESULTS_QUEUE},
    logging::init_logging,
    proto::demo::example1::{work_payload::Kind, Job, JobResult},
    rabbitmq::{connect_with_retry, declare_queues, publish_bytes},
};
use tokio::time::sleep;
use tracing::{error, info};

use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
    },
    types::FieldTable,
};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let worker_name = worker_name();
    let _guard = init_logging(&worker_name)?;

    let connection = connect_with_retry(30, 2).await?;
    let channel = connection.create_channel().await?;
    declare_queues(&channel).await?;

    channel
        .basic_qos(1, BasicQosOptions::default())
        .await?;

    info!("[worker {}] connected and waiting for jobs", worker_name);

    let mut consumer = channel
        .basic_consume(
            JOBS_QUEUE,
            &format!("consumer-{}", worker_name),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let body = delivery.data.clone();

                match process_delivery(&channel, &worker_name, &body).await {
                    Ok(()) => {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    Err(err) => {
                        error!("[worker {}] error: {}", worker_name, err);
                        delivery
                            .nack(BasicNackOptions {
                                requeue: true,
                                ..Default::default()
                            })
                            .await?;
                    }
                }
            }
            Err(err) => {
                error!("[worker {}] consumer error: {}", worker_name, err);
            }
        }
    }

    Ok(())
}

async fn process_delivery(
    channel: &lapin::Channel,
    worker_name: &str,
    body: &[u8],
) -> Result<(), DynError> {
    let job = Job::decode(body)?;

    info!(
        "[worker {}] got job: batch_id={} job_id={}",
        worker_name, job.batch_id, job.job_id
    );

    let sleep_time = rand::thread_rng().gen_range(1.0f64..3.0f64);
    sleep(Duration::from_secs_f64(sleep_time)).await;

    let payload_value = match job.payload.and_then(|p| p.kind) {
        Some(Kind::Text(text)) => text,
        Some(Kind::NumericValue(n)) => n.to_string(),
        Some(Kind::RawBytes(_)) => "<bytes>".to_string(),
        None => "".to_string(),
    };

    let result = JobResult {
        batch_id: job.batch_id.clone(),
        job_id: job.job_id,
        worker: worker_name.to_string(),
        result: format!("processed-{}", payload_value),
        processing_seconds: (sleep_time * 100.0).round() / 100.0,
        context: job.context,
        warnings: vec![],
    };

    let mut buf = Vec::with_capacity(result.encoded_len());
    result.encode(&mut buf)?;

    publish_bytes(channel, RESULTS_QUEUE, &buf).await?;

    info!(
        "[worker {}] sent result: batch_id={} job_id={} result={}",
        worker_name, result.batch_id, result.job_id, result.result
    );

    Ok(())
}