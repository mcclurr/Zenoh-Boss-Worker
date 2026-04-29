use std::time::Duration;

use lapin::{
    options::{
        BasicPublishOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::{rabbitmq_host, DynError, JOBS_QUEUE, RESULTS_QUEUE};

pub async fn connect_with_retry(retries: u32, delay_secs: u64) -> Result<Connection, DynError> {
    let host = rabbitmq_host();
    let addr = format!("amqp://guest:guest@{}:5672/%2f", host);

    let mut last_error: Option<String> = None;

    for attempt in 1..=retries {
        match Connection::connect(&addr, ConnectionProperties::default()).await {
            Ok(conn) => return Ok(conn),
            Err(err) => {
                last_error = Some(err.to_string());
                warn!(
                    "[connect] attempt {}/{} failed: {}",
                    attempt, retries, err
                );
                sleep(Duration::from_secs(delay_secs)).await;
            }
        }
    }

    Err(format!(
        "Could not connect to RabbitMQ after {} attempts: {}",
        retries,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )
    .into())
}

pub async fn declare_queues(channel: &Channel) -> Result<(), DynError> {
    channel
        .queue_declare(
            JOBS_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            RESULTS_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

pub async fn publish_bytes(channel: &Channel, queue_name: &str, payload: &[u8]) -> Result<(), DynError> {
    channel
        .basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("application/octet-stream".into()),
        )
        .await?
        .await?;

    Ok(())
}

pub async fn create_channel() -> Result<Channel, DynError> {
    let conn = connect_with_retry(30, 2).await?;
    let channel = conn.create_channel().await?;
    declare_queues(&channel).await?;
    info!("Connected to RabbitMQ");
    Ok(channel)
}