mod handler;
mod solver;

use bw_core::{
    config::{
        assignment_request_key, assignment_result_key, zenoh_client_config, DynError,
    },
    logging::init_logging,
};

use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let _guard = init_logging("assignment-service-rust")?;

    let request_key = assignment_request_key();
    let result_key = assignment_result_key();

    let zenoh_config = zenoh_client_config()?;
    let zenoh_session = zenoh::open(zenoh_config).await?;

    info!("[assignment-service-rust] connected to Zenoh");

    let result_pub = zenoh_session
        .declare_publisher(result_key.clone())
        .await?;

    let subscriber = zenoh_session
        .declare_subscriber(request_key.clone())
        .with(flume::bounded(1024))
        .await?;

    info!(
        "[assignment-service-rust] subscribed: request_key={} result_key={}",
        request_key, result_key
    );

    loop {
        let sample = subscriber.recv_async().await?;
        let payload = sample.payload().to_bytes();

        match handler::handle_assignment_bytes(payload.as_ref()) {
            Ok(result_bytes) => {
                result_pub.put(result_bytes).await?;

                info!(
                    "[assignment-service-rust] published assignment result: key={}",
                    result_key
                );
            }
            Err(err) => {
                error!(
                    "[assignment-service-rust] failed to process assignment request: {}",
                    err
                );
            }
        }
    }
}