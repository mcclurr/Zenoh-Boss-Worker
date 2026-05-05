use std::env;

pub type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub const JOBS_QUEUE: &str = "jobs";
pub const RESULTS_QUEUE: &str = "results";

pub const PRODUCER_TO_ORCHESTRATOR_KEY: &str = "demo/producer/batch";
pub const ORCHESTRATOR_TO_CONSUMER_KEY: &str = "demo/orchestrator/output";

pub fn assignment_request_key() -> String {
    env::var("ASSIGNMENT_REQUEST_KEY")
        .unwrap_or_else(|_| "demo/assignment/request".to_string())
}

pub fn assignment_result_key() -> String {
    env::var("ASSIGNMENT_RESULT_KEY")
        .unwrap_or_else(|_| "demo/assignment/result".to_string())
}

pub fn rabbitmq_host() -> String {
    env::var("RABBITMQ_HOST").unwrap_or_else(|_| "rabbitmq".to_string())
}

pub fn worker_name() -> String {
    env::var("WORKER_NAME").unwrap_or_else(|_| hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "worker".to_string()))
}

pub fn zenoh_endpoint() -> String {
    env::var("ZENOH_ENDPOINT").unwrap_or_else(|_| "tcp/zenoh:7447".to_string())
}

pub fn zenoh_client_config() -> Result<zenoh::Config, DynError> {
    let endpoint = zenoh_endpoint();
    let json5 = format!(
        r#"{{
          mode: "client",
          connect: {{ endpoints: ["{endpoint}"] }},
          scouting: {{
            multicast: {{
              enabled: false
            }}
          }}
        }}"#
    );
    Ok(zenoh::Config::from_json5(&json5)?)
}