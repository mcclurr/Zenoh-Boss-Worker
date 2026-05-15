use bw_core::config::DynError;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub num_threads: usize,
    pub person_gather_window_seconds: f64,
    pub person_last_success_ttl_seconds: f64,
}

impl OrchestratorConfig {
    pub fn max_active_filters(&self) -> usize {
        self.num_threads
    }
}

fn require_env(name: &str) -> Result<String, DynError> {
    let value = std::env::var(name)?;

    if value.trim().is_empty() {
        return Err(format!("{name} must be set").into());
    }

    Ok(value)
}

pub fn load_orchestrator_config_from_env()
    -> Result<OrchestratorConfig, DynError>
{
    Ok(OrchestratorConfig {
        num_threads: require_env("NUM_THREADS")?.parse()?,
        person_gather_window_seconds: require_env(
            "PERSON_GATHER_WINDOW_SECONDS",
        )?
        .parse()?,
        person_last_success_ttl_seconds: require_env(
            "PERSON_LAST_SUCCESS_TTL_SECONDS",
        )?
        .parse()?,
    })
}