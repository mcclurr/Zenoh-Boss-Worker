// rust/services/orchestrator/src/config.rs
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