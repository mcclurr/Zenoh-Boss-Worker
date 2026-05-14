import os
from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestratorConfig:
    num_threads: int
    person_gather_window_seconds: float
    person_last_success_ttl_seconds: float

    @property
    def max_active_filters(self) -> int:
        return self.num_threads


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"{name} must be set")
    return value


def load_orchestrator_config_from_env() -> OrchestratorConfig:
    return OrchestratorConfig(
        num_threads=int(require_env("NUM_THREADS")),
        person_gather_window_seconds=float(require_env("PERSON_GATHER_WINDOW_SECONDS")),
        person_last_success_ttl_seconds=float(require_env("PERSON_LAST_SUCCESS_TTL_SECONDS")),
    )