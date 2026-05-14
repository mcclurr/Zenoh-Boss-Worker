# python/src/bw/services/orchestrator/config.py

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestratorConfig:
    worker_ids: list[str]
    worker_max_concurrent_requests: int
    person_gather_window_seconds: float
    person_last_success_ttl_seconds: float
    result_timeout_seconds: float

    @property
    def max_active_filters(self) -> int:
        return self.worker_max_concurrent_requests * len(self.worker_ids)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"{name} must be set")
    return value


def load_orchestrator_config_from_env() -> OrchestratorConfig:
    worker_ids = [
        worker_id.strip()
        for worker_id in require_env("ZENOH_WORKER_IDS").split(",")
        if worker_id.strip()
    ]

    if not worker_ids:
        raise RuntimeError("ZENOH_WORKER_IDS must contain at least one worker ID")

    return OrchestratorConfig(
        worker_ids=worker_ids,
        worker_max_concurrent_requests=int(require_env("WORKER_MAX_CONCURRENT_REQUESTS")),
        person_gather_window_seconds=float(require_env("PERSON_GATHER_WINDOW_SECONDS")),
        person_last_success_ttl_seconds=float(require_env("PERSON_LAST_SUCCESS_TTL_SECONDS")),
        result_timeout_seconds=float(require_env("RESULT_TIMEOUT_SECONDS")),
    )