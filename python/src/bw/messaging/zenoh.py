import json
import os

import zenoh


ZENOH_ENDPOINT = os.getenv("ZENOH_ENDPOINT", "")

JOBS_BATCH_KEY = os.getenv("JOBS_BATCH_KEY", "")
WORKER_STATUS_KEY = os.getenv("WORKER_STATUS_KEY", "")
ORCHESTRATOR_TO_CONSUMER_KEY = os.getenv("ORCHESTRATOR_TO_CONSUMER_KEY", "")
CHORE_FILTER_RESULT_KEY = os.getenv("CHORE_FILTER_RESULT_KEY", "")
ZENOH_WORKER_REQUEST_PREFIX = os.getenv("ZENOH_WORKER_REQUEST_PREFIX", "")


def worker_request_key(worker_instance_id: str) -> str:
    return f"{ZENOH_WORKER_REQUEST_PREFIX}/{worker_instance_id}/requests"


def open_zenoh_session():
    config = zenoh.Config.from_json5(
        json.dumps(
            {
                "mode": "client",
                "connect": {
                    "endpoints": [ZENOH_ENDPOINT],
                },
                "scouting": {
                    "multicast": {
                        "enabled": False,
                    },
                },
            }
        )
    )

    return zenoh.open(config)