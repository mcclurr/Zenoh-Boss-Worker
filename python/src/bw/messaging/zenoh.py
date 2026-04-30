import json
import os

import zenoh


ZENOH_ENDPOINT = os.getenv("ZENOH_ENDPOINT", "tcp/zenoh:7447")

TOPIC_A_KEY = os.getenv("TOPIC_A_KEY", "demo/input/a")
TOPIC_B_KEY = os.getenv("TOPIC_B_KEY", "demo/input/b")

PRODUCER_TO_ORCHESTRATOR_KEY = "demo/producer/batch"
ORCHESTRATOR_TO_CONSUMER_KEY = "demo/orchestrator/output"


def open_zenoh_session():
    """
    Open a Zenoh session as a client connected to the configured router.
    """
    config = zenoh.Config.from_json5(json.dumps({
        "mode": "client",
        "connect": {
            "endpoints": [ZENOH_ENDPOINT]
        },
        "scouting": {
            "multicast": {
                "enabled": False
            }
        }
    }))
    return zenoh.open(config)