import json
import os

import zenoh


ZENOH_ENDPOINT = os.getenv("ZENOH_ENDPOINT", "")

TOPIC_A_KEY = os.getenv("TOPIC_A_KEY", "")
TOPIC_B_KEY = os.getenv("TOPIC_B_KEY", "")

PRODUCER_TO_ORCHESTRATOR_KEY = os.getenv("PRODUCER_TO_ORCHESTRATOR_KEY", "")
ORCHESTRATOR_TO_CONSUMER_KEY = os.getenv("ORCHESTRATOR_TO_CONSUMER_KEY", "")


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