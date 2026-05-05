import os
import time

from bw.common.log import init_logging
from bw.messaging.zenoh import open_zenoh_session
from bw.services.assignment.handler import handle_assignment_bytes


ASSIGNMENT_REQUEST_KEY = os.getenv(
    "ASSIGNMENT_REQUEST_KEY",
    "demo/assignment/request",
)

ASSIGNMENT_RESULT_KEY = os.getenv(
    "ASSIGNMENT_RESULT_KEY",
    "demo/assignment/result",
)


def main() -> None:
    logger = init_logging("assignment-service")

    with open_zenoh_session() as session:
        result_pub = session.declare_publisher(ASSIGNMENT_RESULT_KEY)

        logger.info(
            "[assignment-service] subscribed: request_key=%s result_key=%s",
            ASSIGNMENT_REQUEST_KEY,
            ASSIGNMENT_RESULT_KEY,
        )

        def on_request(sample) -> None:
            try:
                result_bytes = handle_assignment_bytes(sample.payload.to_bytes())
                result_pub.put(result_bytes)

                logger.info(
                    "[assignment-service] published assignment result: key=%s",
                    ASSIGNMENT_RESULT_KEY,
                )

            except Exception as exc:
                logger.exception(
                    "[assignment-service] failed to process assignment request: %s",
                    exc,
                )

        session.declare_subscriber(ASSIGNMENT_REQUEST_KEY, on_request)

        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()