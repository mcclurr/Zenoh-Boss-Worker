import os
import time

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    JOBS_BATCH_KEY,
    WORKER_STATUS_KEY,
    open_zenoh_session,
)
from bw.services.orchestrator.batch_runner import BatchRunner
from bw.services.orchestrator.coordinator import BatchCoordinator
from bw.services.orchestrator.result_dispatcher import RabbitResultDispatcher


MATCH_WINDOW_SECONDS = float(os.getenv("MATCH_WINDOW_SECONDS", "0.5"))


def main() -> None:
    logger = init_logging("orchestrator-python")

    result_dispatcher = RabbitResultDispatcher(logger=logger)
    result_dispatcher.start()

    logger.info("[orchestrator] started RabbitMQ result dispatcher")

    with open_zenoh_session() as zenoh_session:
        logger.info("[orchestrator] connected to Zenoh")

        consumer_pub = zenoh_session.declare_publisher(ORCHESTRATOR_TO_CONSUMER_KEY)

        batch_runner = BatchRunner(
            result_dispatcher=result_dispatcher,
            consumer_pub=consumer_pub,
            logger=logger,
        )

        coordinator = BatchCoordinator(
            batch_runner=batch_runner,
            logger=logger,
            match_window_seconds=MATCH_WINDOW_SECONDS,
        )

        zenoh_session.declare_subscriber(JOBS_BATCH_KEY, coordinator.on_topic_a)
        zenoh_session.declare_subscriber(WORKER_STATUS_KEY, coordinator.on_topic_b)

        logger.info(
            "[orchestrator] subscribed to jobs_key=%s worker_key=%s "
            "match_window=%.3fs",
            JOBS_BATCH_KEY,
            WORKER_STATUS_KEY,
            MATCH_WINDOW_SECONDS,
        )

        while True:
            time.sleep(0.1)
            coordinator.expire_stale_if_idle(time.monotonic())


if __name__ == "__main__":
    main()