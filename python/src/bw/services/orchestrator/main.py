import os
import time

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    JOBS_BATCH_KEY,
    ORCHESTRATOR_TO_CONSUMER_KEY,
    WORKER_STATUS_KEY,
    open_zenoh_session,
)
from bw.services.orchestrator.batch_runner import BatchRunner
from bw.services.orchestrator.coordinator import BatchCoordinator
from bw.services.orchestrator.result_dispatcher import ZenohResultDispatcher



ZENOH_WORKER_IDS = [
    worker_id.strip()
    for worker_id in os.getenv("ZENOH_WORKER_IDS", "").split(",")
    if worker_id.strip()
]

WORKER_MAX_CONCURRENT_REQUESTS = int(os.getenv("WORKER_MAX_CONCURRENT_REQUESTS", ""))


def main() -> None:
    logger = init_logging("orchestrator-python")

    if not ZENOH_WORKER_IDS:
        raise RuntimeError("ZENOH_WORKER_IDS must contain at least one worker ID")

    with open_zenoh_session() as zenoh_session:
        logger.info("[orchestrator] connected to Zenoh")

        result_dispatcher = ZenohResultDispatcher(
            zenoh_session=zenoh_session,
            logger=logger,
        )
        result_dispatcher.start()

        consumer_pub = zenoh_session.declare_publisher(
            ORCHESTRATOR_TO_CONSUMER_KEY
        )

        batch_runner = BatchRunner(
            zenoh_session=zenoh_session,
            result_dispatcher=result_dispatcher,
            consumer_pub=consumer_pub,
            logger=logger,
            worker_instance_ids=ZENOH_WORKER_IDS,
            max_inflight_per_worker=WORKER_MAX_CONCURRENT_REQUESTS,
        )

        coordinator = BatchCoordinator(
            batch_runner=batch_runner,
            logger=logger
        )

        zenoh_session.declare_subscriber(JOBS_BATCH_KEY, coordinator.on_topic_a)
        zenoh_session.declare_subscriber(WORKER_STATUS_KEY, coordinator.on_topic_b)

        logger.info(
            "[orchestrator] subscribed: chores_key=%s person_key=%s "
            "summary_key=%s worker_ids=%s max_inflight_per_worker=%s",
            JOBS_BATCH_KEY,
            WORKER_STATUS_KEY,
            ORCHESTRATOR_TO_CONSUMER_KEY,
            ZENOH_WORKER_IDS,
            WORKER_MAX_CONCURRENT_REQUESTS,
        )

        while True:
            time.sleep(0.1)
            coordinator.expire_stale_if_idle(time.monotonic())


if __name__ == "__main__":
    main()