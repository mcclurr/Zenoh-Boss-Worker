import time

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    JOBS_BATCH_KEY,
    ORCHESTRATOR_TO_CONSUMER_KEY,
    WORKER_STATUS_KEY,
    open_zenoh_session,
)
from bw.services.orchestrator.batch_runner import BatchRunner
from bw.services.orchestrator.config import load_orchestrator_config_from_env
from bw.services.orchestrator.coordinator import BatchCoordinator
from bw.services.orchestrator.handler import OrchestratorHandler
from bw.services.orchestrator.result_dispatcher import ZenohResultDispatcher


def main() -> None:
    logger = init_logging("orchestrator-python")
    config = load_orchestrator_config_from_env()

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
            worker_instance_ids=config.worker_ids,
            max_inflight_per_worker=config.worker_max_concurrent_requests,
            result_timeout_seconds=config.result_timeout_seconds,
        )

        coordinator = BatchCoordinator(
            batch_runner=batch_runner,
            config=config,
            logger=logger,
        )

        handler = OrchestratorHandler(
            coordinator=coordinator,
            logger=logger,
        )

        zenoh_session.declare_subscriber(JOBS_BATCH_KEY, handler.on_chores_sample)
        zenoh_session.declare_subscriber(WORKER_STATUS_KEY, handler.on_person_sample)

        logger.info(
            "[orchestrator] subscribed: chores_key=%s person_key=%s "
            "summary_key=%s worker_ids=%s max_inflight_per_worker=%s",
            JOBS_BATCH_KEY,
            WORKER_STATUS_KEY,
            ORCHESTRATOR_TO_CONSUMER_KEY,
            config.worker_ids,
            config.worker_max_concurrent_requests,
        )

        while True:
            time.sleep(0.1)
            coordinator.expire_stale_if_idle(time.monotonic())


if __name__ == "__main__":
    main()