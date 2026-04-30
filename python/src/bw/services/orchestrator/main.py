import os
import time

from bw.common.log import init_logging
from bw.messaging.rabbitmq import (
    connect_with_retry,
    declare_queues,
)
from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    TOPIC_A_KEY,
    TOPIC_B_KEY,
    open_zenoh_session,
)
from bw.services.orchestrator.batch_runner import BatchRunner
from bw.services.orchestrator.coordinator import BatchCoordinator


MATCH_WINDOW_SECONDS = float(os.getenv("MATCH_WINDOW_SECONDS", ""))


def main() -> None:
    logger = init_logging("orchestrator-python")

    rabbit_connection = connect_with_retry(logger=logger)
    rabbit_channel = rabbit_connection.channel()
    declare_queues(rabbit_channel)

    logger.info("[orchestrator] connected to RabbitMQ")

    with open_zenoh_session() as zenoh_session:
        logger.info("[orchestrator] connected to Zenoh")

        consumer_pub = zenoh_session.declare_publisher(ORCHESTRATOR_TO_CONSUMER_KEY)

        batch_runner = BatchRunner(
            rabbit_channel=rabbit_channel,
            consumer_pub=consumer_pub,
            logger=logger,
        )

        coordinator = BatchCoordinator(
            batch_runner=batch_runner,
            logger=logger,
            match_window_seconds=MATCH_WINDOW_SECONDS,
        )

        zenoh_session.declare_subscriber(TOPIC_A_KEY, coordinator.on_topic_a)
        zenoh_session.declare_subscriber(TOPIC_B_KEY, coordinator.on_topic_b)

        logger.info(
            "[orchestrator] subscribed to topic_a=%s topic_b=%s "
            "match_window=%.3fs",
            TOPIC_A_KEY,
            TOPIC_B_KEY,
            MATCH_WINDOW_SECONDS,
        )

        while True:
            time.sleep(0.1)
            coordinator.expire_stale_if_idle(time.monotonic())


if __name__ == "__main__":
    main()