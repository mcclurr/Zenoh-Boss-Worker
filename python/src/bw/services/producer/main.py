import random
import time

from common import common_pb2
from example1 import input_pair_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    TOPIC_A_KEY,
    TOPIC_B_KEY,
    open_zenoh_session,
)


def main() -> None:
    logger = init_logging("producer")

    with open_zenoh_session() as session:
        topic_a_pub = session.declare_publisher(TOPIC_A_KEY)
        topic_b_pub = session.declare_publisher(TOPIC_B_KEY)

        logger.info(f"[producer] publishing topic A to {TOPIC_A_KEY}")
        logger.info(f"[producer] publishing topic B to {TOPIC_B_KEY}")

        cycle_number = 1

        while True:
            cycle_id = f"cycle-{cycle_number:04d}"

            context = common_pb2.RequestContext(
                request_id=f"req-{cycle_number:04d}",
                created_at_unix_ms=int(time.time() * 1000),
                source="producer",
                tags={"env": "demo", "transport": "zenoh", "mode": "two-topic"},
            )

            topic_a = input_pair_pb2.TopicAMessage(
                cycle_id=cycle_id,
                text=f"message-a-{cycle_number}",
                context=context,
            )

            topic_b = input_pair_pb2.TopicBMessage(
                cycle_id=cycle_id,
                value=random.randint(100, 999),
                context=context,
            )

            topic_a_pub.put(topic_a.SerializeToString())
            logger.info(
                f"[producer] sent topic A: cycle_id={topic_a.cycle_id} "
                f"text={topic_a.text}"
            )

            time.sleep(0.1)

            topic_b_pub.put(topic_b.SerializeToString())
            logger.info(
                f"[producer] sent topic B: cycle_id={topic_b.cycle_id} "
                f"value={topic_b.value}"
            )

            cycle_number += 1
            time.sleep(1)


if __name__ == "__main__":
    main()