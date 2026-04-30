import time

from chores import chores_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    open_zenoh_session,
)


def main() -> None:
    logger = init_logging("consumer")

    with open_zenoh_session() as session:
        logger.info("[consumer] subscribed to %s", ORCHESTRATOR_TO_CONSUMER_KEY)

        def on_summary(sample):
            try:
                summary = chores_pb2.ChoreFilterSummary()
                summary.ParseFromString(sample.payload.to_bytes())

                logger.info(
                    "[consumer] received chore filter summary: "
                    "chores_id=%s people_evaluated=%s total_chores_accepted=%s",
                    summary.chores_id,
                    summary.total_people_evaluated,
                    summary.total_chores_accepted,
                )

                for result in summary.results:
                    accepted = [
                        f"{chore.name}({chore.estimated_minutes}m)"
                        for chore in result.accepted_chores
                    ]
                    rejected = [
                        f"{chore.name}({chore.estimated_minutes}m)"
                        for chore in result.rejected_chores
                    ]

                    logger.info(
                        "[consumer] filter result: "
                        "filter_id=%s person_id=%s available_minutes=%s "
                        "used_minutes=%s remaining_minutes=%s "
                        "accepted=%s rejected=%s",
                        result.filter_id,
                        result.person.person_id,
                        result.person.available_minutes,
                        result.used_minutes,
                        result.remaining_minutes,
                        accepted,
                        rejected,
                    )

            except Exception as exc:
                logger.exception("[consumer] failed to decode output: %s", exc)

        session.declare_subscriber(ORCHESTRATOR_TO_CONSUMER_KEY, on_summary)

        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()