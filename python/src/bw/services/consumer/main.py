import time

from example1 import batch_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    open_zenoh_session,
)


def main() -> None:
    logger = init_logging("consumer")

    with open_zenoh_session() as session:
        logger.info(f"[consumer] subscribed to {ORCHESTRATOR_TO_CONSUMER_KEY}")

        def on_summary(sample):
            try:
                summary = batch_pb2.BatchSummary()
                summary.ParseFromString(sample.payload.to_bytes())

                logger.info(
                    f"[consumer] received summary: "
                    f"batch_id={summary.batch_id} "
                    f"total_jobs={summary.total_jobs} "
                    f"results_received={summary.results_received}"
                )

                for result in summary.results:
                    logger.info(
                        f"[consumer] result: "
                        f"job_id={result.job_id} worker={result.worker} "
                        f"result={result.result}"
                    )
            except Exception as exc:
                logger.exception(f"[consumer] failed to decode output: {exc}")

        session.declare_subscriber(ORCHESTRATOR_TO_CONSUMER_KEY, on_summary)

        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()