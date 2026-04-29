import time

from common import common_pb2
from example1 import batch_pb2, job_pb2

from rbw.common.log import init_logging
from rbw.messaging.zenoh import (
    PRODUCER_TO_ORCHESTRATOR_KEY,
    open_zenoh_session,
)


def main() -> None:
    logger = init_logging("producer")

    with open_zenoh_session() as session:
        pub = session.declare_publisher(PRODUCER_TO_ORCHESTRATOR_KEY)
        logger.info(f"[producer] publishing to {PRODUCER_TO_ORCHESTRATOR_KEY}")

        batch_number = 1

        while True:
            context = common_pb2.RequestContext(
                request_id=f"req-{batch_number:04d}",
                created_at_unix_ms=int(time.time() * 1000),
                source="producer",
                tags={"env": "demo", "transport": "zenoh"},
            )

            jobs = []
            for job_id in range(1, 6):
                payload = job_pb2.WorkPayload(text=f"dummy-work-{job_id}")
                job = job_pb2.Job(
                    batch_id=f"batch-{batch_number:04d}",
                    job_id=job_id,
                    payload=payload,
                    context=context,
                    steps=["validate", "transform", "publish"],
                )
                jobs.append(job)

            batch = batch_pb2.BatchRequest(
                batch_id=f"batch-{batch_number:04d}",
                total_jobs=len(jobs),
                context=context,
                jobs=jobs,
            )

            pub.put(batch.SerializeToString())
            logger.info(
                f"[producer] sent batch: batch_id={batch.batch_id} "
                f"total_jobs={batch.total_jobs}"
            )

            batch_number += 1
            time.sleep(10)


if __name__ == "__main__":
    main()