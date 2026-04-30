import random
import time

from common import common_pb2
from example1 import batch_pb2, input_pair_pb2, job_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    JOBS_BATCH_KEY,
    WORKER_STATUS_KEY,
    open_zenoh_session,
)


TOTAL_WORKERS = 10
WORKER_MESSAGES_PER_BATCH = 2
JOBS_PER_BATCH = 3


def main() -> None:
    logger = init_logging("producer")

    with open_zenoh_session() as session:
        jobs_pub = session.declare_publisher(JOBS_BATCH_KEY)
        worker_pub = session.declare_publisher(WORKER_STATUS_KEY)

        logger.info("[producer] publishing jobs batches to %s", JOBS_BATCH_KEY)
        logger.info("[producer] publishing worker messages to %s", WORKER_STATUS_KEY)

        cycle_number = 1

        while True:
            cycle_id = f"cycle-{cycle_number:04d}"

            context = common_pb2.RequestContext(
                request_id=f"req-{cycle_number:04d}",
                created_at_unix_ms=int(time.time() * 1000),
                source="producer",
                tags={
                    "env": "demo",
                    "transport": "zenoh",
                    "mode": "jobs-and-workers",
                },
            )

            jobs_batch = build_jobs_batch(
                cycle_id=cycle_id,
                context=context,
            )

            jobs_pub.put(jobs_batch.SerializeToString())

            logger.info(
                "[producer] sent jobs batch: batch_id=%s total_jobs=%s",
                jobs_batch.batch_id,
                jobs_batch.total_jobs,
            )

            worker_count = WORKER_MESSAGES_PER_BATCH
            worker_ids = random.sample(
                range(1, TOTAL_WORKERS + 1),
                k=worker_count,
            )

            for worker_number in worker_ids:
                worker_msg = input_pair_pb2.WorkerMessage(
                    cycle_id=cycle_id,
                    worker_id=f"worker-{worker_number}",
                    context=context,
                )

                worker_pub.put(worker_msg.SerializeToString())

                logger.info(
                    "[producer] sent worker message: cycle_id=%s worker_id=%s",
                    worker_msg.cycle_id,
                    worker_msg.worker_id,
                )

                time.sleep(0.01)

            cycle_number += 1
            time.sleep(1)


def build_jobs_batch(
    cycle_id: str,
    context: common_pb2.RequestContext,
) -> batch_pb2.BatchRequest:
    batch = batch_pb2.BatchRequest(
        batch_id=f"jobs-{cycle_id}",
        total_jobs=JOBS_PER_BATCH,
        context=context,
    )

    for job_number in range(1, JOBS_PER_BATCH + 1):
        job = job_pb2.Job(
            batch_id=batch.batch_id,
            job_id=job_number,
            payload=job_pb2.WorkPayload(
                text=f"debug-job-{job_number}"
            ),
            context=context,
            steps=["debug-producer-job"],
        )

        batch.jobs.append(job)

    return batch


if __name__ == "__main__":
    main()