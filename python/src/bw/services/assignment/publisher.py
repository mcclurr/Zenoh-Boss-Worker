import os
import queue
import time
import uuid

from assignment import assignment_pb2
from common import common_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import open_zenoh_session


REQUEST_KEY = os.getenv("ASSIGNMENT_REQUEST_KEY", "demo/assignment/request")
RESULT_KEY = os.getenv("ASSIGNMENT_RESULT_KEY", "demo/assignment/result")
RESULT_TIMEOUT_SECONDS = float(os.getenv("RESULT_TIMEOUT_SECONDS", "10"))
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "3"))


def build_request() -> assignment_pb2.AssignmentRequest:
    assignment_id = f"assignment-{uuid.uuid4()}"

    context = common_pb2.RequestContext(
        request_id=f"req-{uuid.uuid4()}",
        created_at_unix_ms=int(time.time() * 1000),
        source="assignment-demo-publisher",
        tags={"mode": "demo"},
    )

    request = assignment_pb2.AssignmentRequest(
        assignment_id=assignment_id,
        context=context,
    )

    request.workers.extend([
        assignment_pb2.Worker(worker_id="alice", name="Alice"),
        assignment_pb2.Worker(worker_id="bob", name="Bob"),
        assignment_pb2.Worker(worker_id="carol", name="Carol"),
    ])

    request.jobs.extend([
        assignment_pb2.Job(job_id="dishes", name="Wash dishes"),
        assignment_pb2.Job(job_id="laundry", name="Fold laundry"),
        assignment_pb2.Job(job_id="vacuum", name="Vacuum"),
    ])

    request.costs.extend([
        assignment_pb2.AssignmentCost(worker_id="alice", job_id="dishes", cost=1),
        assignment_pb2.AssignmentCost(worker_id="alice", job_id="laundry", cost=10),
        assignment_pb2.AssignmentCost(worker_id="alice", job_id="vacuum", cost=5),

        assignment_pb2.AssignmentCost(worker_id="bob", job_id="dishes", cost=10),
        assignment_pb2.AssignmentCost(worker_id="bob", job_id="laundry", cost=1),
        assignment_pb2.AssignmentCost(worker_id="bob", job_id="vacuum", cost=5),

        assignment_pb2.AssignmentCost(worker_id="carol", job_id="dishes", cost=5),
        assignment_pb2.AssignmentCost(worker_id="carol", job_id="laundry", cost=5),
        assignment_pb2.AssignmentCost(worker_id="carol", job_id="vacuum", cost=1),
    ])

    return request


def log_result(logger, result: assignment_pb2.AssignmentResult) -> None:
    logger.info(
        "[publisher] received assignment result: assignment_id=%s total_cost=%s",
        result.assignment_id,
        result.total_cost,
    )

    for a in result.assignments:
        logger.info(
            "[publisher] assignment: worker=%s job=%s cost=%s",
            a.worker_id,
            a.job_id,
            a.cost,
        )


def main():
    logger = init_logging("assignment-demo")
    result_queue: queue.Queue[assignment_pb2.AssignmentResult] = queue.Queue()

    with open_zenoh_session() as session:
        request_pub = session.declare_publisher(REQUEST_KEY)

        def on_result(sample):
            try:
                result = assignment_pb2.AssignmentResult()
                result.ParseFromString(sample.payload.to_bytes())
                result_queue.put(result)

            except Exception as exc:
                logger.exception("[publisher] failed to decode result: %s", exc)

        session.declare_subscriber(RESULT_KEY, on_result)

        logger.info(
            "[publisher] ready: request_key=%s result_key=%s timeout=%ss",
            REQUEST_KEY,
            RESULT_KEY,
            RESULT_TIMEOUT_SECONDS,
        )

        while True:
            request = build_request()

            request_pub.put(request.SerializeToString())

            logger.info(
                "[publisher] sent assignment request: assignment_id=%s workers=%s jobs=%s",
                request.assignment_id,
                len(request.workers),
                len(request.jobs),
            )

            while True:
                try:
                    result = result_queue.get(timeout=RESULT_TIMEOUT_SECONDS)
                except queue.Empty:
                    logger.warning(
                        "[publisher] timed out waiting for result: assignment_id=%s",
                        request.assignment_id,
                    )
                    break

                if result.assignment_id != request.assignment_id:
                    logger.warning(
                        "[publisher] ignoring result for different assignment: "
                        "expected=%s actual=%s",
                        request.assignment_id,
                        result.assignment_id,
                    )
                    continue

                log_result(logger, result)
                break

            time.sleep(PUBLISH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()