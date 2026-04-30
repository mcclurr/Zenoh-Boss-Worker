import os
import time

from example1 import batch_pb2, job_pb2, result_pb2

from bw.common.log import init_logging
from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    RESULTS_QUEUE,
    connect_with_retry,
    declare_queues,
    get_hostname,
    publish_bytes,
)


WORKER_SLEEP_SECONDS = float(os.getenv("WORKER_SLEEP_SECONDS", ""))


def main() -> None:
    worker_name = os.getenv("WORKER_NAME", get_hostname())
    logger = init_logging(worker_name)

    connection = connect_with_retry(logger=logger)
    channel = connection.channel()
    declare_queues(channel)

    channel.basic_qos(prefetch_count=1)

    logger.info("[worker %s] connected and waiting for worker-batch requests", worker_name)

    def callback(ch, method, properties, body):
        try:
            request = batch_pb2.WorkerBatchRequest()
            request.ParseFromString(body)

            logger.info(
                "[worker %s] got worker-batch request: "
                "batch_id=%s source_batch_id=%s selected_worker_id=%s jobs=%s",
                worker_name,
                request.batch_id,
                request.jobs_batch.batch_id,
                request.worker.worker_id,
                len(request.jobs_batch.jobs),
            )

            result = process_worker_batch_request(
                request=request,
                actual_worker_name=worker_name,
                logger=logger,
            )

            publish_bytes(ch, RESULTS_QUEUE, result.SerializeToString())

            logger.info(
                "[worker %s] sent worker-batch result: "
                "batch_id=%s result_id=%s result=%s",
                worker_name,
                result.batch_id,
                result.job_id,
                result.result,
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:
            logger.exception("[worker %s] error: %s", worker_name, exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(
        queue=JOBS_QUEUE,
        on_message_callback=callback,
        auto_ack=False,
    )

    channel.start_consuming()


def process_worker_batch_request(
    request: batch_pb2.WorkerBatchRequest,
    actual_worker_name: str,
    logger,
) -> result_pb2.JobResult:
    jobs_batch = request.jobs_batch
    selected_worker = request.worker

    processed_outputs: list[str] = []

    for job in jobs_batch.jobs:
        payload_value = _payload_to_string(job.payload)

        logger.info(
            "[worker %s] processing bundled job: "
            "request_batch_id=%s selected_worker_id=%s job_id=%s payload=%s",
            actual_worker_name,
            request.batch_id,
            selected_worker.worker_id,
            job.job_id,
            payload_value,
        )

        processed_outputs.append(
            f"job_id={job.job_id}:processed-{payload_value}"
        )

    time.sleep(WORKER_SLEEP_SECONDS)

    result = result_pb2.JobResult(
        batch_id=request.batch_id,
        job_id=1,
        worker=actual_worker_name,
        result=(
            f"selected_worker={selected_worker.worker_id}; "
            f"processed_jobs={len(jobs_batch.jobs)}; "
            f"outputs=[{', '.join(processed_outputs)}]"
        ),
        processing_seconds=WORKER_SLEEP_SECONDS,
        context=request.context,
        warnings=[],
    )

    return result


def _payload_to_string(payload: job_pb2.WorkPayload) -> str:
    which = payload.WhichOneof("kind")

    if which == "text":
        return payload.text

    if which == "numeric_value":
        return str(payload.numeric_value)

    if which == "raw_bytes":
        return "<bytes>"

    return ""


if __name__ == "__main__":
    main()