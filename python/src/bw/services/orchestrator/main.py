import time

from example1 import batch_pb2, job_pb2, result_pb2

from bw.common.log import init_logging
from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    RESULTS_QUEUE,
    connect_with_retry,
    declare_queues,
    publish_bytes,
)
from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    PRODUCER_TO_ORCHESTRATOR_KEY,
    open_zenoh_session,
)


def process_batch(
    batch: batch_pb2.BatchRequest,
    logger,
    rabbit_channel,
    zenoh_pub,
) -> None:
    batch_id = batch.batch_id
    total_jobs = batch.total_jobs

    logger.info(f"[orchestrator] starting batch_id={batch_id} total_jobs={total_jobs}")

    for job in batch.jobs:
        publish_bytes(rabbit_channel, JOBS_QUEUE, job.SerializeToString())
        logger.info(
            f"[orchestrator] queued job: batch_id={job.batch_id} "
            f"job_id={job.job_id}"
        )

    received_results = 0
    seen_job_ids = set()
    results = []

    logger.info(
        f"[orchestrator] waiting for {total_jobs} results for batch_id={batch_id}"
    )

    while received_results < total_jobs:
        method_frame, header_frame, body = rabbit_channel.basic_get(
            queue=RESULTS_QUEUE,
            auto_ack=False,
        )

        if method_frame is None:
            time.sleep(0.25)
            continue

        try:
            result = result_pb2.JobResult()
            result.ParseFromString(body)

            if result.batch_id != batch_id:
                logger.warning(
                    "[orchestrator] ignoring stale/non-matching result: "
                    f"batch_id={result.batch_id} job_id={result.job_id}"
                )
                rabbit_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                continue

            job_id = result.job_id

            if job_id not in seen_job_ids:
                seen_job_ids.add(job_id)
                received_results += 1
                results.append(result)
                logger.info(
                    f"[orchestrator] received result {received_results}/{total_jobs}: "
                    f"batch_id={result.batch_id} job_id={result.job_id} "
                    f"worker={result.worker} result={result.result}"
                )
            else:
                logger.warning(
                    f"[orchestrator] duplicate result ignored: "
                    f"batch_id={result.batch_id} job_id={result.job_id}"
                )

            rabbit_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as exc:
            logger.exception(f"[orchestrator] error processing result: {exc}")
            rabbit_channel.basic_nack(
                delivery_tag=method_frame.delivery_tag,
                requeue=True,
            )

    summary = batch_pb2.BatchSummary(
        batch_id=batch_id,
        total_jobs=total_jobs,
        results_received=len(results),
        context=batch.context,
    )
    summary.results.extend(results)

    zenoh_pub.put(summary.SerializeToString())
    logger.info(
        f"[orchestrator] published batch summary to zenoh: "
        f"batch_id={summary.batch_id} total_jobs={summary.total_jobs} "
        f"results_received={summary.results_received}"
    )


def main() -> None:
    logger = init_logging("orchestrator-python")

    rabbit_connection = connect_with_retry(logger=logger)
    rabbit_channel = rabbit_connection.channel()
    declare_queues(rabbit_channel)

    logger.info("[orchestrator] connected to RabbitMQ")

    with open_zenoh_session() as zenoh_session:
        logger.info("[orchestrator] connected to Zenoh")

        consumer_pub = zenoh_session.declare_publisher(ORCHESTRATOR_TO_CONSUMER_KEY)

        def on_batch(sample):
            try:
                batch = batch_pb2.BatchRequest()
                batch.ParseFromString(sample.payload.to_bytes())

                logger.info(
                    f"[orchestrator] received batch request: "
                    f"batch_id={batch.batch_id} total_jobs={batch.total_jobs}"
                )
                process_batch(batch, logger, rabbit_channel, consumer_pub)
            except Exception as exc:
                logger.exception(
                    f"[orchestrator] failed to process zenoh batch request: {exc}"
                )

        zenoh_session.declare_subscriber(PRODUCER_TO_ORCHESTRATOR_KEY, on_batch)
        logger.info(f"[orchestrator] subscribed to {PRODUCER_TO_ORCHESTRATOR_KEY}")

        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()