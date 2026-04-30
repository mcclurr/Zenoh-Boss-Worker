import os
import random
import time

from example1 import job_pb2, result_pb2

from bw.common.log import init_logging
from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    RESULTS_QUEUE,
    connect_with_retry,
    declare_queues,
    get_hostname,
    publish_bytes,
)


def main() -> None:
    worker_name = os.getenv("WORKER_NAME", "")
    logger = init_logging(worker_name)

    connection = connect_with_retry(logger=logger)
    channel = connection.channel()
    declare_queues(channel)

    channel.basic_qos(prefetch_count=1)

    logger.info(f"[worker {worker_name}] connected and waiting for jobs")

    def callback(ch, method, properties, body):
        try:
            job = job_pb2.Job()
            job.ParseFromString(body)

            logger.info(
                f"[worker {worker_name}] got job: "
                f"batch_id={job.batch_id} job_id={job.job_id}"
            )

            sleep_time = random.uniform(3.0, 5.0)
            time.sleep(sleep_time)

            payload_value = ""
            which = job.payload.WhichOneof("kind")
            if which == "text":
                payload_value = job.payload.text
            elif which == "numeric_value":
                payload_value = str(job.payload.numeric_value)
            elif which == "raw_bytes":
                payload_value = "<bytes>"

            result = result_pb2.JobResult(
                batch_id=job.batch_id,
                job_id=job.job_id,
                worker=worker_name,
                result=f"processed-{payload_value}",
                processing_seconds=round(sleep_time, 2),
                context=job.context,
            )

            publish_bytes(ch, RESULTS_QUEUE, result.SerializeToString())

            logger.info(
                f"[worker {worker_name}] sent result: "
                f"batch_id={result.batch_id} job_id={result.job_id} "
                f"result={result.result}"
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:
            logger.exception(f"[worker {worker_name}] error: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(
        queue=JOBS_QUEUE,
        on_message_callback=callback,
        auto_ack=False,
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()