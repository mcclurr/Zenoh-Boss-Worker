import logging
import os
import socket
import time

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
JOBS_QUEUE = "jobs"
RESULTS_QUEUE = "results"


def get_hostname() -> str:
    return socket.gethostname()


def connect_with_retry(
    retries: int = 30,
    delay: float = 2.0,
    logger: logging.Logger | None = None,
) -> pika.BlockingConnection:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                heartbeat=60,
            )
            connection = pika.BlockingConnection(parameters)
            return connection
        except Exception as exc:
            last_error = exc
            message = f"[connect] attempt {attempt}/{retries} failed: {exc}"
            if logger is not None:
                logger.warning(message)
            else:
                print(message)
            time.sleep(delay)

    raise RuntimeError(
        f"Could not connect to RabbitMQ after {retries} attempts: {last_error}"
    )


def declare_queues(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.queue_declare(queue=JOBS_QUEUE, durable=True)
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)


def publish_bytes(channel, queue_name: str, payload: bytes) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=payload,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/octet-stream",
        ),
    )