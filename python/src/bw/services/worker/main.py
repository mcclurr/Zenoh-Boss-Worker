import os
import time

from chores import chores_pb2

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

    logger.info("[worker %s] connected and waiting for chore filter requests", worker_name)

    def callback(ch, method, properties, body):
        try:
            request = chores_pb2.ChoreFilterRequest()
            request.ParseFromString(body)

            logger.info(
                "[worker %s] got chore filter request: "
                "filter_id=%s chores_id=%s person_id=%s chores=%s available_minutes=%s",
                worker_name,
                request.filter_id,
                request.chores.chores_id,
                request.person.person_id,
                len(request.chores.chores),
                request.person.available_minutes,
            )

            result = process_chore_filter_request(
                request=request,
                actual_worker_name=worker_name,
                logger=logger,
            )

            publish_bytes(ch, RESULTS_QUEUE, result.SerializeToString())

            logger.info(
                "[worker %s] sent chore filter result: "
                "filter_id=%s person_id=%s accepted=%s rejected=%s used_minutes=%s remaining_minutes=%s",
                worker_name,
                result.filter_id,
                result.person.person_id,
                len(result.accepted_chores),
                len(result.rejected_chores),
                result.used_minutes,
                result.remaining_minutes,
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


def process_chore_filter_request(
    request: chores_pb2.ChoreFilterRequest,
    actual_worker_name: str,
    logger,
) -> chores_pb2.ChoreFilterResult:
    person = request.person
    available_minutes = person.available_minutes

    accepted_chores: list[chores_pb2.Chore] = []
    rejected_chores: list[chores_pb2.Chore] = []

    used_minutes = 0

    for chore in request.chores.chores:
        if used_minutes + chore.estimated_minutes <= available_minutes:
            accepted_chores.append(chore)
            used_minutes += chore.estimated_minutes

            logger.info(
                "[worker %s] accepted chore: filter_id=%s person_id=%s "
                "chore_id=%s name=%s estimated_minutes=%s used_minutes=%s",
                actual_worker_name,
                request.filter_id,
                person.person_id,
                chore.chore_id,
                chore.name,
                chore.estimated_minutes,
                used_minutes,
            )
        else:
            rejected_chores.append(chore)

            logger.info(
                "[worker %s] rejected chore: filter_id=%s person_id=%s "
                "chore_id=%s name=%s estimated_minutes=%s used_minutes=%s available_minutes=%s",
                actual_worker_name,
                request.filter_id,
                person.person_id,
                chore.chore_id,
                chore.name,
                chore.estimated_minutes,
                used_minutes,
                available_minutes,
            )

    time.sleep(WORKER_SLEEP_SECONDS)

    result = chores_pb2.ChoreFilterResult(
        filter_id=request.filter_id,
        chores_id=request.chores.chores_id,
        person=person,
        used_minutes=used_minutes,
        remaining_minutes=max(available_minutes - used_minutes, 0),
        context=request.context,
    )

    result.accepted_chores.extend(accepted_chores)
    result.rejected_chores.extend(rejected_chores)

    return result


if __name__ == "__main__":
    main()