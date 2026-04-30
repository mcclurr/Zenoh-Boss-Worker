import random
import time

from common import common_pb2
from chores import chores_pb2

from bw.common.log import init_logging
from bw.messaging.zenoh import (
    JOBS_BATCH_KEY,
    WORKER_STATUS_KEY,
    open_zenoh_session,
)


PERSON_MESSAGES_PER_BATCH = 3

PUBLISH_INTERVAL_SECONDS = 1.0
PERSON_MESSAGE_GAP_SECONDS = 0.01


STATIC_CHORES = [
    {"name": "wash dishes", "estimated_minutes": 10},
    {"name": "sweep kitchen", "estimated_minutes": 8},
    {"name": "take out trash", "estimated_minutes": 5},
    {"name": "fold laundry", "estimated_minutes": 15},
    {"name": "vacuum living room", "estimated_minutes": 20},
    {"name": "clean bathroom sink", "estimated_minutes": 12},
    {"name": "wipe counters", "estimated_minutes": 6},
    {"name": "water plants", "estimated_minutes": 4},
    {"name": "make bed", "estimated_minutes": 7},
    {"name": "mop hallway", "estimated_minutes": 18},
]


STATIC_PEOPLE = {
    "person-1": 10,
    "person-2": 15,
    "person-3": 20,
    "person-4": 25,
    "person-5": 30,
    "person-6": 35,
    "person-7": 40,
    "person-8": 45,
    "person-9": 50,
    "person-10": 60,
}


def main() -> None:
    logger = init_logging("producer")

    with open_zenoh_session() as session:
        chores_pub = session.declare_publisher(JOBS_BATCH_KEY)
        person_pub = session.declare_publisher(WORKER_STATUS_KEY)

        logger.info("[producer] publishing chores to %s", JOBS_BATCH_KEY)
        logger.info(
            "[producer] publishing person availability messages to %s",
            WORKER_STATUS_KEY,
        )

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
                    "mode": "chores-and-people",
                },
            )

            chores = build_chores(
                cycle_id=cycle_id,
                context=context,
            )

            chores_pub.put(chores.SerializeToString())

            logger.info(
                "[producer] sent chores: chores_id=%s chores=%s",
                chores.chores_id,
                len(chores.chores),
            )

            selected_person_ids = random.sample(
                list(STATIC_PEOPLE.keys()),
                k=PERSON_MESSAGES_PER_BATCH,
            )

            for person_id in selected_person_ids:
                person = chores_pb2.PersonAvailability(
                    cycle_id=cycle_id,
                    person_id=person_id,
                    available_minutes=STATIC_PEOPLE[person_id],
                    context=context,
                )

                person_pub.put(person.SerializeToString())

                logger.info(
                    "[producer] sent person availability: "
                    "cycle_id=%s person_id=%s available_minutes=%s",
                    person.cycle_id,
                    person.person_id,
                    person.available_minutes,
                )

                time.sleep(PERSON_MESSAGE_GAP_SECONDS)

            cycle_number += 1
            time.sleep(PUBLISH_INTERVAL_SECONDS)


def build_chores(
    cycle_id: str,
    context: common_pb2.RequestContext,
) -> chores_pb2.Chores:
    chores = chores_pb2.Chores(
        chores_id=f"chores-{cycle_id}",
        context=context,
    )

    for chore_number, chore_data in enumerate(STATIC_CHORES, start=1):
        chore = chores_pb2.Chore(
            chore_id=f"{cycle_id}-chore-{chore_number}",
            name=chore_data["name"],
            estimated_minutes=chore_data["estimated_minutes"],
        )

        chores.chores.append(chore)

    return chores


if __name__ == "__main__":
    main()