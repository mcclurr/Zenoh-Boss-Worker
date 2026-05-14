import threading
from enum import Enum
from typing import Callable

from chores import chores_pb2


OnPersonComplete = Callable[[chores_pb2.PersonAvailability, bool], None]


class JobSubmissionMode(Enum):
    PER_PERSON = "per_person"
    WINDOW_BATCH = "window_batch"


class WindowExecutor:
    def submit_window(
        self,
        chores: chores_pb2.Chores,
        people: list[chores_pb2.PersonAvailability],
        on_person_complete: OnPersonComplete,
    ) -> None:
        raise NotImplementedError


class PerPersonExecutor(WindowExecutor):
    def __init__(
        self,
        batch_runner,
        logger,
    ) -> None:
        self.batch_runner = batch_runner
        self.logger = logger

    def submit_window(
        self,
        chores: chores_pb2.Chores,
        people: list[chores_pb2.PersonAvailability],
        on_person_complete: OnPersonComplete,
    ) -> None:
        for person in people:
            thread = threading.Thread(
                target=self._run_person_job,
                args=(chores, person, on_person_complete),
                daemon=True,
            )

            thread.start()

            self.logger.info(
                "[executor] started per-person job: "
                "chores_id=%s person_id=%s",
                chores.chores_id,
                person.person_id,
            )

    def _run_person_job(
        self,
        chores: chores_pb2.Chores,
        person: chores_pb2.PersonAvailability,
        on_person_complete: OnPersonComplete,
    ) -> None:
        succeeded = False

        try:
            self.batch_runner.run_chore_filter(
                chores=chores,
                person=person,
            )

            succeeded = True

        except Exception:
            self.logger.exception(
                "[executor] failed per-person job: "
                "chores_id=%s person_id=%s",
                chores.chores_id,
                person.person_id,
            )

        finally:
            on_person_complete(person, succeeded)


def build_window_executor(
    mode: JobSubmissionMode,
    batch_runner,
    logger,
) -> WindowExecutor:
    if mode == JobSubmissionMode.PER_PERSON:
        return PerPersonExecutor(
            batch_runner=batch_runner,
            logger=logger,
        )

    if mode == JobSubmissionMode.WINDOW_BATCH:
        raise NotImplementedError(
            "WINDOW_BATCH mode is not implemented yet"
        )

    raise ValueError(f"Unsupported job submission mode: {mode}")