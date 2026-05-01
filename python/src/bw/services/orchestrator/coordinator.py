import os
import threading
import time
from dataclasses import dataclass

from chores import chores_pb2

from bw.services.orchestrator.batch_runner import BatchRunner


PERSON_GATHER_WINDOW_SECONDS = float(
    os.getenv("PERSON_GATHER_WINDOW_SECONDS", "")
)

MAX_ACTIVE_FILTERS = int(os.getenv("MAX_ACTIVE_FILTERS", ""))

PERSON_LAST_SUCCESS_TTL_SECONDS = float(
    os.getenv("PERSON_LAST_SUCCESS_TTL_SECONDS", "")
)


@dataclass
class PendingPerson:
    message: chores_pb2.PersonAvailability
    received_monotonic: float
    sequence_number: int


@dataclass
class ChoresWindow:
    chores: chores_pb2.Chores
    received_monotonic: float
    pending_people: dict[str, PendingPerson]
    timer: threading.Timer | None
    sequence_number: int = 0


class BatchCoordinator:
    def __init__(
        self,
        batch_runner: BatchRunner,
        logger,
        match_window_seconds: float,
    ) -> None:
        self.batch_runner = batch_runner
        self.logger = logger
        self.match_window_seconds = match_window_seconds

        self.lock = threading.Lock()
        self.current_window: ChoresWindow | None = None

        self.active_filter_count = 0

        # person_id -> last selected monotonic timestamp
        self.person_last_selected: dict[str, float] = {}

    def on_topic_a(self, sample) -> None:
        chores = chores_pb2.Chores()
        chores.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.current_window is not None:
                dropped_people = len(self.current_window.pending_people)

                if self.current_window.timer is not None:
                    self.current_window.timer.cancel()

                self.logger.info(
                    "[orchestrator] replacing unflushed chores window: "
                    "old_chores_id=%s dropped_people=%s",
                    self.current_window.chores.chores_id,
                    dropped_people,
                )

            timer = threading.Timer(
                PERSON_GATHER_WINDOW_SECONDS,
                self._flush_current_window,
            )
            timer.daemon = True

            self.current_window = ChoresWindow(
                chores=chores,
                received_monotonic=now,
                pending_people={},
                timer=timer,
            )

            timer.start()

        self.logger.info(
            "[orchestrator] received chores: chores_id=%s chores=%s "
            "person_gather_window=%.3fs",
            chores.chores_id,
            len(chores.chores),
            PERSON_GATHER_WINDOW_SECONDS,
        )

    def on_topic_b(self, sample) -> None:
        person = chores_pb2.PersonAvailability()
        person.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.current_window is None:
                self.logger.info(
                    "[orchestrator] dropping person availability because no active "
                    "chores window exists: cycle_id=%s person_id=%s",
                    person.cycle_id,
                    person.person_id,
                )
                return

            self.current_window.sequence_number += 1

            self.current_window.pending_people[person.person_id] = PendingPerson(
                message=person,
                received_monotonic=now,
                sequence_number=self.current_window.sequence_number,
            )

            pending_count = len(self.current_window.pending_people)
            chores_id = self.current_window.chores.chores_id

        self.logger.info(
            "[orchestrator] received person availability for current chores: "
            "chores_id=%s cycle_id=%s person_id=%s available_minutes=%s "
            "pending_unique_people=%s",
            chores_id,
            person.cycle_id,
            person.person_id,
            person.available_minutes,
            pending_count,
        )

    def expire_stale_if_idle(self, now: float) -> None:
        with self.lock:
            self._prune_person_history_locked(now)

    def _flush_current_window(self) -> None:
        with self.lock:
            if self.current_window is None:
                return

            window = self.current_window
            self.current_window = None

            available_slots = MAX_ACTIVE_FILTERS - self.active_filter_count

            if available_slots <= 0:
                dropped_count = len(window.pending_people)

                self.logger.info(
                    "[orchestrator] dropping chores window because no filter slots "
                    "are available: chores_id=%s active=%s max=%s "
                    "dropped_people=%s",
                    window.chores.chores_id,
                    self.active_filter_count,
                    MAX_ACTIVE_FILTERS,
                    dropped_count,
                )
                return

            if not window.pending_people:
                self.logger.info(
                    "[orchestrator] dropping chores window because no person "
                    "availability messages arrived: chores_id=%s",
                    window.chores.chores_id,
                )
                return

            people_to_run = self._choose_people_to_run_locked(
                people=list(window.pending_people.values()),
                max_people=available_slots,
            )

            selected_person_ids = {
                pending_person.message.person_id
                for pending_person in people_to_run
            }

            dropped_person_ids = [
                person_id
                for person_id in window.pending_people.keys()
                if person_id not in selected_person_ids
            ]

            chores = chores_pb2.Chores()
            chores.CopyFrom(window.chores)

            now = time.monotonic()

            for pending_person in people_to_run:
                person = chores_pb2.PersonAvailability()
                person.CopyFrom(pending_person.message)

                self.active_filter_count += 1
                self.person_last_selected[person.person_id] = now

                thread = threading.Thread(
                    target=self._run_filter_thread,
                    args=(chores, person),
                    daemon=True,
                )
                thread.start()

            self.logger.info(
                "[orchestrator] flushed chores window: chores_id=%s selected=%s "
                "dropped=%s active=%s max=%s",
                chores.chores_id,
                list(selected_person_ids),
                dropped_person_ids,
                self.active_filter_count,
                MAX_ACTIVE_FILTERS,
            )

    def _choose_people_to_run_locked(
        self,
        people: list[PendingPerson],
        max_people: int,
    ) -> list[PendingPerson]:
        prioritized = sorted(
            people,
            key=lambda pending_person: (
                self.person_last_selected.get(
                    pending_person.message.person_id,
                    0.0,
                ),
                pending_person.sequence_number,
            ),
        )

        return prioritized[:max_people]

    def _run_filter_thread(
        self,
        chores: chores_pb2.Chores,
        person: chores_pb2.PersonAvailability,
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
                "[orchestrator] failed to run chore filter: person_id=%s",
                person.person_id,
            )

        finally:
            now = time.monotonic()

            with self.lock:
                self.active_filter_count -= 1

                if not succeeded:
                    self.person_last_selected.pop(person.person_id, None)

                self._prune_person_history_locked(now)

    def _prune_person_history_locked(self, now: float) -> None:
        stale_person_ids = [
            person_id
            for person_id, last_selected in self.person_last_selected.items()
            if now - last_selected > PERSON_LAST_SUCCESS_TTL_SECONDS
        ]

        for person_id in stale_person_ids:
            del self.person_last_selected[person_id]

        if stale_person_ids:
            self.logger.info(
                "[orchestrator] pruned person history: count=%s",
                len(stale_person_ids),
            )