import threading
import time
from dataclasses import dataclass

from chores import chores_pb2

from bw.services.orchestrator.config import OrchestratorConfig
from bw.services.orchestrator.executor import WindowExecutor


@dataclass
class PendingPerson:
    message: chores_pb2.PersonAvailability
    received_monotonic: float
    sequence_number: int


@dataclass
class PersonWindow:
    received_monotonic: float
    pending_people: dict[str, PendingPerson]
    timer: threading.Timer | None
    sequence_number: int = 0


class BatchCoordinator:
    def __init__(
        self,
        executor: WindowExecutor,
        config: OrchestratorConfig,
        logger,
    ) -> None:
        self.executor = executor
        self.config = config
        self.logger = logger

        self.lock = threading.Lock()

        # Latest known chores state.
        self.latest_chores: chores_pb2.Chores | None = None
        self.latest_chores_received_monotonic: float | None = None

        # Active person/job collection window.
        self.current_window: PersonWindow | None = None

        self.active_filter_count = 0

        # person_id -> last selected monotonic timestamp
        self.person_last_selected: dict[str, float] = {}

    def receive_chores(
        self,
        chores: chores_pb2.Chores,
    ) -> None:
        """
        Receive chores.

        Chores do not open processing windows. They are stored as the latest
        known chores state and used when the current person window flushes.
        """
        now = time.monotonic()

        with self.lock:
            old_chores_id = (
                self.latest_chores.chores_id
                if self.latest_chores is not None
                else None
            )

            self.latest_chores = chores
            self.latest_chores_received_monotonic = now

        self.logger.info(
            "[orchestrator] stored latest chores: "
            "chores_id=%s chores=%s previous_chores_id=%s",
            chores.chores_id,
            len(chores.chores),
            old_chores_id,
        )

    def receive_person(
        self,
        person: chores_pb2.PersonAvailability,
    ) -> None:
        """
        Receive person/job availability.

        Person messages drive the processing window. The first person message
        opens the window and starts the timer. Additional people may join until
        the timer expires.
        """
        now = time.monotonic()

        with self.lock:
            if self.current_window is None:
                timer = threading.Timer(
                    self.config.person_gather_window_seconds,
                    self._flush_current_window,
                )

                timer.daemon = True

                self.current_window = PersonWindow(
                    received_monotonic=now,
                    pending_people={},
                    timer=timer,
                )

                timer.start()

                self.logger.info(
                    "[orchestrator] opened person window: "
                    "first_cycle_id=%s first_person_id=%s "
                    "person_gather_window=%.3fs",
                    person.cycle_id,
                    person.person_id,
                    self.config.person_gather_window_seconds,
                )

            self.current_window.sequence_number += 1

            self.current_window.pending_people[person.person_id] = PendingPerson(
                message=person,
                received_monotonic=now,
                sequence_number=self.current_window.sequence_number,
            )

            pending_count = len(self.current_window.pending_people)
            sequence_number = self.current_window.sequence_number

            latest_chores_id = (
                self.latest_chores.chores_id
                if self.latest_chores is not None
                else None
            )

        self.logger.info(
            "[orchestrator] received person availability: "
            "cycle_id=%s person_id=%s available_minutes=%s "
            "pending_unique_people=%s sequence_number=%s "
            "latest_chores_id=%s",
            person.cycle_id,
            person.person_id,
            person.available_minutes,
            pending_count,
            sequence_number,
            latest_chores_id,
        )

    def _on_person_complete(
        self,
        person: chores_pb2.PersonAvailability,
        succeeded: bool,
    ) -> None:
        now = time.monotonic()

        with self.lock:
            self.active_filter_count -= 1

            if self.active_filter_count < 0:
                self.logger.warning(
                    "[orchestrator] active filter count went negative"
                )
                self.active_filter_count = 0

            if not succeeded:
                self.person_last_selected.pop(
                    person.person_id,
                    None,
                )

            self._prune_person_history_locked(now)

        self.logger.info(
            "[orchestrator] completed person job: "
            "person_id=%s succeeded=%s active=%s max=%s",
            person.person_id,
            succeeded,
            self.active_filter_count,
            self.config.max_active_filters,
        )

    def expire_stale_if_idle(
        self,
        now: float,
    ) -> None:
        with self.lock:
            self._prune_person_history_locked(now)

    def _flush_current_window(self) -> None:
        with self.lock:
            if self.current_window is None:
                return

            window = self.current_window
            self.current_window = None

            if self.latest_chores is None:
                dropped_count = len(window.pending_people)

                self.logger.info(
                    "[orchestrator] dropping person window because no latest "
                    "chores message is available: dropped_people=%s",
                    dropped_count,
                )

                return

            if not window.pending_people:
                self.logger.info(
                    "[orchestrator] dropping person window because no person "
                    "availability messages arrived"
                )

                return

            if self.active_filter_count > 0:
                dropped_count = len(window.pending_people)

                self.logger.info(
                    "[orchestrator] dropping person window because another batch is still "
                    "active: latest_chores_id=%s active=%s max=%s dropped_people=%s",
                    self.latest_chores.chores_id,
                    self.active_filter_count,
                    self.config.max_active_filters,
                    dropped_count,
                )

                return

            people_to_run = self._choose_people_to_run_locked(
                people=list(window.pending_people.values()),
                max_people=self.config.max_active_filters,
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
            chores.CopyFrom(self.latest_chores)

            now = time.monotonic()

            people_messages = []

            for pending_person in people_to_run:
                person = chores_pb2.PersonAvailability()
                person.CopyFrom(pending_person.message)

                self.active_filter_count += 1
                self.person_last_selected[person.person_id] = now

                people_messages.append(person)

            self.executor.submit_window(
                chores=chores,
                people=people_messages,
                on_person_complete=self._on_person_complete,
            )

            self.logger.info(
                "[orchestrator] flushed person window using latest chores: "
                "chores_id=%s selected=%s dropped=%s active=%s max=%s",
                chores.chores_id,
                list(selected_person_ids),
                dropped_person_ids,
                self.active_filter_count,
                self.config.max_active_filters,
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

    def _prune_person_history_locked(
        self,
        now: float,
    ) -> None:
        stale_person_ids = [
            person_id
            for person_id, last_selected
            in self.person_last_selected.items()
            if (
                now - last_selected
                > self.config.person_last_success_ttl_seconds
            )
        ]

        for person_id in stale_person_ids:
            del self.person_last_selected[person_id]

        if stale_person_ids:
            self.logger.info(
                "[orchestrator] pruned person history: count=%s",
                len(stale_person_ids),
            )