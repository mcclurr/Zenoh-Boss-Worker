# python/src/bw/services/orchestrator/handler.py

from chores import chores_pb2

from bw.services.orchestrator.coordinator import BatchCoordinator


class OrchestratorHandler:
    def __init__(self, coordinator: BatchCoordinator, logger) -> None:
        self.coordinator = coordinator
        self.logger = logger

    def on_chores_sample(self, sample) -> None:
        try:
            chores = chores_pb2.Chores()
            chores.ParseFromString(sample.payload.to_bytes())
            self.coordinator.receive_chores(chores)
        except Exception:
            self.logger.exception("[orchestrator-handler] failed to process chores sample")

    def on_person_sample(self, sample) -> None:
        try:
            person = chores_pb2.PersonAvailability()
            person.ParseFromString(sample.payload.to_bytes())
            self.coordinator.receive_person(person)
        except Exception:
            self.logger.exception("[orchestrator-handler] failed to process person sample")