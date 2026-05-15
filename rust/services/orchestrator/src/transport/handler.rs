use std::sync::{Arc, Mutex};

use prost::Message;

use bw_core::proto::demo::chores::{
    Chores,
    PersonAvailability,
};

use crate::coordination::coordinator::BatchCoordinator;

pub struct OrchestratorHandler {
    coordinator: Arc<Mutex<BatchCoordinator>>,
}

impl OrchestratorHandler {
    pub fn new(
        coordinator: Arc<Mutex<BatchCoordinator>>,
    ) -> Self {
        Self {
            coordinator,
        }
    }

    pub fn on_chores_bytes(
        &self,
        bytes: &[u8],
    ) {
        match Chores::decode(bytes) {
            Ok(chores) => {
                println!(
                    "[handler] received chores: chores_id={}",
                    chores.chores_id,
                );

                if let Ok(mut coordinator) =
                    self.coordinator.lock()
                {
                    coordinator.receive_chores(chores);
                }
            }

            Err(error) => {
                eprintln!(
                    "[handler] failed to decode chores: {:?}",
                    error,
                );
            }
        }
    }

    pub fn on_person_bytes(
        &self,
        bytes: &[u8],
    ) {
        match PersonAvailability::decode(bytes) {
            Ok(person) => {
                println!(
                    "[handler] received person: person_id={}",
                    person.person_id,
                );

                if let Ok(mut coordinator) =
                    self.coordinator.lock()
                {
                    coordinator.receive_person(person);
                }
            }

            Err(error) => {
                eprintln!(
                    "[handler] failed to decode person: {:?}",
                    error,
                );
            }
        }
    }
}