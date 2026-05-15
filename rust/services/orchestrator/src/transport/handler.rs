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
        Self { coordinator }
    }

    pub fn on_chores_bytes(
        &self,
        bytes: &[u8],
    ) {
        tracing::info!(
            "[handler] received raw chores payload: bytes={}",
            bytes.len(),
        );

        match Chores::decode(bytes) {
            Ok(chores) => {
                tracing::info!(
                    "[handler] decoded chores: chores_id={} chores={}",
                    chores.chores_id,
                    chores.chores.len(),
                );

                match self.coordinator.lock() {
                    Ok(mut coordinator) => {
                        coordinator.receive_chores(chores);
                    }
                    Err(err) => {
                        tracing::error!(
                            "[handler] failed to lock coordinator for chores: {}",
                            err,
                        );
                    }
                }
            }

            Err(error) => {
                tracing::error!(
                    "[handler] failed to decode chores: error={:?}",
                    error,
                );
            }
        }
    }

    pub fn on_person_bytes(
        &self,
        bytes: &[u8],
    ) {
        tracing::info!(
            "[handler] received raw person payload: bytes={}",
            bytes.len(),
        );

        match PersonAvailability::decode(bytes) {
            Ok(person) => {
                tracing::info!(
                    "[handler] decoded person availability: cycle_id={} person_id={} available_minutes={}",
                    person.cycle_id,
                    person.person_id,
                    person.available_minutes,
                );

                match self.coordinator.lock() {
                    Ok(mut coordinator) => {
                        coordinator.receive_person(person);
                    }
                    Err(err) => {
                        tracing::error!(
                            "[handler] failed to lock coordinator for person: {}",
                            err,
                        );
                    }
                }
            }

            Err(error) => {
                tracing::error!(
                    "[handler] failed to decode person: error={:?}",
                    error,
                );
            }
        }
    }
}