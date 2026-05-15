use std::collections::HashMap;
use std::time::{Duration, Instant};

use bw_core::proto::demo::chores::{
    Chores,
    PersonAvailability,
};

use crate::config::OrchestratorConfig;
use crate::execution::executor::{
    PersonJobCompletion,
    WindowExecutor,
};

#[derive(Debug, Clone)]
struct PendingPerson {
    message: PersonAvailability,
    received_at: Instant,
    sequence_number: u64,
}

#[derive(Debug)]
struct PersonWindow {
    opened_at: Instant,
    pending_people: HashMap<String, PendingPerson>,
    sequence_number: u64,
}

pub struct BatchCoordinator {
    executor: Box<dyn WindowExecutor + Send>,
    config: OrchestratorConfig,

    latest_chores: Option<Chores>,
    latest_chores_received_at: Option<Instant>,

    current_window: Option<PersonWindow>,

    active_filter_count: usize,

    // person_id -> last selected timestamp
    person_last_selected: HashMap<String, Instant>,
}

impl BatchCoordinator {
    pub fn new(
        executor: Box<dyn WindowExecutor + Send>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            executor,
            config,
            latest_chores: None,
            latest_chores_received_at: None,
            current_window: None,
            active_filter_count: 0,
            person_last_selected: HashMap::new(),
        }
    }

    pub fn receive_chores(
        &mut self,
        chores: Chores,
    ) {
        let now = Instant::now();

        let old_chores_id = self
            .latest_chores
            .as_ref()
            .map(|chores| chores.chores_id.clone());

        println!(
            "[coordinator] stored latest chores: chores_id={} chores={} previous_chores_id={:?}",
            chores.chores_id,
            chores.chores.len(),
            old_chores_id,
        );

        self.latest_chores = Some(chores);
        self.latest_chores_received_at = Some(now);
    }

    pub fn receive_person(
        &mut self,
        person: PersonAvailability,
    ) {
        let now = Instant::now();

        if self.current_window.is_none() {
            println!(
                "[coordinator] opened person window: first_cycle_id={} first_person_id={} window_seconds={}",
                person.cycle_id,
                person.person_id,
                self.config.person_gather_window_seconds,
            );

            self.current_window = Some(PersonWindow {
                opened_at: now,
                pending_people: HashMap::new(),
                sequence_number: 0,
            });
        }

        let latest_chores_id = self
            .latest_chores
            .as_ref()
            .map(|chores| chores.chores_id.clone());

        let window = self
            .current_window
            .as_mut()
            .expect("window must exist after opening");

        window.sequence_number += 1;

        window.pending_people.insert(
            person.person_id.clone(),
            PendingPerson {
                message: person.clone(),
                received_at: now,
                sequence_number: window.sequence_number,
            },
        );

        println!(
            "[coordinator] received person availability: cycle_id={} person_id={} available_minutes={} pending_unique_people={} sequence_number={} latest_chores_id={:?}",
            person.cycle_id,
            person.person_id,
            person.available_minutes,
            window.pending_people.len(),
            window.sequence_number,
            latest_chores_id,
        );
    }

    pub fn expire_stale_if_idle(
        &mut self,
        now: Instant,
    ) {
        self.flush_window_if_expired(now);
        self.prune_person_history(now);
    }

    fn flush_window_if_expired(
        &mut self,
        now: Instant,
    ) {
        let Some(window) = self.current_window.as_ref() else {
            return;
        };

        let window_duration =
            Duration::from_secs_f64(self.config.person_gather_window_seconds);

        if now.duration_since(window.opened_at) < window_duration {
            return;
        }

        self.flush_current_window(now);
    }

    fn flush_current_window(
        &mut self,
        now: Instant,
    ) {
        let Some(window) = self.current_window.take() else {
            return;
        };

        let Some(chores) = self.latest_chores.clone() else {
            println!(
                "[coordinator] dropping person window because no latest chores message is available: dropped_people={}",
                window.pending_people.len(),
            );
            return;
        };

        if window.pending_people.is_empty() {
            println!(
                "[coordinator] dropping person window because no person availability messages arrived"
            );
            return;
        }

        if self.active_filter_count > 0 {
            println!(
                "[coordinator] dropping person window because another batch is still active: latest_chores_id={} active={} max={} dropped_people={}",
                chores.chores_id,
                self.active_filter_count,
                self.config.max_active_filters(),
                window.pending_people.len(),
            );
            return;
        }

        let people_to_run = self.choose_people_to_run(
            window.pending_people.values().cloned().collect(),
            self.config.max_active_filters(),
        );

        let selected_person_ids: Vec<String> = people_to_run
            .iter()
            .map(|pending| pending.message.person_id.clone())
            .collect();

        let dropped_person_ids: Vec<String> = window
            .pending_people
            .keys()
            .filter(|person_id| !selected_person_ids.contains(person_id))
            .cloned()
            .collect();

        let people_messages: Vec<PersonAvailability> = people_to_run
            .into_iter()
            .map(|pending| {
                let person = pending.message;

                self.active_filter_count += 1;
                self.person_last_selected
                    .insert(person.person_id.clone(), now);

                person
            })
            .collect();

        println!(
            "[coordinator] flushing person window using latest chores: chores_id={} selected={:?} dropped={:?} active={} max={}",
            chores.chores_id,
            selected_person_ids,
            dropped_person_ids,
            self.active_filter_count,
            self.config.max_active_filters(),
        );

        let completions = self.executor.submit_window(
            chores,
            people_messages,
        );

        for completion in completions {
            self.on_person_complete(completion);
        }
    }

    fn choose_people_to_run(
        &self,
        mut people: Vec<PendingPerson>,
        max_people: usize,
    ) -> Vec<PendingPerson> {
        people.sort_by_key(|pending| {
            let last_selected = self
                .person_last_selected
                .get(&pending.message.person_id)
                .cloned();

            (
                last_selected,
                pending.sequence_number,
            )
        });

        people.into_iter().take(max_people).collect()
    }

    fn on_person_complete(
        &mut self,
        completion: PersonJobCompletion,
    ) {
        if self.active_filter_count == 0 {
            println!("[coordinator] active filter count would go negative");
        } else {
            self.active_filter_count -= 1;
        }

        if !completion.succeeded {
            self.person_last_selected
                .remove(&completion.person_id);
        }

        println!(
            "[coordinator] completed person job: person_id={} succeeded={} active={} max={}",
            completion.person_id,
            completion.succeeded,
            self.active_filter_count,
            self.config.max_active_filters(),
        );
    }

    fn prune_person_history(
        &mut self,
        now: Instant,
    ) {
        let ttl =
            Duration::from_secs_f64(self.config.person_last_success_ttl_seconds);

        let stale_person_ids: Vec<String> = self
            .person_last_selected
            .iter()
            .filter_map(|(person_id, last_selected)| {
                if now.duration_since(*last_selected) > ttl {
                    Some(person_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for person_id in &stale_person_ids {
            self.person_last_selected.remove(person_id);
        }

        if !stale_person_ids.is_empty() {
            println!(
                "[coordinator] pruned person history: count={}",
                stale_person_ids.len(),
            );
        }
    }
}