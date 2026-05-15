// rust/services/orchestrator/src/processing/processor.rs

use bw_core::proto::demo::chores::{
    ChoreFilterRequest,
    ChoreFilterResult,
};
use std::time::Duration;

pub fn process_chore_filter_request(
    request: ChoreFilterRequest,
) -> ChoreFilterResult {
    let person = request.person.clone().expect("request must include person");
    let chores = request.chores.clone().expect("request must include chores");

    let available_minutes = person.available_minutes;
    let mut used_minutes = 0;

    let mut accepted_chores = Vec::new();
    let mut rejected_chores = Vec::new();

    for chore in chores.chores.iter() {
        if used_minutes + chore.estimated_minutes <= available_minutes {
            accepted_chores.push(chore.clone());
            used_minutes += chore.estimated_minutes;
        } else {
            rejected_chores.push(chore.clone());
        }
    }

    std::thread::sleep(Duration::from_secs(2));

    ChoreFilterResult {
        filter_id: request.filter_id,
        chores_id: chores.chores_id,
        person: Some(person),
        accepted_chores,
        rejected_chores,
        used_minutes,
        remaining_minutes: available_minutes.saturating_sub(used_minutes),
        context: request.context,
    }
}