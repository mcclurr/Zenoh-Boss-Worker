use std::collections::HashMap;

use bw_core::config::DynError;

pub const IMPOSSIBLE_COST: u32 = 1_000_000_000;

#[derive(Debug, Clone)]
pub struct AssignmentInput {
    pub workers: Vec<String>,
    pub jobs: Vec<String>,
    pub costs: HashMap<(String, String), u32>,
}

#[derive(Debug, Clone)]
pub struct AssignmentOutput {
    pub assignments: Vec<(String, String, u32)>,
    pub total_cost: u32,
}

pub fn solve_assignment(input: AssignmentInput) -> Result<AssignmentOutput, DynError> {
    if input.workers.is_empty() {
        return Ok(AssignmentOutput {
            assignments: vec![],
            total_cost: 0,
        });
    }

    if input.jobs.is_empty() {
        return Err("Cannot assign workers because there are no jobs".into());
    }

    if input.workers.len() > input.jobs.len() {
        return Err(format!(
            "More workers than jobs is not supported by this simple solver: workers={} jobs={}",
            input.workers.len(),
            input.jobs.len()
        )
        .into());
    }

    let mut memo: HashMap<(usize, u64), (u32, Vec<usize>)> = HashMap::new();

    fn dp(
        worker_index: usize,
        used_mask: u64,
        input: &AssignmentInput,
        memo: &mut HashMap<(usize, u64), (u32, Vec<usize>)>,
    ) -> (u32, Vec<usize>) {
        if worker_index == input.workers.len() {
            return (0, vec![]);
        }

        let key = (worker_index, used_mask);

        if let Some(cached) = memo.get(&key) {
            return cached.clone();
        }

        let worker_id = &input.workers[worker_index];

        let mut best_cost = IMPOSSIBLE_COST;
        let mut best_path: Vec<usize> = vec![];

        for (job_index, job_id) in input.jobs.iter().enumerate() {
            if used_mask & (1_u64 << job_index) != 0 {
                continue;
            }

            let pair_cost = input
                .costs
                .get(&(worker_id.clone(), job_id.clone()))
                .copied()
                .unwrap_or(IMPOSSIBLE_COST);

            let (remaining_cost, remaining_path) = dp(
                worker_index + 1,
                used_mask | (1_u64 << job_index),
                input,
                memo,
            );

            let total = pair_cost.saturating_add(remaining_cost);

            if total < best_cost {
                best_cost = total;

                best_path = vec![job_index];
                best_path.extend(remaining_path);
            }
        }

        memo.insert(key, (best_cost, best_path.clone()));

        (best_cost, best_path)
    }

    if input.jobs.len() > 63 {
        return Err("This simple DP solver supports at most 63 jobs".into());
    }

    let (total_cost, chosen_job_indices) = dp(0, 0, &input, &mut memo);

    if total_cost >= IMPOSSIBLE_COST {
        return Err("No valid assignment found".into());
    }

    let mut assignments = Vec::new();

    for (worker_id, job_index) in input.workers.iter().zip(chosen_job_indices.iter()) {
        let job_id = input.jobs[*job_index].clone();
        let cost = *input
            .costs
            .get(&(worker_id.clone(), job_id.clone()))
            .ok_or("Missing selected assignment cost")?;

        assignments.push((worker_id.clone(), job_id, cost));
    }

    Ok(AssignmentOutput {
        assignments,
        total_cost,
    })
}