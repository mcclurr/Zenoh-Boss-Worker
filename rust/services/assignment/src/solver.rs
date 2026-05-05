use std::collections::HashMap;

use bw_core::config::DynError;
use lapjv::lapjv;
use ndarray::Array2;

pub const IMPOSSIBLE_COST: f64 = 1_000_000_000.0;

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
    let workers = input.workers;
    let jobs = input.jobs;

    if workers.is_empty() {
        return Ok(AssignmentOutput {
            assignments: vec![],
            total_cost: 0,
        });
    }

    if jobs.is_empty() {
        return Err("Cannot assign workers because there are no jobs".into());
    }

    if workers.len() > jobs.len() {
        return Err(format!(
            "More workers than jobs is not supported: workers={} jobs={}",
            workers.len(),
            jobs.len()
        )
        .into());
    }

    let mut cost_matrix = Array2::<f64>::from_elem(
        (workers.len(), jobs.len()),
        IMPOSSIBLE_COST,
    );

    for (worker_index, worker_id) in workers.iter().enumerate() {
        for (job_index, job_id) in jobs.iter().enumerate() {
            if let Some(cost) = input.costs.get(&(worker_id.clone(), job_id.clone())) {
                cost_matrix[[worker_index, job_index]] = *cost as f64;
            }
        }
    }

    let (_row_assignment, column_assignment) = lapjv(&cost_matrix)?;

    let mut assignments = Vec::new();
    let mut total_cost: u32 = 0;

    for (worker_index, job_index) in column_assignment.iter().enumerate() {
        let worker_id = workers[worker_index].clone();
        let job_id = jobs[*job_index].clone();

        let cost_f64 = cost_matrix[[worker_index, *job_index]];

        if cost_f64 >= IMPOSSIBLE_COST {
            return Err("No valid assignment found".into());
        }

        let cost = cost_f64 as u32;
        assignments.push((worker_id, job_id, cost));
        total_cost = total_cost.saturating_add(cost);
    }

    Ok(AssignmentOutput {
        assignments,
        total_cost,
    })
}