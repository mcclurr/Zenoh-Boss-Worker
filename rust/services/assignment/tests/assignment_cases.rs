use std::{collections::HashMap, fs, path::PathBuf};

use serde::Deserialize;

#[path = "../src/solver.rs"]
mod solver;

use solver::{solve_assignment, AssignmentInput};

#[derive(Debug, Deserialize)]
struct GoldenCase {
    name: String,
    workers: Vec<String>,
    jobs: Vec<String>,
    costs: Vec<CostEntry>,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct CostEntry {
    worker_id: String,
    job_id: String,
    cost: u32,
}

#[derive(Debug, Deserialize)]
struct Expected {
    total_cost: u32,
    assignments: Option<Vec<ExpectedAssignment>>,
}

#[derive(Debug, Deserialize)]
struct ExpectedAssignment {
    worker_id: String,
    job_id: String,
    cost: u32,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn assignment_case_dir() -> PathBuf {
    repo_root().join("test_cases").join("assignment")
}

fn normalize_assignments(
    mut assignments: Vec<(String, String, u32)>,
) -> Vec<(String, String, u32)> {
    assignments.sort_by(|a, b| {
        let left = (&a.0, &a.1);
        let right = (&b.0, &b.1);
        left.cmp(&right)
    });

    assignments
}

fn print_cost_matrix(
    workers: &[String],
    jobs: &[String],
    costs: &HashMap<(String, String), u32>,
) {
    println!("  workers: {:?}", workers);
    println!("  jobs:    {:?}", jobs);
    println!("  cost matrix:");

    print!("    {:<12}", "");
    for job in jobs {
        print!("{:<12}", job);
    }
    println!();

    for worker in workers {
        print!("    {:<12}", worker);

        for job in jobs {
            match costs.get(&(worker.clone(), job.clone())) {
                Some(cost) => print!("{:<12}", cost),
                None => print!("{:<12}", "INF"),
            }
        }

        println!();
    }
}

fn print_assignments(label: &str, assignments: &[(String, String, u32)]) {
    println!("  {}:", label);

    for (worker_id, job_id, cost) in assignments {
        println!(
            "    {:<12} -> {:<12} cost={}",
            worker_id, job_id, cost
        );
    }
}

#[test]
fn golden_assignment_cases() {
    let case_dir = assignment_case_dir();

    println!("loading golden assignment cases from {:?}", case_dir);

    let entries = fs::read_dir(&case_dir)
        .unwrap_or_else(|err| panic!("failed to read {:?}: {}", case_dir, err));

    let mut case_count = 0;

    for entry in entries {
        let path = entry.unwrap().path();

        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }

        case_count += 1;

        println!();
        println!("============================================================");
        println!("case file: {:?}", path.file_name().unwrap());

        let raw = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {:?}: {}", path, err));

        let case: GoldenCase = serde_json::from_str(&raw)
            .unwrap_or_else(|err| panic!("failed to parse {:?}: {}", path, err));

        println!("case name: {}", case.name);

        let costs = case
            .costs
            .iter()
            .map(|entry| {
                (
                    (entry.worker_id.clone(), entry.job_id.clone()),
                    entry.cost,
                )
            })
            .collect::<HashMap<_, _>>();

        print_cost_matrix(&case.workers, &case.jobs, &costs);

        let result = solve_assignment(AssignmentInput {
            workers: case.workers.clone(),
            jobs: case.jobs.clone(),
            costs,
        })
        .unwrap_or_else(|err| panic!("case {} failed: {}", case.name, err));

        println!("  actual total cost:   {}", result.total_cost);
        println!("  expected total cost: {}", case.expected.total_cost);

        assert_eq!(
            result.total_cost, case.expected.total_cost,
            "case {} total_cost mismatch",
            case.name
        );

        if let Some(expected_assignments) = case.expected.assignments {
            let expected = expected_assignments
                .into_iter()
                .map(|a| (a.worker_id, a.job_id, a.cost))
                .collect::<Vec<_>>();

            let actual_normalized = normalize_assignments(result.assignments);
            let expected_normalized = normalize_assignments(expected);

            print_assignments("actual assignments", &actual_normalized);
            print_assignments("expected assignments", &expected_normalized);

            assert_eq!(
                actual_normalized,
                expected_normalized,
                "case {} assignments mismatch",
                case.name
            );
        }

        println!("case passed: {}", case.name);
    }

    println!();
    println!("checked {} golden assignment cases", case_count);
}