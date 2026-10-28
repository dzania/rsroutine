//! Spawn-and-join throughput.
//!
//! Each parent task spawns `BATCH_COUNT` batches of `BATCH_LEN` trivial children and joins each
//! batch before starting the next, so at most `BATCH_LEN` children per parent are live at once.
//! Children do no work, so this measures per-task cost: stack setup, queueing, waking workers,
//! and joining.
//!
//! Two scenarios run:
//!
//! - One parent: a single spawner, with the other workers mostly idle. Shows how much the
//!   runtime spends waking and parking workers.
//! - One parent per CPU: every worker spawns at once. Shows contention on shared state.
//!
//! Run with `cargo bench --bench spawn_join`.

use std::{
    thread::available_parallelism,
    time::{Duration, Instant},
};

const BATCH_COUNT: u64 = 100;
const BATCH_LEN: u64 = 1_000;
const TASKS_PER_PARENT: u64 = BATCH_COUNT * BATCH_LEN;
const ROUND_COUNT: usize = 10;

fn run_parent() -> u64 {
    let mut total = 0_u64;
    for batch in 0..BATCH_COUNT {
        let children: Vec<_> = (0..BATCH_LEN)
            .map(|index| rsroutine::spawn(move || batch * BATCH_LEN + index))
            .collect();
        for child in children {
            total += child.join().expect("child task must not panic");
        }
    }
    total
}

fn run_round(parent_count: u64) -> Duration {
    let start = Instant::now();
    let parents: Vec<_> = (0..parent_count)
        .map(|_| rsroutine::spawn(run_parent))
        .collect();
    let mut total = 0_u64;
    for parent in parents {
        total += parent.join().expect("parent task must not panic");
    }
    let elapsed = start.elapsed();

    assert_eq!(total, parent_count * (0..TASKS_PER_PARENT).sum::<u64>());
    elapsed
}

fn run_scenario(parent_count: u64) {
    let task_count = parent_count * TASKS_PER_PARENT;
    println!("{parent_count} parent(s), {task_count} spawn+join per round");

    let mut rounds_ms = Vec::with_capacity(ROUND_COUNT);
    for round in 0..ROUND_COUNT {
        let elapsed_ms = run_round(parent_count).as_secs_f64() * 1e3;
        println!("  round {round}: {elapsed_ms:.2} ms");
        rounds_ms.push(elapsed_ms);
    }

    rounds_ms.sort_by(f64::total_cmp);
    let median_ms = rounds_ms[ROUND_COUNT / 2];
    let per_task_ns = median_ms * 1e6 / task_count as f64;
    println!(
        "  min {:.2} ms, median {median_ms:.2} ms, max {:.2} ms, median {per_task_ns:.0} ns/task",
        rounds_ms[0],
        rounds_ms[ROUND_COUNT - 1],
    );
}

fn main() {
    // Start the worker threads outside the timed rounds.
    rsroutine::spawn(|| ())
        .join()
        .expect("warm-up task must not panic");

    let cpu_count = available_parallelism()
        .expect("available parallelism must be known")
        .get() as u64;

    run_scenario(1);
    run_scenario(cpu_count);
}
