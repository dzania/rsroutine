//! Spawn-and-join throughput.
//!
//! A parent task spawns `BATCH_COUNT` batches of `BATCH_LEN` trivial children and joins each
//! batch before starting the next, so at most `BATCH_LEN` children are live at once. Every
//! child stack comes either from a fresh `mmap` or from a worker's routine pool, which makes
//! this a direct measure of per-task setup and teardown cost.
//!
//! Run with `cargo bench --bench spawn_join`.

use std::time::{Duration, Instant};

const BATCH_COUNT: u64 = 100;
const BATCH_LEN: u64 = 1_000;
const TASKS_PER_ROUND: u64 = BATCH_COUNT * BATCH_LEN;
const ROUND_COUNT: usize = 10;

fn run_round() -> Duration {
    let start = Instant::now();
    let parent = rsroutine::spawn(|| {
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
    });
    let total = parent.join().expect("parent task must not panic");
    let elapsed = start.elapsed();

    assert_eq!(total, (0..TASKS_PER_ROUND).sum::<u64>());
    elapsed
}

fn main() {
    // Start the worker threads outside the timed rounds.
    rsroutine::spawn(|| ())
        .join()
        .expect("warm-up task must not panic");

    let mut rounds_ms = Vec::with_capacity(ROUND_COUNT);
    for round in 0..ROUND_COUNT {
        let elapsed = run_round();
        println!("round {round}: {:.2} ms", elapsed.as_secs_f64() * 1e3);
        rounds_ms.push(elapsed.as_secs_f64() * 1e3);
    }

    rounds_ms.sort_by(f64::total_cmp);
    let median_ms = rounds_ms[ROUND_COUNT / 2];
    let per_task_ns = median_ms * 1e6 / TASKS_PER_ROUND as f64;
    println!(
        "{TASKS_PER_ROUND} spawn+join per round: min {:.2} ms, median {median_ms:.2} ms, \
         max {:.2} ms, median {per_task_ns:.0} ns/task",
        rounds_ms[0],
        rounds_ms[ROUND_COUNT - 1],
    );
}
