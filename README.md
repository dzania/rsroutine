# rsroutine

A small green-thread runtime for Rust, written as a learning project. Each task runs on its own
stack, and switching between tasks is done in hand-written assembly.

Apple Silicon macOS only for now.

Tasks can spawn and join other tasks, yield to each other, and report panics:

```rust
use rsroutine::{spawn, yield_now};

fn main() {
    // A parent task splits the work across child tasks, then joins them.
    let parent = spawn(|| {
        let children: Vec<_> = (0..10u64)
            .map(|chunk| {
                spawn(move || {
                    let mut sum = 0;
                    for n in chunk * 1_000..(chunk + 1) * 1_000 {
                        sum += n;
                        if n % 100 == 0 {
                            yield_now(); // Let other tasks on this worker run.
                        }
                    }
                    sum
                })
            })
            .collect();

        children
            .into_iter()
            .map(|child| child.join().unwrap())
            .sum::<u64>()
    });

    assert_eq!(parent.join().unwrap(), (0..10_000).sum());

    // A panic inside a task comes back as an Err from join().
    let failed = spawn(|| -> u64 { panic!("boom") });
    assert!(failed.join().is_err());
}
```

## How it works

- Every task gets a 32 KiB `mmap`'d stack with a guard page below it.
- `swap_context` (`src/asm/aarch_macos.S`) saves the callee-saved registers and swaps the stack
  pointer.
- One worker thread per CPU. New tasks go on a shared queue; once a task starts, it stays on that
  worker, so values on its stack never cross threads.
- `yield_now()` hands the worker to the next task, and `join()` parks the caller until the result
  is ready.

## Limitations

- Blocking calls inside a task (`std::thread::sleep`, blocking I/O) block the whole worker.
- A stack overflow crashes with a bus error, not a Rust "stack overflow" message.
- No sleep, channels, or async I/O yet.
