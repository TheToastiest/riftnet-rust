use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use tokio::sync::oneshot;

// A type alias for a trait object representing a callable task that can be sent across threads
type Job = Box<dyn FnOnce() + Send + 'static>;

/// Shared state accessed by both the pool interface and the worker threads
struct PoolState {
    tasks: Mutex<VecDeque<Job>>,
    condvar: Condvar,
    stop: AtomicBool,
    paused: AtomicBool,
}

pub struct TaskThreadPool {
    // Option is used so we can take ownership of the handle during Drop/stop
    workers: Vec<Option<thread::JoinHandle<()>>>,
    state: Arc<PoolState>,
    thread_count: usize,
}

impl TaskThreadPool {
    /// Initializes the thread pool. Defaults to hardware concurrency if 0 is provided.
    pub fn new(num_threads: usize) -> Self {
        let thread_count = if num_threads == 0 {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
        } else {
            num_threads
        };

        let state = Arc::new(PoolState {
            tasks: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            stop: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        });

        let mut workers = Vec::with_capacity(thread_count);

        for i in 0..thread_count {
            let state_clone = Arc::clone(&state);

            // Rust's thread builder natively handles cross-platform thread naming
            let builder = thread::Builder::new().name(format!("PoolWorker-{}", i));

            let handle = builder
                .spawn(move || {
                    Self::worker_loop(state_clone);
                })
                .expect("Failed to create OS thread");

            workers.push(Some(handle));
        }

        Self {
            workers,
            state,
            thread_count,
        }
    }

    /// The core worker loop, detached from the main struct to prevent lifetime entanglement
    fn worker_loop(state: Arc<PoolState>) {
        loop {
            let job = {
                let mut tasks = state.tasks.lock().unwrap();

                // Equivalent to the C++ condition_.wait(lock, predicate)
                loop {
                    // Exit condition: Stopped and queue is fully drained
                    if state.stop.load(Ordering::Acquire) && tasks.is_empty() {
                        return;
                    }

                    // Execution condition: Not paused and queue has work
                    if !state.paused.load(Ordering::Acquire) && !tasks.is_empty() {
                        break tasks.pop_front().unwrap();
                    }

                    // Sleep and release lock until notified
                    tasks = state.condvar.wait(tasks).unwrap();
                }
            }; // Mutex lock is dropped here naturally

            // Execute the job completely outside the lock
            job();
        }
    }

    /// Enqueues a task and returns a Future that resolves when the task completes.
    pub fn enqueue<F, R>(&self, f: F) -> oneshot::Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if self.state.stop.load(Ordering::Acquire) {
            panic!("enqueue called on stopped TaskThreadPool");
        }

        // Create a one-time communication channel
        let (sender, receiver) = oneshot::channel();

        // Wrap the closure and the sender in a Boxed Job
        let job = Box::new(move || {
            let result = f();
            // We ignore send errors; if the caller dropped the receiver, they just don't care about the result
            let _ = sender.send(result);
        });

        {
            let mut tasks = self.state.tasks.lock().unwrap();
            tasks.push_back(job);
        }

        self.state.condvar.notify_one();
        receiver
    }

    pub fn stop(&mut self) {
        // Atomic swap ensures we only stop and join once
        if self.state.stop.swap(true, Ordering::SeqCst) {
            return;
        }

        self.state.condvar.notify_all();

        for worker in &mut self.workers {
            if let Some(handle) = worker.take() {
                let _ = handle.join();
            }
        }
    }

    pub fn pause(&self) {
        self.state.paused.store(true, Ordering::Release);
    }

    pub fn resume(&self) {
        self.state.paused.store(false, Ordering::Release);
        self.state.condvar.notify_all();
    }

    pub fn clear_queue(&self) {
        let mut tasks = self.state.tasks.lock().unwrap();
        tasks.clear();
    }

    pub fn thread_count(&self) -> usize {
        self.thread_count
    }
}

// Replaces the C++ Destructor. Rust guarantees this runs when the pool goes out of scope.
impl Drop for TaskThreadPool {
    fn drop(&mut self) {
        self.stop();
    }
}