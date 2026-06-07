// threading.rs
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use tokio::sync::oneshot;
use crate::RiftTask;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct PoolState {
    tasks: Mutex<VecDeque<Job>>,
    condvar: Condvar,
    stop: AtomicBool,
    paused: AtomicBool,
}

pub struct TaskThreadPool {
    workers: Vec<Option<thread::JoinHandle<()>>>,
    state: Arc<PoolState>,
    thread_count: usize,
}

impl TaskThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let thread_count = num_threads.max(1); // Ensure at least 1
        let state = Arc::new(PoolState {
            tasks: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            stop: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        });

        let mut workers = Vec::with_capacity(thread_count);
        for i in 0..thread_count {
            let state_clone = Arc::clone(&state);
            let handle = thread::Builder::new()
                .name(format!("RiftWorker-{}", i))
                .spawn(move || Self::worker_loop(state_clone))
                .expect("Critical: Failed to spawn OS thread");
            workers.push(Some(handle));
        }

        Self { workers, state, thread_count }
    }

    fn worker_loop(state: Arc<PoolState>) {
        loop {
            let job = {
                // Use match to handle potential Mutex poisoning gracefully
                let mut tasks = match state.tasks.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(), // Recover even if poisoned
                };

                loop {
                    if state.stop.load(Ordering::Acquire) && tasks.is_empty() { return; }
                    if !state.paused.load(Ordering::Acquire) && !tasks.is_empty() {
                        break tasks.pop_front().expect("Queue check failed");
                    }
                    tasks = state.condvar.wait(tasks).unwrap_or_else(|e| e.into_inner());
                }
            };

            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
        }
    }

    pub fn enqueue<T>(&self, task: T) -> oneshot::Receiver<T::Output>
    where T: RiftTask {
        let (sender, receiver) = oneshot::channel();
        let job = Box::new(move || {
            let result = task.execute();
            let _ = sender.send(result);
        });

        {
            let mut tasks = self.state.tasks.lock().unwrap_or_else(|e| e.into_inner());
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