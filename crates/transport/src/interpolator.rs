// interpolator.rs
use std::collections::VecDeque;
use riftnet_core::Tick;

pub struct Snapshot<T> {
    pub tick: Tick,
    pub state: T,
}

pub struct Interpolator<T> {
    buffer: VecDeque<Snapshot<T>>,
    buffer_size: usize,
}

impl<T> Interpolator<T> {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(buffer_size),
            buffer_size
        }
    }

    pub fn push_snapshot(&mut self, snapshot: Snapshot<T>) {
        // Ensure buffer is sorted by tick for binary search optimization
        if let Some(last) = self.buffer.back() {
            if snapshot.tick <= last.tick { return; }
        }

        if self.buffer.len() >= self.buffer_size {
            self.buffer.pop_front();
        }
        self.buffer.push_back(snapshot);
    }

    pub fn interpolate(&self, render_tick: Tick, lerp_factor: f32) -> Option<T>
    where T: Clone + Interpolatable {
        if self.buffer.len() < 2 { return None; }

        for i in 0..self.buffer.len() - 1 {
            let a = &self.buffer[i];
            let b = &self.buffer[i + 1];

            if render_tick >= a.tick && render_tick < b.tick {
                return Some(a.state.lerp(&b.state, lerp_factor));
            }
        }
        None
    }
}

pub trait Interpolatable {
    fn lerp(&self, other: &Self, factor: f32) -> Self;

    // TODO: Implement SLERP for rotational components (Quaternions).
    // Required for the SPAWN Engine rigid-body physics integration.
    // fn slerp(&self, other: &Self, factor: f32) -> Self;

    // TODO: Add support for Velocity/Angular Momentum interpolation.
    // Necessary for high-fidelity prediction in RAPID-state physics environments.
}