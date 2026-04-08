use std::collections::VecDeque;

pub const OFFLINE_BUFFER_INTERVALS: usize = 5;

#[derive(Debug)]
pub struct IntervalBuffer<T> {
    capacity: usize,
    queue: VecDeque<T>,
    dropped_intervals: u64,
}

impl<T> IntervalBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: VecDeque::with_capacity(capacity),
            dropped_intervals: 0,
        }
    }

    pub fn push(&mut self, item: T) -> bool {
        let mut dropped_oldest = false;
        if self.queue.len() == self.capacity {
            self.queue.pop_front();
            self.dropped_intervals += 1;
            dropped_oldest = true;
        }
        self.queue.push_back(item);
        dropped_oldest
    }

    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn dropped_intervals(&self) -> u64 {
        self.dropped_intervals
    }
}

#[cfg(test)]
#[path = "tests/buffers_tests.rs"]
mod tests;
