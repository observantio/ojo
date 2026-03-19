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
mod tests {
    use super::{IntervalBuffer, OFFLINE_BUFFER_INTERVALS};

    #[test]
    fn interval_buffer_is_fifo() {
        let mut buf = IntervalBuffer::new(OFFLINE_BUFFER_INTERVALS);
        for value in [1, 2, 3] {
            assert!(!buf.push(value));
        }

        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn interval_buffer_caps_and_drops_oldest() {
        let mut buf = IntervalBuffer::new(OFFLINE_BUFFER_INTERVALS);
        for value in 0..OFFLINE_BUFFER_INTERVALS {
            assert!(!buf.push(value));
        }

        assert!(buf.push(99));
        assert_eq!(buf.len(), OFFLINE_BUFFER_INTERVALS);
        assert_eq!(buf.dropped_intervals(), 1);
        assert_eq!(buf.pop(), Some(1));
    }
}
