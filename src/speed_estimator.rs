use std::ops::{Add, Sub};
use std::time::{Duration, Instant};
use arraydeque::ArrayDeque;

#[derive(Debug)]
pub struct SpeedEstimator<T: Add<T, Output=T> + Sub<T, Output=T> + Copy, const COUNT: usize = 1> {
    queue: ArrayDeque<(T, Instant), COUNT>
}

impl<T: Add<T, Output=T> + Sub<T, Output=T> + Copy + Default, const COUNT: usize> Default for SpeedEstimator<T, COUNT> {
    fn default() -> Self {
        let mut queue: ArrayDeque<(T, Instant), COUNT> = ArrayDeque::new();
        queue.push_back((T::default(), Instant::now())).unwrap();
        Self {
            queue
        }
    }
}

impl<T: Add<T, Output=T> + Sub<T, Output=T> + Copy, const COUNT: usize> SpeedEstimator<T, COUNT> {
    pub fn update(&mut self, adder: T) {
        if self.queue.is_full() {
            self.queue.pop_front();
        }
        let last = self.queue[0].0 + adder;
        self.queue.push_back((last, Instant::now())).unwrap();
    }

    pub fn speed(&self, adder: T) -> Option<(T, Duration)> {
        let l1 = self.queue.get(0)?;
        let l2 = self.queue.get(COUNT - 1)?;
        Some((l1.0 - l2.0, l1.1 - l2.1))
    }
}