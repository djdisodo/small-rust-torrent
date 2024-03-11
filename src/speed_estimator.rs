use std::time::{Duration, Instant};
use arraydeque::ArrayDeque;

struct Update {
    time: Instant,
    bytes_received: u32,
}

#[derive(Default)]
pub struct SpeedEstimator<const N: usize> {
    updates: ArrayDeque<Update, N>,
}

impl<const N: usize> SpeedEstimator<N> {
    pub fn new() -> Self {
        Self {
            updates: ArrayDeque::new(),
        }
    }

    pub fn update(&mut self, n: u32) {
        let update = Update {
            time: Instant::now(),
            bytes_received: n,
        };

        // Pop before pushing if full
        if self.updates.is_full() {
            let _ = self.updates.pop_front();
        }

        self.updates.push_back(update).unwrap();
    }

    pub fn estimated_speed(&self) -> u32 {
        if self.updates.is_empty() {
            return 0;
        }

        let mut total_bytes = 0;
        let mut total_time = Duration::new(0, 0);

        let now = Instant::now();

        for update in &self.updates {
            total_bytes += update.bytes_received;
            total_time += now - update.time;
        }

        if total_time.as_secs() == 0 {
            return 0;
        }

        total_bytes / total_time.as_secs() as u32
    }
}