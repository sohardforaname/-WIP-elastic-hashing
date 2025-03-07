use std::sync::atomic::{AtomicUsize, Ordering};

/// Probe strategy enumeration
pub enum ProbeStrategy {
    Linear,
    Quadratic,
    DoubleHash,
    Uniform, // Added uniform probing strategy
}

/// Probe sequence generator
pub struct ProbeSequence {
    initial_pos: usize,
    current_step: usize,
    capacity: usize,
    strategy: ProbeStrategy,
    secondary_hash: usize,
    // Random number generator state for uniform probing
    random_state: u64,
}

static PROBE_NUM: AtomicUsize = AtomicUsize::new(0);
static RANDOM_MUL: u64 = 6364136223846793005;
static RANDOM_ADD: u64 = 1442695040888963407;

pub fn reset_probe_num() {
    PROBE_NUM.store(0, Ordering::Relaxed);
}

pub fn get_probe_num() -> usize {
    PROBE_NUM.load(Ordering::Relaxed)
}

impl ProbeSequence {
    /// Create a new probe sequence
    pub fn new(key: u64, capacity: usize, strategy: ProbeStrategy) -> Self {
        let initial_pos = (key as usize) % capacity;
        // For double hashing, calculate the second hash value
        let secondary_hash = match strategy {
            ProbeStrategy::DoubleHash => {
                // A simple second hash function, ensuring the result is not zero
                1 + (key as usize % (capacity - 1))
            }
            _ => 0,
        };

        ProbeSequence {
            initial_pos,
            current_step: 0,
            capacity,
            strategy,
            secondary_hash,
            random_state: key, // Use key as random seed
        }
    }

    /// Get the next probe position
    pub fn next(&mut self) -> usize {
        self.next_no_limit() % self.capacity
    }

    pub fn next_no_limit(&mut self) -> usize {
        PROBE_NUM.fetch_add(1, Ordering::Relaxed);
        let pos = match self.strategy {
            ProbeStrategy::Linear => self.initial_pos + self.current_step,
            ProbeStrategy::Quadratic => {
                self.initial_pos + self.current_step + self.current_step * self.current_step
            }
            ProbeStrategy::DoubleHash => self.initial_pos + self.current_step * self.secondary_hash,
            ProbeStrategy::Uniform => {
                // Use a simple linear congruential generator to generate pseudo-random sequence
                self.random_state = self
                    .random_state
                    .wrapping_mul(RANDOM_MUL)
                    .wrapping_add(RANDOM_ADD);
                let random_increment = (self.random_state >> 32) as usize;
                self.initial_pos + random_increment
            }
        };

        self.current_step += 1;
        pos
    }
}
