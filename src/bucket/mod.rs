mod probe;
mod map;
mod func;
mod traits;
mod utils;

pub use map::{ElasticHashMap, EntryState};

// Add ElasticHashing type alias for backward compatibility
pub type ElasticHashing = ElasticHashMap<i32, i32>;

#[cfg(test)]
mod test;
