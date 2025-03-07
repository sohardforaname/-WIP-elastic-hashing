/// Construct a valid delta
///
/// delta is load factor of the hashtable, it's the fraction of
/// free slots in the hashtable after it's considered full
///
/// the 1/delta must be power of 2
pub fn delta(x: i32) -> f32 {
    1f32 / (1 << (x as usize)) as f32
}
