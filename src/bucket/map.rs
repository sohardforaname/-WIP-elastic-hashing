use std::{
    borrow::Borrow,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    ops::ControlFlow,
};

use super::{probe::ElasticProbe, utils::delta};
use crate::probe;

// Generic KV pair
type KVPair<K, V> = (K, V);

/// Hash table element state enumeration
#[derive(Debug, Clone)]
pub enum EntryState<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Empty slot
    Empty,
    /// Occupied slot
    Occupied(KVPair<K, V>),
    /// Tombstone marker (deleted element)
    Tombstone,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub size: usize,
    pub data: Vec<EntryState<K, V>>,
    bucket_offsets: Vec<usize>,
    bucket_load: Vec<usize>,
    delta: f32,
    max_elements: usize,
    batch_max: Vec<usize>,
    current_batch: (usize, usize),
    tombstone_count: usize,
    tombstone_bucket_map: Vec<usize>,
}

impl<K, V> ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(size: usize, delta_factor: i32) -> Self {
        if size == 0 {
            panic!("Size must be greater than 0");
        }
        // adjust size to the nearest power of 2
        let size = size.next_power_of_two();
        let delta = delta(delta_factor);
        let max_elements = (size as f32 * (1.0 - delta)) as usize;

        let mut hashing = ElasticHashMap {
            size,
            data: Vec::with_capacity(size),
            bucket_offsets: Vec::new(),
            delta,
            max_elements,
            batch_max: Vec::new(),
            current_batch: (0, 0),
            bucket_load: Vec::new(),
            tombstone_count: 0,
            tombstone_bucket_map: Vec::new(),
        };
        hashing.calc_bucket_size(size);
        for i in 0..hashing.bucket_count() {
            hashing
                .batch_max
                .push(hashing.insert_batch_size(i as i32) as _);
            hashing.tombstone_bucket_map.push(0);
        }
        hashing.current_batch = (0, 0);
        hashing
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity, 3)
    }

    pub fn get_underlying_size(&self) -> usize {
        self.data.len()
    }

    fn hash_key<Q: ?Sized>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// give key and i, get the probe sequence on bucket i
    pub fn sequence<Q: ?Sized>(&self, key: &Q, bucket_idx: usize) -> ElasticProbe
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let hash = self.hash_key(key);
        let seq = probe::ProbeSequence::new(
            hash,
            self.get_bucket(bucket_idx).len(),
            probe::ProbeStrategy::Uniform,
        );
        ElasticProbe::new(seq)
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut probe = self.sequence(key, 1);
        // let mut k = 0;
        // let mut bucket_table = vec![false; self.bucket_count()];
        // let mut done_bucket = 0;
        // loop {
        //     k += 1;
        //     if let Some((i, j)) = Self::de_phi(k - 1_u128) {
        //         let pos = probe.next_no_limit();
        //         if i > self.bucket_count() as u32 {
        //             continue;
        //         }
        //         if bucket_table[i as usize - 1] {
        //             continue;
        //         }
        //         let bucket_idx = i as usize - 1;
        //         let bucket_len = self.get_bucket(bucket_idx).len();
        //         let actual_pos = pos & (bucket_len - 1);

        //         let start = self.bucket_offsets[bucket_idx];
        //         let actual_idx = start + actual_pos;

        //         match &self.data[actual_idx] {
        //             EntryState::Occupied((ref stored_key, ref value)) => {
        //                 if key.eq(stored_key.borrow()) {
        //                     return Some(value);
        //                 }

        //                 if j >= bucket_len as u32 {
        //                     bucket_table[bucket_idx] = true;
        //                     done_bucket += 1;
        //                     if done_bucket >= self.bucket_count() {
        //                         return None;
        //                     }
        //                 }
        //             }
        //             EntryState::Empty => {
        //                 bucket_table[bucket_idx] = true;
        //                 done_bucket += 1;
        //                 if done_bucket >= self.bucket_count() {
        //                     return None;
        //                 }
        //             }
        //             EntryState::Tombstone => {
        //                 if j >= bucket_len as u32 {
        //                     bucket_table[bucket_idx] = true;
        //                     done_bucket += 1;
        //                     if done_bucket >= self.bucket_count() {
        //                         return None;
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }
    }

    #[allow(unused_assignments)]
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut probe = self.sequence(key, 1);
        let mut k = 0;
        let mut bucket_table = vec![false; self.bucket_count()];
        let mut done_bucket = 0;
        let mut found_idx = None;

        loop {
            k += 1;
            if let Some((i, j)) = Self::de_phi(k - 1_u128) {
                let pos = probe.next_no_limit();
                if i > self.bucket_count() as u32 {
                    continue;
                }
                if bucket_table[i as usize - 1] {
                    continue;
                }

                let bucket_idx = i as usize - 1;
                let bucket_len = self.get_bucket(bucket_idx).len();
                let actual_pos = pos & (bucket_len - 1);

                let start = self.bucket_offsets[bucket_idx];
                let actual_idx = start + actual_pos;

                match &self.data[actual_idx] {
                    EntryState::Occupied((ref stored_key, _)) => {
                        if key.eq(stored_key.borrow()) {
                            found_idx = Some(actual_idx);
                            break;
                        }

                        if j >= bucket_len as u32 {
                            bucket_table[bucket_idx] = true;
                            done_bucket += 1;
                            if done_bucket >= self.bucket_count() {
                                return None;
                            }
                        }
                    }
                    EntryState::Empty => {
                        bucket_table[bucket_idx] = true;
                        done_bucket += 1;
                        if done_bucket >= self.bucket_count() {
                            return None;
                        }
                    }
                    EntryState::Tombstone => {
                        if j >= bucket_len as u32 {
                            bucket_table[bucket_idx] = true;
                            done_bucket += 1;
                            if done_bucket >= self.bucket_count() {
                                return None;
                            }
                        }
                    }
                }
            }
        }

        if let Some(idx) = found_idx {
            if let EntryState::Occupied((_, ref mut value)) = &mut self.data[idx] {
                return Some(value);
            }
        }

        None
    }

    fn insert_batch_size(&self, i: i32) -> i32 {
        let bucket_size = self.get_bucket(i as usize).len();
        if i == 0 {
            (bucket_size as f32 * 0.75).ceil() as i32
        } else {
            let i_bucket_size = self.get_bucket(i as usize - 1).len();
            i_bucket_size as i32
                - (i_bucket_size as f32 * self.delta / 2.0).floor() as i32
                - (i_bucket_size as f32 * 0.75).ceil() as i32
                + (bucket_size as f32 * 0.75).ceil() as i32
        }
    }

    // tumbstone insert will always return None on success
    fn try_tombstone(&mut self, key: K, value: V) -> Result<(), (K, V)> {
        for (bucket_idx, tombstone_count) in self.tombstone_bucket_map.clone().iter().enumerate() {
            if tombstone_count > &0 {
                let mut probe = self.sequence(&key, bucket_idx as i32 + 1);
                // has tombstone in this bucket
                let bucket = self.get_bucket_mut(bucket_idx);
                for j in 1..=5 {
                    let pos = probe.probe(bucket_idx as u32 + 1, j as _);
                    let is_tombstone = matches!(bucket[pos], EntryState::Tombstone);
                    if is_tombstone {
                        bucket[pos] = EntryState::Occupied((key, value));
                        self.tombstone_bucket_map[bucket_idx] -= 1;
                        return Ok(());
                    }
                }
            }
        }
        Err((key, value))
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        // check if key is already in the map
        if let Some(v) = self.get_mut(&key) {
            let old_value = v.clone();
            *v = value;
            return Some(old_value);
        }

        let re = self.try_tombstone(key.clone(), value.clone());
        if let Ok(_) = re {
            return None;
        }
        let (key, value) = re.unwrap_err();
        let old_value = self.get(&key).cloned();

        if self.current_batch.1 == 0 {
            if let ControlFlow::Break(_) = self.try_seq(key.clone(), value.clone(), 1, i32::MAX) {
                return old_value;
            }
            unreachable!()
        }

        let i = self.current_batch.1;
        let i_plus_1 = i + 1;
        let epsilon_1 = self.epsilon(i as i32 - 1);
        let epsilon_2 = self.epsilon(i_plus_1 as i32 - 1);

        if epsilon_1 > self.delta / 2.0 && epsilon_2 > 0.25 {
            let f_epsilon_1 = self.f::<3000>(epsilon_1);
            if let ControlFlow::Break(_) = self.try_seq(key.clone(), value.clone(), i, f_epsilon_1)
            {
                return old_value;
            }

            if let ControlFlow::Break(_) = self.try_seq(key, value, i_plus_1, i32::MAX) {
                return old_value;
            }
        } else if epsilon_1 <= self.delta / 2.0 {
            if let ControlFlow::Break(_) = self.try_seq(key, value, i_plus_1, i32::MAX) {
                return old_value;
            }
        } else if epsilon_2 <= 0.25 {
            if let ControlFlow::Break(_) = self.try_seq(key, value, i, i32::MAX) {
                return old_value;
            }
        }
        unreachable!()
    }

    fn try_seq(&mut self, key: K, value: V, i: usize, max_try: i32) -> ControlFlow<()> {
        let mut probe = self.sequence(&key, i as _);
        for j in 1..=max_try {
            let bucket = self.get_bucket_mut(i - 1);
            let pos = probe.probe(i as _, j as _);
            match &bucket[pos] {
                EntryState::Empty | EntryState::Tombstone => {
                    let is_tombstone = matches!(bucket[pos], EntryState::Tombstone);
                    bucket[pos] = EntryState::Occupied((key, value));
                    self.bucket_load[i - 1] += 1;
                    self.current_batch.0 += 1;
                    if self.current_batch.0 >= self.batch_max[self.current_batch.1] {
                        self.current_batch = (0, self.current_batch.1 + 1);
                        let i = self.current_batch.1 - 1;
                        #[cfg(any(debug_assertions, test))]
                        {
                            for j in 0..i {
                                let bucket_size = self.get_bucket(j).len();
                                let bucket_load = self.bucket_load[j];
                                let expected = bucket_size
                                    - (bucket_size as f32 * self.delta / 2.0).floor() as usize;
                                assert_eq!(
                                    expected,
                                    bucket_load,
                                    "when done batch {}, A_{} has {} elements, while it should have {}",
                                    i,
                                    j + 1,
                                    bucket_load,
                                    expected
                                );
                            }
                        }
                        #[cfg(any(debug_assertions, test))]
                        {
                            let bucket_size = self.get_bucket(i).len();
                            let bucket_load = self.bucket_load[i];
                            let expected = (bucket_size as f32 * 0.75).ceil() as usize;
                            assert_eq!(
                                expected,
                                bucket_load,
                                "when done batch {}, A_{} has {} elements, while it should have {}",
                                i,
                                i + 1,
                                bucket_load,
                                expected
                            );
                        }
                    }
                    if is_tombstone {
                        self.tombstone_bucket_map[i - 1] -= 1;
                    }
                    return ControlFlow::Break(());
                }
                EntryState::Occupied((ref stored_key, _)) if stored_key == &key => {
                    bucket[pos] = EntryState::Occupied((key, value));
                    return ControlFlow::Break(());
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn f<const C: i32>(&self, epsilon: f32) -> i32 {
        (C as f32 * f32::min(epsilon.recip().ln().powi(2), self.delta.recip().ln())) as i32
    }

    fn epsilon(&self, i: i32) -> f32 {
        let bucket_size = self.get_bucket(i as usize).len();
        let bucket_load = self.bucket_load[i as usize];
        let load_factor = bucket_load as f32 / bucket_size as f32;
        1.0 - load_factor
    }

    /// calculate the bucket size and offsets
    ///
    /// it's sure that the bucket size is power of 2
    fn calc_bucket_size(&mut self, size: usize) {
        let mut current_size = (size + 1) / 2;
        let mut remaining_size = size;

        self.bucket_offsets = Vec::new();
        self.bucket_offsets.push(0);

        self.data = Vec::with_capacity(size);

        while remaining_size > 0 {
            self.data
                .resize(self.data.len() + current_size, EntryState::Empty);

            self.bucket_offsets.push(self.data.len());
            self.bucket_load.push(0);
            remaining_size = remaining_size.saturating_sub(current_size);
            current_size = (current_size + 1) / 2;
        }

        self.bucket_offsets.pop();
    }

    /// get the bucket slice
    ///
    /// it's sure that the bucket size is power of 2
    pub fn get_bucket(&self, bucket_idx: usize) -> &[EntryState<K, V>] {
        if bucket_idx >= self.bucket_offsets.len() {
            return &[];
        }

        let start = self.bucket_offsets[bucket_idx];
        let end = if bucket_idx + 1 < self.bucket_offsets.len() {
            self.bucket_offsets[bucket_idx + 1]
        } else {
            self.data.len()
        };

        &self.data[start..end]
    }

    /// get the mutable bucket slice
    ///
    /// it's sure that the bucket size is power of 2
    pub fn get_bucket_mut(&mut self, bucket_idx: usize) -> &mut [EntryState<K, V>] {
        if bucket_idx >= self.bucket_offsets.len() {
            return &mut [];
        }

        let start = self.bucket_offsets[bucket_idx];
        let end = if bucket_idx + 1 < self.bucket_offsets.len() {
            self.bucket_offsets[bucket_idx + 1]
        } else {
            self.data.len()
        };

        &mut self.data[start..end]
    }

    pub fn bucket_count(&self) -> usize {
        self.bucket_offsets.len()
    }

    #[allow(unused_assignments)]
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut probe = self.sequence(key, 1);
        let mut k = 0;
        let mut bucket_table = vec![false; self.bucket_count()];
        let mut done_bucket = 0;
        let mut found_location = None;

        loop {
            k += 1;
            if let Some((i, j)) = Self::de_phi(k - 1_u128) {
                let pos = probe.next_no_limit();
                if i > self.bucket_count() as u32 {
                    continue;
                }
                if bucket_table[i as usize - 1] {
                    continue;
                }

                let bucket_idx = i as usize - 1;
                let bucket_len = self.get_bucket(bucket_idx).len();
                let actual_pos = pos & (bucket_len - 1);

                let start = self.bucket_offsets[bucket_idx];
                let actual_idx = start + actual_pos;

                match &self.data[actual_idx] {
                    EntryState::Occupied((ref stored_key, _)) => {
                        if key.eq(stored_key.borrow()) {
                            found_location = Some((actual_idx, bucket_idx));
                            break;
                        }

                        if j >= bucket_len as u32 {
                            bucket_table[bucket_idx] = true;
                            done_bucket += 1;
                            if done_bucket >= self.bucket_count() {
                                return None;
                            }
                        }
                    }
                    EntryState::Empty => {
                        bucket_table[bucket_idx] = true;
                        done_bucket += 1;
                        if done_bucket >= self.bucket_count() {
                            return None;
                        }
                    }
                    EntryState::Tombstone => {
                        if j >= bucket_len as u32 {
                            bucket_table[bucket_idx] = true;
                            done_bucket += 1;
                            if done_bucket >= self.bucket_count() {
                                return None;
                            }
                        }
                    }
                }
            }
        }

        if let Some((idx, bucket_idx)) = found_location {
            if let EntryState::Occupied((_, value)) =
                std::mem::replace(&mut self.data[idx], EntryState::Tombstone)
            {
                self.tombstone_bucket_map[bucket_idx] += 1;
                self.tombstone_count += 1;
                return Some(value);
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.bucket_load.iter().sum::<usize>() - self.tombstone_count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        for item in self.data.iter_mut() {
            *item = EntryState::Empty;
        }
        for load in self.bucket_load.iter_mut() {
            *load = 0;
        }
        self.current_batch = (0, 0);
        self.tombstone_count = 0;
        for i in self.tombstone_bucket_map.iter_mut() {
            *i = 0;
        }
    }
}
