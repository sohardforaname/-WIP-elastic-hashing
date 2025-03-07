use std::ops::ControlFlow;

use crate::probe;

pub struct ElasticProbe {
    seq: probe::ProbeSequence,
    // i: u32,
    pos: usize,
}

impl ElasticProbe {
    pub fn new(seq: probe::ProbeSequence) -> Self {
        Self { seq, pos: 0 }
    }

    pub fn probe(&mut self, i: u32, j: u32) -> usize {
        debug_assert!(j > 0);
        let x = ElasticHashing::phi(i, j);
        while self.pos < x as usize {
            self.pos += 1;
            self.seq.next();
        }
        self.pos += 1;
        self.seq.next()
    }
    pub fn next_no_limit(&mut self) -> usize {
        self.pos += 1;
        self.seq.next_no_limit()
    }
}

type KV = (i32, i32);

#[derive(Debug)]
pub struct ElasticHashing {
    pub size: usize,
    pub data: Vec<Option<KV>>,
    bucket_offsets: Vec<usize>, //
    /// bucket_elements
    bucket_load: Vec<usize>,
    delta: f32,
    max_elements: usize,
    /// [(max_elements, current_batch_index)]
    batch_max: Vec<usize>,
    /// (current_batch_elements, current_batch_index)
    /// initial value is (0,0)
    current_batch: (usize, usize),
}

impl ElasticHashing {
    pub fn new(size: usize, delta_factor: i32) -> Self {
        if size == 0 {
            panic!("Size must be greater than 0");
        }
        let delta = delta(delta_factor);
        let max_elements = (size as f32 * delta) as usize;

        let mut hashing = ElasticHashing {
            size,
            data: Vec::with_capacity(size),
            bucket_offsets: Vec::new(),
            delta,
            max_elements,
            batch_max: Vec::new(),
            current_batch: (0, 0),
            bucket_load: Vec::new(),
        };
        hashing.calc_bucket_size(size);
        for i in 0..hashing.bucket_count() {
            hashing
                .batch_max
                .push(hashing.insert_batch_size(i as i32) as _);
        }
        hashing.current_batch = (0, 0);
        hashing
    }

    // A[i][j], all 1 based index
    pub fn sequence(&self, x: i32, i: i32) -> ElasticProbe {
        debug_assert!(i > 0);
        let seq = probe::ProbeSequence::new(
            x as u64,
            self.get_bucket(i as usize - 1).len(),
            probe::ProbeStrategy::Uniform,
        );
        ElasticProbe::new(seq)
    }

    pub fn get(&self, x: i32) -> Option<i32> {
        let mut probe = self.sequence(x, 1);
        let mut k = 0;
        // create vec of bucket size
        let mut bucket_table = vec![false; self.bucket_count()];
        let mut done_bucket = 0;
        loop {
            k += 1;
            let pos = probe.next_no_limit();
            if let Some((i, j)) = Self::de_phi(k - 1_u128) {
                debug_assert_eq!(
                    Self::phi(i, j),
                    k - 1_u128,
                    "phi(i,j) != k i: {}, j: {}, k: {}",
                    i,
                    j,
                    k - 1
                );
                if i > self.bucket_count() as u32 {
                    continue;
                }
                if bucket_table[i as usize - 1] {
                    continue;
                }
                let bucket = self.get_bucket(i as usize - 1);
                if let Some((x_1, y)) = bucket[pos % bucket.len()] {
                    if x_1 == x {
                        return Some(y);
                    }
                    if j >= bucket.len() as u32 {
                        bucket_table[i as usize - 1] = true;
                        done_bucket += 1;
                        if done_bucket >= self.bucket_count() {
                            return None;
                        }
                    }
                } else {
                    bucket_table[i as usize - 1] = true;
                    done_bucket += 1;
                    if done_bucket >= self.bucket_count() {
                        return None;
                    }
                }
            }
        }
    }

    /// Calculate the batch size for the i-th insert batch
    ///
    /// param i is zero based index
    ///
    /// if i == 0, insert batch size is  \lceil0.75 \cdot |A_1|\rceil
    /// for i>= 0, insert batch size is
    /// $|A_i| - \lfloor\delta|A_i|/2\rfloor - \lceil0.75 \cdot |A_i|\rceil + \lceil0.75 \cdot |A_{i+1}|\rceil$
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

    pub fn insert(&mut self, x: i32, v: i32) {
        if self.current_batch.1 == 0 {
            // first batch, insert directly according to probe sequence
            if let ControlFlow::Break(_) = self.try_seq(x, v, 1, i32::MAX) {
                return;
            }
            unreachable!()
        }

        // for i >= 1, insert according to current batch
        let i = self.current_batch.1;
        let i_plus_1 = i + 1;
        let epsilon_1 = self.epsilon(i as i32 - 1);
        let epsilon_2 = self.epsilon(i_plus_1 as i32 - 1);

        if epsilon_1 > self.delta / 2.0 && epsilon_2 > 0.25 {
            let f_epsilon_1 = self.f::<3000>(epsilon_1);
            // check if h_{i,1}(x), ... , h_{i,f_epsilon_1}(x) are all occupied,
            // if not then place x at the first empty slot
            if let ControlFlow::Break(_) = self.try_seq(x, v, i, f_epsilon_1) {
                return;
            }

            // else, try h_{i+1,1}(x), ... , h_{i+1,f_epsilon_2}(x)
            if let ControlFlow::Break(_) = self.try_seq(x, v, i_plus_1, i32::MAX) {
                return;
            }
        } else if epsilon_1 <= self.delta / 2.0 {
            // try h_{i+1,1}(x), ... , h_{i+1,f_epsilon_2}(x)
            if let ControlFlow::Break(_) = self.try_seq(x, v, i_plus_1, i32::MAX) {
                return;
            }
        } else {
            // case 3: epsilon_2 <= 0.25
            // try h_{i,1}(x), ...
            if let ControlFlow::Break(_) = self.try_seq(x, v, i, i32::MAX) {
                return;
            }
        }
        unreachable!()
    }

    // here i is one based index
    fn try_seq(&mut self, x: i32, v: i32, i: usize, max_try: i32) -> ControlFlow<()> {
        let mut probe = self.sequence(x, i as _);
        for j in 1..=max_try {
            let bucket = self.get_bucket_mut(i - 1);
            let pos = probe.probe(i as _, j as _);
            if bucket[pos].is_none() {
                bucket[pos] = Some((x, v));
                self.bucket_load[i - 1] += 1;
                self.current_batch.0 += 1;
                if self.current_batch.0 >= self.batch_max[self.current_batch.1] {
                    self.current_batch = (0, self.current_batch.1 + 1);
                    let i = self.current_batch.1 - 1;
                    // at the end of the batch, for j < i + 1,
                    // A_j has exactly $|A_j| - \lfloor \delta |A_j| / 2 \rfloor$ elements
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
                    // A_{i+1} has exactly $\lceil0.75 \cdot |A_{i+1}|\rceil$ elements
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
                return ControlFlow::Break(());
            } else if let Some((x_old, _)) = bucket[pos] {
                if x_old == x {
                    bucket[pos] = Some((x, v));
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// $f(\epsilon) = c · min(log^2{\epsilon^{-1}}, log \delta^{-1})$
    fn f<const C: i32>(&self, epsilon: f32) -> i32 {
        (C as f32 * f32::min(epsilon.recip().ln().powi(2), self.delta.recip().ln())) as i32
    }

    /// Ai is 1 − \epsilon full
    /// here i is zero based index
    fn epsilon(&self, i: i32) -> f32 {
        let bucket_size = self.get_bucket(i as usize).len();
        let bucket_load = self.bucket_load[i as usize];
        let load_factor = bucket_load as f32 / bucket_size as f32;
        1.0 - load_factor
    }

    fn calc_bucket_size(&mut self, size: usize) {
        let mut current_size = (size + 1) / 2;
        let mut remaining_size = size;

        self.bucket_offsets = Vec::new();
        self.bucket_offsets.push(0); // 第一个桶的起始位置

        self.data = Vec::with_capacity(size);

        while remaining_size > 0 {
            // 为当前桶预留空间
            self.data.resize(self.data.len() + current_size, None);

            // 记录下一个桶的起始位置
            self.bucket_offsets.push(self.data.len());
            self.bucket_load.push(0);
            remaining_size = remaining_size.saturating_sub(current_size);
            current_size = (current_size + 1) / 2;
        }

        // 移除最后一个偏移量，因为它指向数据的末尾
        self.bucket_offsets.pop();

        println!("Bucket sizes:");
        for i in 0..self.bucket_offsets.len() {
            let start = self.bucket_offsets[i];
            let end = if i + 1 < self.bucket_offsets.len() {
                self.bucket_offsets[i + 1]
            } else {
                self.data.len()
            };
            println!("Bucket {}: capacity {}", i, end - start);
        }
    }

    // 获取指定桶的切片
    pub fn get_bucket(&self, bucket_idx: usize) -> &[Option<KV>] {
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

    // 获取指定桶的可变切片
    pub fn get_bucket_mut(&mut self, bucket_idx: usize) -> &mut [Option<KV>] {
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

    // 获取桶的数量
    pub fn bucket_count(&self) -> usize {
        self.bucket_offsets.len()
    }

    fn de_phi(x: u128) -> Option<(u32, u32)> {
        if x == 0 {
            return None;
        }

        let mut a: u32 = 0;
        let mut b: u32 = 0;

        // 计算 x 的位数
        let x_bits = 128 - x.leading_zeros() as usize;

        // 从最高位开始读取
        let mut i = x_bits;
        let mut c = 0;
        let mut first_b = true;

        while i > 0 {
            i -= 1;
            let bit = (x >> i) & 1;
            c += 1;

            // 读取 b 的位（从最高位到分隔符"0"）, b 是 1 b0 1 b1 ... 1 bn 0
            if c % 2 == 0 {
                if first_b {
                    first_b = false;
                    if bit == 0 {
                        return None;
                    }
                }
                b = (b << 1) | bit as u32;
            } else if bit == 0 {
                // 0 后面的就是a，即x的低位
                // 找到分隔符"0"后，读取剩余的位作为a
                a = 0;
                // i -= 1;
                let mut first = true;
                while i > 0 {
                    i -= 1;
                    let bit = (x >> i) & 1;
                    if first {
                        first = false;
                        if bit == 0 {
                            return None;
                        }
                    }
                    a = (a << 1) | bit as u32;
                }
                break;
            }
        }

        // 确保 a 和 b 都不为 0
        if a == 0 || b == 0 {
            return None;
        }

        Some((a, b))
    }

    fn phi(a: u32, b: u32) -> u128 {
        debug_assert!(a > 0);
        debug_assert!(b > 0);
        let mut result: u128 = 0;

        // 获取 b 的有效位数
        let b_bits = if b == 0 {
            0
        } else {
            32 - b.leading_zeros() as usize
        };

        // 获取 a 的有效位数
        let a_bits = if a == 0 {
            0
        } else {
            32 - a.leading_zeros() as usize
        };

        // 对 b 的每个有效位，添加 "1" 前缀
        for i in (0..b_bits).rev() {
            // 添加 "1" 前缀
            result = (result << 1) | 1;
            // 添加 b 的当前位
            result = (result << 1) | ((b >> i) & 1) as u128;
        }

        // 添加 "0" 分隔符
        result <<= 1;

        // 添加 a 的有效位
        for i in (0..a_bits).rev() {
            result = (result << 1) | ((a >> i) & 1) as u128;
        }

        result
    }
}

#[cfg(test)]
mod test;

/// construct a valid delta
///
/// delta is load factor of the hashtable, it's the fraction of
/// free slots in the hashtable after it's considered full
///
/// the 1/delta must be power of 2
fn delta(x: i32) -> f32 {
    1f32 / (1 << (x as usize)) as f32
}
