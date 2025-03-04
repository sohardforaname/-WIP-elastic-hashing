use std::io::Cursor;

use murmur3::murmur3_32;

static MURMUR_SEED_1: u32 = 0x9747b28c;
static MURMUR_SEED_2: u32 = 0x85ebca6b;

pub struct ElasticHashing {
    pub size: usize,
    data: Vec<i32>,
    bucket_offsets: Vec<usize>, // 存储每个桶的起始偏移量
}

impl ElasticHashing {
    pub fn new(size: usize) -> Self {
        if size == 0 {
            panic!("Size must be greater than 0");
        }

        let mut hashing = ElasticHashing {
            size,
            data: Vec::with_capacity(size),
            bucket_offsets: Vec::new(),
        };
        hashing.calc_bucket_size(size);
        hashing
    }

    fn calc_bucket_size(&mut self, size: usize) {
        let mut current_size = (size + 1) / 2;
        let mut remaining_size = size;
        
        self.bucket_offsets = Vec::new();
        self.bucket_offsets.push(0); // 第一个桶的起始位置
        
        self.data = Vec::with_capacity(size);
        
        while remaining_size > 0 {
            // 为当前桶预留空间
            self.data.resize(self.data.len() + current_size, 0);
            
            // 记录下一个桶的起始位置
            self.bucket_offsets.push(self.data.len());
            
            remaining_size -= current_size;
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
    pub fn get_bucket(&self, bucket_idx: usize) -> &[i32] {
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
    pub fn get_bucket_mut(&mut self, bucket_idx: usize) -> &mut [i32] {
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

    fn hash(x: i32) -> u128 {
        let bytes = x.to_le_bytes();
        let mut cursor = Cursor::new(bytes);
        let h1 = murmur3_32(&mut cursor, MURMUR_SEED_1).unwrap();
        let h2 = murmur3_32(&mut cursor, MURMUR_SEED_2).unwrap();
        Self::phi(h1, h2)
    }

    fn phi(a: u32, b: u32) -> u128 {
        let mut exp: u128 = 0;
        for i in (0..32).rev() {
            exp = (exp << 2) | 2;
            exp |= ((b >> i) & 1) as u128;
        }

        exp <<= 1;
        (exp << 32) | a as u128
    }
}

#[test]
fn test_bucket_size() {
    let hash = ElasticHashing::new(10);
    assert_eq!(hash.bucket_count(), 3);
    assert_eq!(hash.get_bucket(0).len(), 5);
    assert_eq!(hash.get_bucket(1).len(), 3);
    assert_eq!(hash.get_bucket(2).len(), 2);
}

#[test]
#[should_panic(expected = "Size must be greater than 0")]
fn test_bucket_size_zero() {
    ElasticHashing::new(0);
}

#[test]
fn test_phi() {
    assert_eq!(ElasticHashing::phi(10, 15), 105637550019019117515809751050);
    assert_eq!(ElasticHashing::phi(12, 19), 105637550019019119027638239244);
    assert_eq!(ElasticHashing::phi(234, 2451), 105637550019055851512098980074);
    assert_eq!(ElasticHashing::phi(14151, 124352), 105637550068028026802524927815);
}

// 10101101010010
