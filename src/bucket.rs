use std::io::Cursor;

use murmur3::murmur3_32;

static MURMUR_SEED_1: u32 = 0x9747b28c;
static MURMUR_SEED_2: u32 = 0x85ebca6b;

pub struct ElasticHashing {
    pub size: usize,
    bucket: Vec<Vec<i32>>,
}

impl ElasticHashing {
    pub fn new(size: usize) -> Self {
        let mut hashing = ElasticHashing {
            size,
            bucket: Vec::new(),
        };
        hashing.calc_bucket_size(size);
        hashing
    }

    fn calc_bucket_size(&mut self, size: usize) {
        if size == 0 {
            panic!("Size must be greater than 0");
        }

        let mut current_size = (size + 1) / 2;
        let mut remaining_size = size;
        self.bucket = Vec::new();

        while remaining_size > 0 {
            self.bucket.push(Vec::with_capacity(current_size));
            remaining_size -= current_size;
            current_size = (current_size + 1) / 2;
        }

        println!("Bucket sizes:");
        for (i, bucket) in self.bucket.iter().enumerate() {
            println!("Bucket {}: capacity {}", i, bucket.capacity());
        }
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
    assert_eq!(hash.bucket.len(), 3);
    assert_eq!(hash.bucket[0].capacity(), 5);
    assert_eq!(hash.bucket[1].capacity(), 3);
    assert_eq!(hash.bucket[2].capacity(), 2);
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
