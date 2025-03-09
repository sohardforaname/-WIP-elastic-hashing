use super::ElasticHashMap;
use std::hash::Hash;

impl<K, V> ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn de_phi(x: u128) -> Option<(u32, u32)> {
        if x == 0 {
            return None;
        }

        let mut a: u32 = 0;
        let mut b: u32 = 0;

        let x_bits = 128 - x.leading_zeros() as usize;

        let mut i: i32 = x_bits as i32 - 2;
        let mut first_b = false;

        while i >= 0 {
            let bit = (x >> i) & 3;
            if (bit >> 1) & 1 != 0 {
                if bit & 1 == 0 && !first_b {
                    return None;
                }
                first_b = true;
                b = b << 1 | (bit & 1) as u32;
                i -= 2;
            } else {
                i += 1;
                a = x as u32 & ((1 << i) - 1);
                break;
            }
        }

        if a == 0 || b == 0 || (a >> (i - 1)) == 0 {
            return None;
        }

        Some((a, b))
    }

    pub fn phi(a: u32, b: u32) -> u128 {
        debug_assert!(a > 0);
        debug_assert!(b > 0);
        let mut result: u128 = 0;

        let b_bits = (32 - b.leading_zeros()) as usize + (b == 0) as usize;
        let a_bits = (32 - a.leading_zeros()) as usize + (a == 0) as usize;

        for i in (0..b_bits).rev() {
            result = (result << 2) + 2 + ((b >> i) & 1) as u128;
        }
        result = (result << (1 + a_bits)) | a as u128;
        result
    }

    const DE_PHI_MAP: [usize; 256] = [
        0, 0, 0, 1, 0, 0, 2, 3, 0, 0, 0, 1, 0, 0, 2, 3,
        0, 0, 4, 5, 0, 0, 6, 7, 0, 0, 4, 5, 0, 0, 6, 7,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 3,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 0, 0, 6, 7,
        0, 0, 8, 9, 0, 0, 10, 11, 0, 0, 8, 9, 0, 0, 10, 11,
        0, 0, 12, 13, 0, 0, 14, 15, 0, 0, 12, 13, 0, 0, 14, 15,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 9, 0, 0, 10, 11,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 13, 0, 0, 14, 15,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 3,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 0, 0, 6, 7,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 9, 0, 0, 10, 11,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 13, 0, 0, 14, 15
    ];

    pub fn de_phi_fast_buggy(x: u128) -> Option<(u32, u32)> {
        const BITMASK: u128 = 0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA;
        let x_zero = x.leading_zeros() as usize;
        let x_bits = 128 - x_zero;
        let x_zero_bitmask = (!(0_u128)) << x_bits;
        let x_b_fence = (BITMASK >> x_zero) & x | (BITMASK >> (x_zero + 1)) | x_zero_bitmask;
        let a_len = (128 - x_b_fence.leading_ones() - 1) as usize;
        let a = x & ((1 << a_len) - 1);
        if a == 0 || a >> (a_len - 1) == 0 {
            return None;
        }
        let mut b_len = (x_bits - a_len - 1) as isize;
        let mut b = 0;
        let x_b_value = x >> (a_len + 1);
        let mut cur_shift = 0;
        while b_len > 0 {
            b |= (Self::DE_PHI_MAP[(x_b_value & (0xFF000000 << cur_shift)) as usize >> 24] as u32) << 12;
            b |= (Self::DE_PHI_MAP[(x_b_value & (0xFF0000 << cur_shift)) as usize >> 16] as u32) << 8;
            b |= (Self::DE_PHI_MAP[(x_b_value & (0xFF00 << cur_shift)) as usize >> 8] as u32) << 4;
            b |= Self::DE_PHI_MAP[(x_b_value & (0xFF << cur_shift)) as usize] as u32;
            b_len -= 32;
            cur_shift += 32;
        }
        Some((a as u32, b as u32))
    }
}

/*

a = 7, b = 5

1110110111
1010101010

1010100010
0101010101

1111110111

*/