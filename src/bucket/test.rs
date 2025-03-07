use crate::probe;

use super::ElasticHashing;

#[test]
pub(crate) fn test_bucket_size() {
    let hash = ElasticHashing::new(10, 1);
    assert_eq!(hash.bucket_count(), 3);
    assert_eq!(hash.get_bucket(0).len(), 5);
    assert_eq!(hash.get_bucket(1).len(), 3);
    assert_eq!(hash.get_bucket(2).len(), 2);
}

#[test]
#[should_panic(expected = "Size must be greater than 0")]
pub(crate) fn test_bucket_size_zero() {
    ElasticHashing::new(0, 1);
}

#[test]
pub(crate) fn test_insert() {
    use rand::Rng;
    let mut hash = ElasticHashing::new(4096, 3);
    let empty = 4096 / 8;
    let space = 4096 - empty; // 112
    let mut rng = rand::rng();
    let data = (0..space)
        .map(|_| rng.random_range(0..1000000))
        .collect::<Vec<_>>();
    for i in 0..space {
        // well, we don't do assert here, because insert it self has assertions
        hash.insert(data[i], data[i]);
    }
    for i in 0..space {
        assert_eq!(hash.get(data[i]), Some(data[i]));
    }
    let data = (0..space)
        .map(|_| rng.random_range(-1000000..0))
        .collect::<Vec<_>>();
    probe::reset_probe_num();
    for i in 0..space {
        assert_eq!(hash.get(data[i]), None);
    }
    eprintln!("probe num: {}", probe::get_probe_num() as f64 / space as f64);
}

#[test]
pub(crate) fn test_phi() {
    // j=1 (1), i=1 (1) → 1 1 0 1 → 0b1101 = 13
    assert_eq!(ElasticHashing::phi(1, 1), 13);

    // j=3 (11), i=2 (10) → 1 1 1 1 0 1 0 → 0b1111010 = 122
    assert_eq!(ElasticHashing::phi(2, 3), 122);

    // j=5 (101), i=3 (11) → 1 1 1 0 1 1 0 1 1 → 0b111011011 = 475
    assert_eq!(ElasticHashing::phi(3, 5), 475);

    assert_eq!(ElasticHashing::phi(15, 7), 0b11111101111);

    assert_eq!(
        ElasticHashing::phi(1024, 1023),
        0b11111111111111111111010000000000
    );
}

#[test]
pub(crate) fn test_de_phi() {
    // 测试 phi 和 de_phi 的互逆性
    let test_cases = vec![
        (1, 1),
        (1, 3),
        (2, 3),
        (3, 5),
        (15, 7),
        (1024, 1023),
        (42, 99),
        (255, 255),
    ];

    for (a, b) in test_cases {
        let encoded = ElasticHashing::phi(a, b);
        let decoded = ElasticHashing::de_phi(encoded);
        assert!(
            decoded.is_some(),
            "de_phi 返回 None，但应该返回 Some((a, b)) a: {}, b: {} encoded: {}",
            a,
            b,
            encoded
        );
        let (a_decoded, b_decoded) = decoded.unwrap();
        assert_eq!(a, a_decoded, "a 解码错误");
        assert_eq!(b, b_decoded, "b 解码错误");
    }
    let test_none = vec![
        0b1111111111111111111111111111111111111111111111111111111111111111,
        0b1111111111111111111111111111111111111111111111111111111111111110,
        14,
    ];
    for encoded in test_none {
        let decoded = ElasticHashing::de_phi(encoded);
        assert!(
            decoded.is_none(),
            "de_phi 返回 Some((a, b))，但应该返回 None encoded: {}",
            encoded
        );
    }
}
