use std::collections::hash_map::Keys;

use crate::probe;

use super::*;

#[test]
pub(crate) fn test_bucket_size() {
    let hash = ElasticHashing::new(10, 1);
    assert_eq!(hash.bucket_count(), 5);
    assert_eq!(hash.get_bucket(0).len(), 8);
    assert_eq!(hash.get_bucket(1).len(), 4);
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
        assert_eq!(hash.get(&data[i]), Some(&data[i]));
    }
    let data = (0..space)
        .map(|_| rng.random_range(-1000000..0))
        .collect::<Vec<_>>();
    probe::reset_probe_num();
    for i in 0..space {
        assert_eq!(hash.get(&data[i]), None);
    }
    eprintln!(
        "probe num: {}",
        probe::get_probe_num() as f64 / space as f64
    );
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
    // test phi and de_phi are inverse
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
            "de_phi returns None, but should return Some((a, b)) a: {}, b: {} encoded: {}",
            a,
            b,
            encoded
        );
        let (a_decoded, b_decoded) = decoded.unwrap();
        assert_eq!(a, a_decoded, "a decode error");
        assert_eq!(b, b_decoded, "b decode error");
    }
    let test_none = vec![
        0b1111111111111111111111111111111111111111111111111111111111111111,
        0b1111111111111111111111111111111111111111111111111111111111111110,
        0b11110011,
        14,
    ];
    for encoded in test_none {
        let decoded = ElasticHashing::de_phi(encoded);
        assert!(
            decoded.is_none(),
            "de_phi returns Some((a, b)), but should return None encoded: {}",
            encoded
        );
    }
}

#[test]
fn test_elastic_hashmap_basic() {
    // create a new hashmap
    let mut map = ElasticHashMap::<String, i32>::with_capacity(16);

    // test insert and get
    map.insert("one".to_string(), 1);
    map.insert("two".to_string(), 2);
    map.insert("three".to_string(), 3);

    assert_eq!(map.get("one"), Some(&1));
    assert_eq!(map.get("two"), Some(&2));
    assert_eq!(map.get("three"), Some(&3));
    assert_eq!(map.get("four"), None);

    // test length
    assert_eq!(map.len(), 3);
    assert!(!map.is_empty());

    // test update value
    map.insert("one".to_string(), 10);
    assert_eq!(map.get("one"), Some(&10));

    // test remove
    assert_eq!(map.remove("two"), Some(2));
    assert_eq!(map.get("two"), None);
    assert_eq!(map.len(), 2);

    // test clear
    map.clear();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
}

#[test]
fn test_elastic_hashmap_index() {
    let mut map = ElasticHashMap::<String, String>::with_capacity(16);

    map.insert("key1".to_string(), "value1".to_string());
    map.insert("key2".to_string(), "value2".to_string());

    // test index operator
    assert_eq!(&map["key1".to_string()], "value1");
    assert_eq!(&map["key2".to_string()], "value2");

    // test mutable index operator
    map["key1".to_string()] = "new_value".to_string();
    assert_eq!(&map["key1".to_string()], "new_value");
}

#[test]
fn test_elastic_hashmap_iterator() {
    let mut map = ElasticHashMap::<i32, String>::with_capacity(16);

    map.insert(1, "one".to_string());
    map.insert(2, "two".to_string());
    map.insert(3, "three".to_string());

    // collect all key-value pairs
    let mut pairs: Vec<(i32, String)> = map.into_iter().collect();
    pairs.sort_by_key(|(k, _)| *k);

    assert_eq!(
        pairs,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string())
        ]
    );
}

#[test]
fn test_elastic_hashmap_from_iterator() {
    let pairs = vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ];

    // create a hashmap from iterator
    let map: ElasticHashMap<String, i32> = pairs.into_iter().collect();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get("a"), Some(&1));
    assert_eq!(map.get("b"), Some(&2));
    assert_eq!(map.get("c"), Some(&3));
}

#[test]
fn test_elastic_hashmap_extend() {
    let mut map = ElasticHashMap::<char, i32>::with_capacity(16);

    map.insert('a', 1);
    map.insert('b', 2);

    // extend hashmap
    map.extend([('c', 3), ('d', 4)]);

    assert_eq!(map.len(), 4);
    assert_eq!(map.get(&'a'), Some(&1));
    assert_eq!(map.get(&'b'), Some(&2));
    assert_eq!(map.get(&'c'), Some(&3));
    assert_eq!(map.get(&'d'), Some(&4));
}

#[test]
fn test_elastic_hashmap_complex_keys() {
    #[derive(PartialEq, Eq, Hash, Clone, Debug)]
    struct ComplexKey {
        id: i32,
        name: String,
    }

    let mut map = ElasticHashMap::<ComplexKey, Vec<i32>>::with_capacity(16);

    let key1 = ComplexKey {
        id: 1,
        name: "one".to_string(),
    };
    let key2 = ComplexKey {
        id: 2,
        name: "two".to_string(),
    };

    map.insert(key1.clone(), vec![1, 2, 3]);
    map.insert(key2.clone(), vec![4, 5, 6]);

    assert_eq!(map.get(&key1), Some(&vec![1, 2, 3]));
    assert_eq!(map.get(&key2), Some(&vec![4, 5, 6]));

    // test mutable reference
    if let Some(value) = map.get_mut(&key1) {
        value.push(4);
    }

    assert_eq!(map.get(&key1), Some(&vec![1, 2, 3, 4]));
}

#[test]
fn test_elastic_hashmap_tombstone() {
    use rand::Rng;
    let mut map = ElasticHashMap::<i32, i32>::with_capacity(32);
    let mut rng = rand::rng();

    // first stage: insert some initial data
    let initial_data: Vec<(i32, i32)> = (0..20).map(|i| (i, rng.random_range(0..1000))).collect();

    for (k, v) in initial_data.iter() {
        map.insert(*k, *v);
    }
    assert_eq!(map.len(), 20);

    // second stage: remove half of the data, create tombstone
    for i in 0..10 {
        assert_eq!(map.remove(&i), Some(initial_data[i as usize].1));
    }
    assert_eq!(map.len(), 10);

    // third stage: insert new data, should reuse tombstone position
    probe::reset_probe_num();
    let new_data: Vec<(i32, i32)> = (0..10).map(|i| (i, rng.random_range(0..1000))).collect();

    for (k, v) in new_data.iter() {
        map.insert(*k, *v);
    }

    // record average probe count
    let avg_probe_first = probe::get_probe_num() as f64 / 10.0;

    // verify all data can be accessed correctly
    for (k, v) in new_data.iter() {
        assert_eq!(map.get(k), Some(v));
    }

    for i in 10..20 {
        assert_eq!(map.get(&i), Some(&initial_data[i as usize].1));
    }

    // fourth stage: verify query performance
    probe::reset_probe_num();
    for (k, _) in new_data.iter() {
        map.get(k);
    }
    let avg_probe_query = probe::get_probe_num() as f64 / 10.0;

    // output performance statistics
    eprintln!(
        "Average probe count - Insert: {:.2}, Query: {:.2}",
        avg_probe_first, avg_probe_query
    );
}
