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

#[test]
fn test_elastic_hashmap_basic() {
    // 创建一个新的哈希表
    let mut map = ElasticHashMap::<String, i32>::with_capacity(16);

    // 测试插入和获取
    map.insert("one".to_string(), 1);
    map.insert("two".to_string(), 2);
    map.insert("three".to_string(), 3);

    assert_eq!(map.get("one"), Some(&1));
    assert_eq!(map.get("two"), Some(&2));
    assert_eq!(map.get("three"), Some(&3));
    assert_eq!(map.get("four"), None);

    // 测试长度
    assert_eq!(map.len(), 3);
    assert!(!map.is_empty());

    // 测试更新值
    map.insert("one".to_string(), 10);
    assert_eq!(map.get("one"), Some(&10));

    // 测试移除
    assert_eq!(map.remove("two"), Some(2));
    assert_eq!(map.get("two"), None);
    assert_eq!(map.len(), 2);

    // 测试清空
    map.clear();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
}

#[test]
fn test_elastic_hashmap_index() {
    let mut map = ElasticHashMap::<String, String>::with_capacity(16);

    map.insert("key1".to_string(), "value1".to_string());
    map.insert("key2".to_string(), "value2".to_string());

    // 测试索引操作符
    assert_eq!(&map["key1".to_string()], "value1");
    assert_eq!(&map["key2".to_string()], "value2");

    // 测试可变索引
    map["key1".to_string()] = "new_value".to_string();
    assert_eq!(&map["key1".to_string()], "new_value");
}

#[test]
fn test_elastic_hashmap_iterator() {
    let mut map = ElasticHashMap::<i32, String>::with_capacity(16);

    map.insert(1, "one".to_string());
    map.insert(2, "two".to_string());
    map.insert(3, "three".to_string());

    // 收集所有键值对
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

    // 从迭代器创建哈希表
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

    // 扩展哈希表
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

    // 测试可变引用
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

    // 第一阶段：插入一些初始数据
    let initial_data: Vec<(i32, i32)> = (0..20).map(|i| (i, rng.random_range(0..1000))).collect();

    for (k, v) in initial_data.iter() {
        map.insert(*k, *v);
    }
    assert_eq!(map.len(), 20);

    // 第二阶段：删除一半的数据，创建墓碑
    for i in 0..10 {
        assert_eq!(map.remove(&i), Some(initial_data[i as usize].1));
    }
    assert_eq!(map.len(), 10);

    // 第三阶段：插入新数据，应该能重用墓碑位置
    probe::reset_probe_num();
    let new_data: Vec<(i32, i32)> = (0..10).map(|i| (i, rng.random_range(0..1000))).collect();

    for (k, v) in new_data.iter() {
        map.insert(*k, *v);
    }

    // 记录平均探测次数
    let avg_probe_first = probe::get_probe_num() as f64 / 10.0;

    // 验证所有数据都能正确访问
    for (k, v) in new_data.iter() {
        assert_eq!(map.get(k), Some(v));
    }

    for i in 10..20 {
        assert_eq!(map.get(&i), Some(&initial_data[i as usize].1));
    }

    // 第四阶段：验证查询性能
    probe::reset_probe_num();
    for (k, _) in new_data.iter() {
        map.get(k);
    }
    let avg_probe_query = probe::get_probe_num() as f64 / 10.0;

    // 输出性能统计
    eprintln!(
        "Average probe count - Insert: {:.2}, Query: {:.2}",
        avg_probe_first, avg_probe_query
    );
}
