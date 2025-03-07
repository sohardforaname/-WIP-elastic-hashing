use std::hash::Hash;

use super::map::{ElasticHashMap, EntryState};

// 实现标准库的HashMap trait
impl<K, V> std::ops::Index<K> for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    type Output = V;

    fn index(&self, key: K) -> &Self::Output {
        self.get(&key).expect("no entry found for key")
    }
}

impl<K, V> std::ops::IndexMut<K> for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn index_mut(&mut self, key: K) -> &mut Self::Output {
        self.get_mut(&key).expect("no entry found for key")
    }
}

// 实现IntoIterator trait
impl<K, V> IntoIterator for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            data: self.data,
            index: 0,
        }
    }
}

// 迭代器结构体
pub struct IntoIter<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    data: Vec<EntryState<K, V>>,
    index: usize,
}

impl<K, V> Iterator for IntoIter<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.data.len() {
            if let EntryState::Occupied((k, v)) =
                std::mem::replace(&mut self.data[self.index], EntryState::Empty)
            {
                self.index += 1;
                return Some((k, v));
            }
            self.index += 1;
        }
        None
    }
}

// 实现FromIterator trait
impl<K, V> FromIterator<(K, V)> for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut map = ElasticHashMap::with_capacity(capacity.max(16));

        for (k, v) in iter {
            map.insert(k, v);
        }

        map
    }
}

// Implement Default trait
impl<K, V> Default for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        ElasticHashMap::with_capacity(16)
    }
}

// Implement Extend trait
impl<K, V> Extend<(K, V)> for ElasticHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}
