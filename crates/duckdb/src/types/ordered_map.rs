/// An ordered map of key-value pairs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedMap<K, V>(pub(crate) Vec<(K, V)>);

impl<K, V> Default for OrderedMap<K, V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<K: PartialEq, V> From<Vec<(K, V)>> for OrderedMap<K, V> {
    fn from(value: Vec<(K, V)>) -> Self {
        Self(value)
    }
}

impl<K: std::cmp::PartialEq, V> OrderedMap<K, V> {
    /// Creates an empty `OrderedMap`.
    pub fn new() -> Self {
        Self(Vec::new())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
    /// Inserts a key-value pair, replacing any existing value for the key.
    ///
    /// Returns the previous value if the key already existed.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some((_, existing)) = self.0.iter_mut().find(|(k, _)| *k == key) {
            return Some(std::mem::replace(existing, value));
        }
        self.0.push((key, value));
        None
    }
    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Returns an iterator over the keys in the map.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.0.iter().map(|(k, _)| k)
    }
    /// Returns an iterator over the values in the map.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.0.iter().map(|(_, v)| v)
    }
    /// Returns an iterator over the key-value pairs in the map.
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.0.iter()
    }
}
