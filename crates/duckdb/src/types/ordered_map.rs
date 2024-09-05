/// An ordered map of key-value pairs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedMap<K, V>(Vec<(K, V)>);

impl<K: PartialEq, V> From<Vec<(K, V)>> for OrderedMap<K, V> {
    fn from(value: Vec<(K, V)>) -> Self {
        OrderedMap(value)
    }
}

impl<K: std::cmp::PartialEq, V> OrderedMap<K, V> {
    /// Returns the value corresponding to the key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
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
