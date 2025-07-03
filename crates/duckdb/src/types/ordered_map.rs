use serde::{Serialize, Serializer};

/// An ordered map of key-value pairs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedMap<K, V>(Vec<(K, V)>);

impl<K: PartialEq, V> From<Vec<(K, V)>> for OrderedMap<K, V> {
    fn from(value: Vec<(K, V)>) -> Self {
        Self(value)
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

impl<K: Serialize, V: Serialize> Serialize for OrderedMap<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

