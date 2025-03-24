use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;
use std::{collections::HashMap, hash::Hash};

use super::message::ErrorCode;

#[derive(Default, Clone, Debug, PartialEq)]
pub(super) struct KeyValueStore<K, V>
where
    K: Hash + Eq + Send,
    V: PartialEq + Send,
{
    map: HashMap<K, V>,
}

impl<K, V> KeyValueStore<K, V>
where
    K: Hash + Eq + Send,
    V: PartialEq + Send,
{
    pub fn new_with_inner(inner: HashMap<K, V>) -> Self {
        Self { map: inner }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn read(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn write(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    pub fn cas(&mut self, key: K, from: V, to: V) -> anyhow::Result<()> {
        let res = self.map.get_mut(&key);

        match res {
            Some(current) => {
                if *current != from {
                    return Err(anyhow::Error::new(ErrorCode::PreconditionFailed));
                }
                *current = to;
                Ok(())
            }
            None => Err(anyhow::Error::new(ErrorCode::KeyDoesNotExist)),
        }
    }
}

impl<'de> Deserialize<'de> for KeyValueStore<usize, usize> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let incoming_map: HashMap<String, usize> = HashMap::deserialize(deserializer)?;
        let inner = incoming_map
            .into_iter()
            // this unwrap is fine -- this is just a toy implementation where all the clients and the nodes always use usize :)
            .map(|(k, v)| (usize::from_str(&k).unwrap(), v))
            .collect::<HashMap<usize, usize>>();

        Ok(KeyValueStore::new_with_inner(inner))
    }
}

impl Serialize for KeyValueStore<usize, usize> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.map.len()))?;
        for (k, v) in &self.map {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}
