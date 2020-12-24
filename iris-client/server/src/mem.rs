use dashmap::DashMap;
use pyo3::prelude::*;
use std::hash::{Hash, Hasher};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    sync::Arc,
};
use waitmap::WaitMap;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Key(pub u64);

impl From<&Key> for Key {
    fn from(i: &Key) -> Self {
        Key(i.0)
    }
}

#[derive(Debug, Clone)]
pub struct MemCell {
    pub item: Py<PyAny>,
    pub remote: Option<String>,
}
#[derive(Clone)]
pub struct Mem {
    objects: Arc<DashMap<Key, MemCell>>,
    in_ref: Arc<DashMap<Key, u64>>,
    out_ref: Arc<DashMap<Key, Vec<String>>>,
}

impl Default for Mem {
    fn default() -> Self {
        Mem {
            objects: Arc::new(DashMap::new()),
            in_ref: Arc::new(DashMap::new()),
            out_ref: Arc::new(DashMap::new()),
        }
    }
}

impl Mem {
    pub fn insert(&self, item: Py<PyAny>) -> u64 {
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        self.objects.insert(Key(id), MemCell { item, remote: None });

        id
    }

    pub fn insert_in_ref(&self, fetch_list: &HashMap<u64, u64>) {
        for (key, value) in fetch_list {
            self.in_ref.insert(Key(*key), *value);
        }
    }

    pub fn insert_out_ref(&self, id: u64, node: String) {
        if self.out_ref.contains_key(&Key(id)) {
            self.out_ref.update(&Key(id), |_key, vec| {
                let mut v = vec.clone();
                v.push(node.clone());
                v
            });
        } else {
            self.out_ref.insert(Key(id), vec![node]);
        }
    }

    pub fn get(&self, id: u64) -> Option<Py<PyAny>> {
        self.objects.get(&Key(id)).map(|x| x.value().item.clone())
    }

    pub fn del(&self, id: u64) -> Option<Vec<String>> {
        self.objects.remove(&Key(id));
        self.out_ref.remove_take(&Key(id)).map(|x| x.value().clone())
    }

    pub fn del_remote(&self, id: u64) {
        if let Some(id) = self.in_ref.remove_take(&Key(id)) {
            let id = id.value();
            self.objects.remove(&Key(*id));
        }
    }
}
