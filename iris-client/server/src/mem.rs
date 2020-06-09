use dashmap::DashMap;
use pyo3::prelude::*;
use std::{collections::{HashMap, hash_map::DefaultHasher}, sync::Arc};
use std::hash::{Hasher, Hash};

#[derive(Debug,Clone)]
pub struct MemCell {
    pub item: Py<PyAny>,
    pub remote: Option<String>
}
#[derive(Default,Debug,Clone)]
pub struct Mem {
    objects: Arc<DashMap<u64, MemCell>>,
    in_ref: Arc<DashMap<u64, u64>>,
    out_ref: Arc<DashMap<u64, Vec<String>>>
}

impl Mem {
    pub fn insert(&self, item: Py<PyAny>) -> u64 {
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        self.objects.insert(id, MemCell {
            item,
            remote: None
        });

        id
    }

    pub fn insert_in_ref(&self, fetch_list:&HashMap<u64, u64>) {
        for (key, value) in fetch_list {
            self.in_ref.insert(*key, *value);
        }
    }

    pub fn insert_out_ref(&self, id: u64, node: String) {
        if self.out_ref.contains_key(&id) {
            self.out_ref.update(&id, |_key, vec|{
                let mut v = vec.clone();
                v.push(node.clone());
                v
            });
        }
        else {
            self.out_ref.insert(id, vec![node]);
        }
    }

    pub fn get(&self, id: &u64) -> Option<Py<PyAny>> {
        self.objects.get(&id).map(|x|x.value().item.clone())
    }

    pub fn del(&self, id: u64) -> Option<Vec<String>> {
        self.objects.remove(&id);
        self.out_ref.remove_take(&id).map(|x|x.value().clone())
    }

    pub fn del_remote(&self, id:u64) {
        if let Some(id) = self.in_ref.remove_take(&id) {
            let id = id.value();
            self.objects.remove(id);
        }
    }
}