use dashmap::DashMap;
use prost::bytes::Bytes;
use pyo3::prelude::*;
use tracing::{debug, info};
use std::{hash::{Hash, Hasher}, sync::atomic::Ordering};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    sync::Arc,
};
use waitmap::WaitMap;

use crate::utils::loads;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Key(pub u64);

impl From<&Key> for Key {
    fn from(i: &Key) -> Self {
        Key(i.0)
    }
}

pub struct MemCell {
    pub item: LazyPyObject,
    pub remote: Option<String>,
}
#[derive(Clone)]
pub struct Mem {
    objects: Arc<WaitMap<Key, MemCell>>,
    in_ref: Arc<DashMap<Key, u64>>,
    out_ref: Arc<DashMap<Key, Vec<String>>>,
    balance: Arc<std::sync::atomic::AtomicI64>
}

#[derive(Clone)]
pub struct LazyPyObject {
    object: once_cell::sync::OnceCell<PyObject>,
    inner: LazyPyObjectInner
}

impl LazyPyObject {
    pub fn new_object(object: PyObject) -> Self {
        Self {
            object: once_cell::sync::OnceCell::new(),
            inner: LazyPyObjectInner::PyObject(object)
        }
    }

    pub fn new_serialized(bytes: Bytes) -> Self {
        Self {
            object: once_cell::sync::OnceCell::new(),
            inner: LazyPyObjectInner::Serialized(bytes)
        }
    }

    pub fn get(&self, pickle: &PyObject, py: Python<'_>) -> PyResult<PyObject> {
        self.object.get_or_try_init(||{
            match &self.inner {
                LazyPyObjectInner::Serialized(bytes) => {
                    loads(pickle, py, bytes.as_ref())
                }
                LazyPyObjectInner::PyObject(object) => {
                    Ok(object.clone())
                }
            }
        }).map(|x|x.clone())
    }
}

#[derive(Clone)]
enum LazyPyObjectInner {
    Serialized(Bytes),
    PyObject(PyObject),
}

impl Default for Mem {
    fn default() -> Self {
        Mem {
            objects: Arc::new(WaitMap::new()),
            in_ref: Arc::new(DashMap::new()),
            out_ref: Arc::new(DashMap::new()),
            balance: Default::default()
        }
    }
}

impl Mem {
    pub fn insert(&self, id:Option<u64>, item: LazyPyObject) -> u64 {
        let id = if let Some(i) = id {
            i
        } else {
            let mut hasher = DefaultHasher::new();
            let id = uuid::Uuid::new_v4();
            id.hash(&mut hasher);
            hasher.finish()
        };
        self.objects.insert(Key(id), MemCell { item, remote: None });
        let b = self.balance.fetch_add(1, Ordering::SeqCst);
        debug!("object balance: {}", b);

        id
    }

    pub fn insert_in_ref(&self, fetch_list: &HashMap<u64, u64>) {
        for (key, value) in fetch_list {
            self.in_ref.insert(Key(*key), *value);
        }
    }

    pub fn insert_out_ref(&self, id: u64, node: String) {
        if let Some(mut x) = self.out_ref.get_mut(&Key(id)) {
            x.push(node);
        } else {
            self.out_ref.insert(Key(id), vec![node]);
        }
    }

    pub async fn get(&self, id: u64) -> Option<LazyPyObject> {
        // let start = std::time::Instant::now();
        let result = self.objects.wait(&Key(id)).await.map(|x| x.value().item.clone());
        // println!("get wait {:?} s",std::time::Instant::now().duration_since(start).as_secs_f64());
        result
    }

    pub fn get_sync(&self, id: u64) -> Option<LazyPyObject> {
        // let start = std::time::Instant::now();
        let result = self.objects.get(&Key(id)).map(|x| x.value().item.clone());
        // println!("get wait {:?} s",std::time::Instant::now().duration_since(start).as_secs_f64());
        result
    }

    pub async fn get_exist(&self, id: u64) -> Option<LazyPyObject> {
        if self.objects.have(&Key(id)) {
            // let start = std::time::Instant::now();
            let result = self.objects.wait(&Key(id)).await.map(|x| x.value().item.clone());
            // println!("get wait {:?} s",std::time::Instant::now().duration_since(start).as_secs_f64());
            result
        }
        else {
            None
        }
    }

    pub async fn del(&self, id: u64) -> Option<Vec<String>> {
        debug!("del object {}", id);
        if self.objects.have(&Key(id)) {
            self.objects.wait(&Key(id)).await;
            self.objects.cancel(&Key(id));
            self.balance.fetch_sub(1, Ordering::SeqCst);
            self.out_ref.remove(&Key(id)).map(|(_,x)| x.clone())
        }
        else {
            None
        }
    }

    pub fn del_remote(&self, id: u64) {
        debug!("del remote object {}", id);
        if let Some((_,id)) = self.in_ref.remove(&Key(id)) {
            self.objects.cancel(&Key(id));
            self.balance.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
