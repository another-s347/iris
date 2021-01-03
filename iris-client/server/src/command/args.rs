
use std::{collections::{HashMap, hash_map::DefaultHasher}, hash::{Hash, Hasher}, time::Duration};

use dashmap::DashMap;
use prost::bytes::Bytes;
use proto::n2n;
use pyo3::{PyObject, Python};
use tonic::{Request,Response};
use crate::{Opt, distributed, hello_world::{greeter_server::Greeter, *}, mem::LazyPyObject, utils::LocalObject};
use futures::FutureExt;
use anyhow::{Context, Result};
use anyhow::anyhow;
pub struct PrepareArgs<'a> {
    pub args: CallArgs,
    pub mem: &'a crate::mem::Mem,
    pub nodes: &'a DashMap<String, distributed::DistributedClient>
}

impl<'a> PrepareArgs<'a> {
    pub async fn prepare(self) -> crate::error::Result<PrepareArgsResult> {
        let fetch_list = &self.args.fetch_lists;
        let local_list = &self.args.local_lists;
        let mut nodes = Vec::with_capacity(fetch_list.len());
        for n in fetch_list {
            let node: &distributed::DistributedClient =
                &self.nodes.get(&n.location).expect(&n.location);
            let node = node.clone();
            nodes.push((node, n));
        }
        let tasks = nodes.iter_mut().map(|(node, o)| {
            let mut hasher = DefaultHasher::new();
            let id = uuid::Uuid::new_v4();
            id.hash(&mut hasher);
            let id = hasher.finish();
            let obj = o.clone();
            tokio::time::timeout(Duration::from_secs(20), node.get_object(tonic::Request::new(n2n::NodeObjectRef {
                id: o.id,
                attr: o.attr.clone(),
                location: o.location.clone(),
            })))
            .map(move |x| (id, x, obj))
        });

        let result: Vec<(u64, _, NodeObjectRef)> = futures::future::join_all(tasks).await;
        
        let mut ret = HashMap::with_capacity(result.len());
        for (id, b, r) in result {
            let b = b.with_context(||format!("{:#?}", r))??.into_inner();
            self.mem.insert(Some(id), LazyPyObject::new_serialized(Bytes::from(b.data)));
            ret.insert(r.id, id);
        }

        self.mem.insert_in_ref(&ret);

        let local_objects = local_list.iter().map(|x|{
            let id = x.id;
            tokio::time::timeout(Duration::from_secs(20),self.mem.get(x.id)).map(move|r|(id, r))
        });
        let mut local_objects:Vec<(u64, _)> = futures::future::join_all(local_objects).await;

        let mut local_ret = HashMap::new();
        local_objects.drain(0..)
            .try_for_each::<_, crate::error::Result<_>>(|(id,o)|{
                let o = o.with_context(||format!("timeout when fetching local object {:#?}", id))?.ok_or(anyhow::anyhow!("local object {} not found", id))?;
                local_ret.insert(id, o);
                Ok(())
            })?;
        
        let arg = map_args_to_local(self.mem, self.args.args, &ret, &local_ret).unwrap();
        let kwarg = map_kwargs_to_local(self.mem, self.args.kwargs, &ret, &local_ret)?;

        Ok(PrepareArgsResult {
            arg,
            kwarg
        })
    }
}

pub struct PrepareArgsResult {
    arg: LocalObject,
    kwarg: LocalObject
}

impl PrepareArgsResult {
    pub fn to_pyobject(self, py: Python<'_>, pickle: &PyObject) -> Result<(PyObject, PyObject)> {
        Ok((self.arg.to_pyobject(py, pickle)?, self.kwarg.to_pyobject(py, pickle)?))
    }
}

pub fn map_kwargs_to_local<'a>(
    object_map: &crate::mem::Mem,
    args: Option<ProtoPyDict>,
    fetch_list: &HashMap<u64, u64>,
    local_list: &HashMap<u64, LazyPyObject>
) -> Result<LocalObject> {
    let tuple = args;
    if let Some(tuple) = tuple {
        map_kwargs_to_local_impl(
            &object_map,
            tuple,
            fetch_list,
            local_list
        )
    } else {
        Ok(LocalObject::Kwargs(vec![]))
    }
}

pub fn map_args_to_local<'a>(
    object_map: &crate::mem::Mem,
    args: Option<ProtoPyTuple>,
    fetch_list: &HashMap<u64, u64>,
    local_list: &HashMap<u64, LazyPyObject>
) -> Result<LocalObject> {
    let tuple = args;
    if let Some(tuple) = tuple {
        map_args_to_local_impl(&object_map, tuple, fetch_list, local_list)
    } else {
        Ok(LocalObject::Tuples(vec![]))
    }
}

pub fn map_kwargs_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyDict,
    fetch_list: &HashMap<u64, u64>,
    local_list: &HashMap<u64, LazyPyObject>
) -> Result<LocalObject> {
    let mut tuple_args = vec![];
    for (key, x) in args.map {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                tuple_args.push((key, LocalObject::I32(x)));
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push((key, LocalObject::I64(x)));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push((key, LocalObject::Boolean(b)));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                // let o = loads(pickle, py, bytes.as_ref()).unwrap();
                tuple_args.push((key, LocalObject::Bytes(bytes)));
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push((
                    key,
                    map_kwargs_to_local_impl(maps, dict, fetch_list, local_list)?,
                ));
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push((key, LocalObject::F32(f)));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push((key, LocalObject::U32(x)));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push((key, LocalObject::U64(x)));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push((key, LocalObject::Str(s)));
            }
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = if let Some(new_id) = fetch_list.get(&id.id) {
                    maps.get_sync(*new_id).ok_or(anyhow!("not found expected object {} from fetch list", id.id))?
                } else if let Some(o) = local_list.get(&id.id) {
                    o.clone()
                } else {
                    maps.get_sync(id.id).ok_or(anyhow!("not found expected object {} from local", id.id))?
                };
                tuple_args.push((key, LocalObject::Object(o, id.attr)));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push((
                    key,
                    map_list_to_local_impl(maps, list, fetch_list, local_list)?,
                ));
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push((
                    key,
                    map_args_to_local_impl(maps, tuple, fetch_list, local_list)?,
                ));
            }
            None => {}
        }
    }

    Ok(LocalObject::Kwargs(tuple_args))
    // tuple_args.into_py_dict(py).to_object(py)
}

pub fn map_list_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyList,
    fetch_list: &HashMap<u64, u64>,
    local_list: &HashMap<u64, LazyPyObject>
) -> Result<LocalObject> {
    let mut tuple_args: Vec<LocalObject> = vec![];
    // py.run("print(args)", Some(vec![("args", args)].into_py_dict(py)), None).unwrap();
    for x in args.items {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                tuple_args.push(LocalObject::I32(x));
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push(LocalObject::I64(x));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push(LocalObject::Boolean(b));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                tuple_args.push(LocalObject::Bytes(bytes));
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, dict, fetch_list, local_list)?);
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push(LocalObject::F32(f));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push(LocalObject::U32(x));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push(LocalObject::U64(x));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push(LocalObject::Str(s));
            }
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = if let Some(new_id) = fetch_list.get(&id.id) {
                    maps.get_sync(*new_id).ok_or(anyhow!("not found expected object {} from fetch list", id.id))?
                } else if let Some(o) = local_list.get(&id.id) {
                    o.clone()
                } else {
                    maps.get_sync(id.id).ok_or(anyhow!("not found expected object {} from local, local_list: {:#?}", id.id, local_list.keys()))?
                };
                tuple_args.push(LocalObject::Object(o, id.attr));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, list, fetch_list, local_list)?)
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, tuple, fetch_list, local_list)?);
            }
            None => {}
        }
    }

    Ok(LocalObject::List(tuple_args))
    // PyList::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}

pub fn map_args_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyTuple,
    fetch_list: &HashMap<u64, u64>,
    local_list: &HashMap<u64, LazyPyObject>
) -> Result<LocalObject> {
    let mut tuple_args: Vec<LocalObject> = vec![];
    // py.run("print(args)", Some(vec![("args", args)].into_py_dict(py)), None).unwrap();
    for x in args.items {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                tuple_args.push(LocalObject::I32(x));
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push(LocalObject::I64(x));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push(LocalObject::Boolean(b));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                // let o = loads(pickle, py, bytes.as_ref()).unwrap();
                tuple_args.push(LocalObject::Bytes(bytes));
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, dict, fetch_list, local_list)?);
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push(LocalObject::F32(f));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push(LocalObject::U32(x));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push(LocalObject::U64(x));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push(LocalObject::Str(s));
            }
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = if let Some(new_id) = fetch_list.get(&id.id) {
                    maps.get_sync(*new_id).ok_or(anyhow!("not found expected object {} from fetch list", id.id))?
                } else if let Some(o) = local_list.get(&id.id) {
                    o.clone()
                } else {
                    maps.get_sync(id.id).ok_or(anyhow!("not found expected object {} from local, local_list: {:#?}", id.id, local_list.keys()))?
                };
                tuple_args.push(LocalObject::Object(o, id.attr));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, list,  fetch_list, local_list)?);
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, tuple, fetch_list, local_list)?);
            }
            None => {}
        }
    }

    // PyTuple::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
    Ok(LocalObject::Tuples(tuple_args))
}