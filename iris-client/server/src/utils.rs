use dashmap::DashMap;

use futures::prelude::*;

use hello_world::*;

use proto::hello_world;

use pyo3::prelude::*;
use pyo3::{
    types::{IntoPyDict, PyBytes, PyDict, PyList, PyTuple, PyType},
    AsPyPointer, PyNativeType, PyTypeInfo,
};

use std::collections::HashMap;

pub fn dbg_py<T>(py: Python<'_>, x: PyResult<T>) -> PyResult<T> {
    if let Err(err) = &x {
        let err = err.clone_ref(py);
        err.print(py);
    }
    x
}

pub fn dumps<T>(pickle: &PyObject, py: Python<'_>, err: T) -> PyResult<Vec<u8>>
where
    T: IntoPy<PyObject>,
{
    let result = pickle.call_method1(py, "dumps", (err,))?;
    let bytes: &PyBytes = result.cast_as(py)?;
    let bytes = bytes.as_bytes().to_vec();
    Ok(bytes)
}

pub fn loads(pickle: &PyObject, py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject>
// where T:AsPyPointer + PyNativeType + PyTypeInfo
{
    if bytes.len() == 0 {
        return Ok(py.None());
    }
    // let pickle = self.pickle.to_object(py);
    let result = pickle.call_method1(py, "loads", (bytes,))?;
    Ok(result)
    // let result:&T = result.cast_as(py)?;
    // Ok(Py::from(result))
}

pub fn map_result(
    pickle: &PyObject,
    py: Python<'_>,
    result: PyResult<NodeObject>,
    current_node: &str,
) -> NodeObject {
    match result {
        Ok(r) => r,
        Err(e) => {
            let err = dumps(pickle, py, e).unwrap();
            NodeObject {
                exception: err,
                location: current_node.to_owned(),
                ..Default::default()
            }
        }
    }
}

pub enum LocalObject {
    List(Vec<LocalObject>),
    Kwargs(Vec<(String, LocalObject)>),
    Tuples(Vec<LocalObject>),
    I32(i32),
    I64(i64),
    Boolean(bool),
    F32(f32),
    U64(u64),
    U32(u32),
    Str(String),
    Object(PyObject, Vec<String>),
    Bytes(Vec<u8>),
}

impl LocalObject {
    pub fn to_pyobject(self, py: Python<'_>, pickle: &PyObject) -> PyObject {
        match self {
            LocalObject::List(mut x) => {
                PyList::new(py, x.drain(0..).map(|i|i.to_pyobject(py, pickle))).to_object(py)
            }
            LocalObject::Kwargs(mut x) => {
                x.drain(0..).map(|(key,o)|(key,o.to_pyobject(py, pickle))).into_py_dict(py).to_object(py)
            }
            LocalObject::Tuples(mut x) => {
                PyTuple::new(py, x.drain(0..).map(|i|i.to_pyobject(py, pickle))).to_object(py)
            }
            LocalObject::I32(x) => x.to_object(py),
            LocalObject::I64(x) => x.to_object(py),
            LocalObject::Boolean(x) => x.to_object(py),
            LocalObject::F32(x) => x.to_object(py),
            LocalObject::U64(x) => x.to_object(py),
            LocalObject::U32(x) => x.to_object(py),
            LocalObject::Str(x) => x.to_object(py),
            LocalObject::Object(mut o, attrs) => {
                for attr in attrs {
                    o = o.getattr(py, attr).unwrap();
                }
                o
            }
            LocalObject::Bytes(bytes) => {
                loads(pickle, py, bytes.as_ref()).unwrap()
            }
        }
    }
}

pub fn map_kwargs_to_local<'a>(
    object_map: &crate::mem::Mem,
    args: Option<ProtoPyDict>,
    fetch_list: &HashMap<u64, u64>,
) -> Option<LocalObject> {
    let tuple = args;
    if let Some(tuple) = tuple {
        Some(map_kwargs_to_local_impl(
            &object_map,
            tuple,
            fetch_list,
        ))
    } else {
        None
    }
}

pub fn map_args_to_local<'a>(
    object_map: &crate::mem::Mem,
    args: Option<ProtoPyTuple>,
    fetch_list: &HashMap<u64, u64>,
) -> LocalObject {
    let tuple = args;
    if let Some(tuple) = tuple {
        map_args_to_local_impl(&object_map, tuple, fetch_list)
    } else {
        LocalObject::Tuples(Vec::new())
    }
}

pub fn map_kwargs_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyDict,
    fetch_list: &HashMap<u64, u64>,
) -> LocalObject {
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
                    map_kwargs_to_local_impl(maps, dict, fetch_list),
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
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(id.id).expect(&format!("id {}", id.id));
                // for attr in id.attr {
                //     o = o.getattr(py, attr).unwrap();
                // }
                tuple_args.push((key, LocalObject::Object(o, id.attr)));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push((
                    key,
                    map_list_to_local_impl(maps, list, fetch_list),
                ));
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push((
                    key,
                    map_args_to_local_impl(maps, tuple, fetch_list),
                ));
            }
            None => {}
        }
    }

    LocalObject::Kwargs(tuple_args)
    // tuple_args.into_py_dict(py).to_object(py)
}

pub fn map_list_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyList,
    fetch_list: &HashMap<u64, u64>,
) -> LocalObject {
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
                tuple_args.push(map_kwargs_to_local_impl(maps, dict, fetch_list));
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
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(id.id).expect(&format!("id {}", id.id));
                // let mut o = o.to_object(py);
                // for attr in id.attr {
                //     o = o.getattr(py, attr).unwrap();
                // }
                tuple_args.push(LocalObject::Object(o, id.attr));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, list, fetch_list))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, tuple, fetch_list));
            }
            None => {}
        }
    }

    LocalObject::List(tuple_args)
    // PyList::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}

pub fn map_args_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    args: ProtoPyTuple,
    fetch_list: &HashMap<u64, u64>,
) -> LocalObject {
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
                tuple_args.push(map_kwargs_to_local_impl(maps, dict, fetch_list));
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
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(id.id).expect(&format!("id {}", id.id));
                // let mut o = o.to_object(py);
                // for attr in id.attr {
                //     o = o.getattr(py, attr).unwrap();
                // }
                tuple_args.push(LocalObject::Object(o, id.attr));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, list,  fetch_list))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, tuple, fetch_list));
            }
            None => {}
        }
    }

    // PyTuple::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
    LocalObject::Tuples(tuple_args)
}
