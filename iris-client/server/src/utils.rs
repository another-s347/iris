

use dashmap::DashMap;

use futures::prelude::*;

use hello_world::{
    *,
};

use proto::hello_world;


use pyo3::prelude::*;
use pyo3::{
    types::{IntoPyDict, PyBytes, PyDict, PyList, PyTuple, PyType},
    AsPyPointer, PyNativeType, PyTypeInfo,
};



use std::{
    collections::HashMap,
};






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

pub fn map_kwargs_to_local<'a>(
    object_map: &crate::mem::Mem,
    py: Python<'a>,
    args: Option<ProtoPyDict>,
    pickle: &PyObject,
    fetch_list: &HashMap<u64, u64>
) -> Option<PyObject> {
    let tuple = args;
    if let Some(tuple) = tuple {
        Some(map_kwargs_to_local_impl(&object_map, py, tuple, pickle, fetch_list))
    } else {
        None
    }
}

pub fn map_args_to_local<'a>(
    object_map: &crate::mem::Mem,
    py: Python<'a>,
    args: Option<ProtoPyTuple>,
    pickle: &PyObject,
    fetch_list: &HashMap<u64, u64>
) -> PyObject {
    let tuple = args;
    if let Some(tuple) = tuple {
        map_args_to_local_impl(&object_map, py, tuple, pickle, fetch_list)
    } else {
        PyTuple::empty(py).to_object(py)
    }
}

pub fn map_kwargs_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    py: Python<'a>,
    args: ProtoPyDict,
    pickle: &PyObject,
    fetch_list: &HashMap<u64, u64>
) -> PyObject {
    let mut tuple_args = vec![];
    for (key, x) in args.map {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                tuple_args.push((key, x.to_object(py)));
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push((key, x.to_object(py)));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push((key, b.to_object(py)));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                let o = loads(pickle, py, bytes.as_ref()).unwrap();
                tuple_args.push((key, o));
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push((key, map_kwargs_to_local_impl(maps, py, dict, pickle, fetch_list)));
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push((key, f.to_object(py)));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push((key, x.to_object(py)));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push((key, x.to_object(py)));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push((key, s.to_object(py)));
            }
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(&id.id).expect(&format!("id {}", id.id));
                let mut o = o.to_object(py);
                for attr in id.attr {
                    o = o.getattr(py, attr).unwrap();
                }
                tuple_args.push((key, o));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push((key, map_list_to_local_impl(maps, py, list, pickle, fetch_list)));
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push((key, map_args_to_local_impl(maps, py, tuple, pickle, fetch_list)));
            }
            None => {}
        }
    }

    tuple_args.into_py_dict(py).to_object(py)
}

pub fn map_list_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    py: Python<'a>,
    args: ProtoPyList,
    pickle: &PyObject,
    fetch_list: &HashMap<u64, u64>
) -> PyObject {
    let mut tuple_args: Vec<PyObject> = vec![];
    // py.run("print(args)", Some(vec![("args", args)].into_py_dict(py)), None).unwrap();
    for x in args.items {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                let object = x.to_object(py);
                tuple_args.push(object);
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push(b.to_object(py));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                let o = loads(pickle, py, bytes.as_ref()).unwrap();
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, py, dict, pickle, fetch_list));
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push(f.to_object(py));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push(s.to_object(py));
            }
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(&id.id).expect(&format!("id {}", id.id));
                let mut o = o.to_object(py);
                for attr in id.attr {
                    o = o.getattr(py, attr).unwrap();
                }
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, py, list, pickle, fetch_list))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, py, tuple, pickle, fetch_list));
            }
            None => {}
        }
    }

    PyList::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}

pub fn map_args_to_local_impl<'a>(
    maps: &crate::mem::Mem,
    py: Python<'a>,
    args: ProtoPyTuple,
    pickle: &PyObject,
    fetch_list: &HashMap<u64, u64>
) -> PyObject {
    let mut tuple_args: Vec<PyObject> = vec![];
    // py.run("print(args)", Some(vec![("args", args)].into_py_dict(py)), None).unwrap();
    for x in args.items {
        match x.data {
            Some(proto_py_any::Data::I32(x)) => {
                let object = x.to_object(py);
                tuple_args.push(object);
            }
            Some(proto_py_any::Data::I64(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::Boolean(b)) => {
                tuple_args.push(b.to_object(py));
            }
            Some(proto_py_any::Data::Bytes(bytes)) => {
                let o = loads(pickle, py, bytes.as_ref()).unwrap();
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, py, dict, pickle, fetch_list));
            }
            Some(proto_py_any::Data::F32(f)) => {
                tuple_args.push(f.to_object(py));
            }
            Some(proto_py_any::Data::U32(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::U64(x)) => {
                tuple_args.push(x.to_object(py));
            }
            Some(proto_py_any::Data::Str(s)) => {
                tuple_args.push(s.to_object(py));
            }
            Some(proto_py_any::Data::ObjectId(mut id)) => {
                if let Some(new_id) = fetch_list.get(&id.id) {
                    id.id = *new_id;
                    id.attr.clear();
                }
                let o = maps.get(&id.id).expect(&format!("id {}", id.id));
                let mut o = o.to_object(py);
                for attr in id.attr {
                    o = o.getattr(py, attr).unwrap();
                }
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, py, list, pickle, fetch_list))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, py, tuple, pickle, fetch_list));
            }
            None => {}
        }
    }
    
    PyTuple::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}