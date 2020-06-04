use common::IrisObjectId;
use dict_derive::{FromPyObject, IntoPyObject};
use futures::prelude::*;
use futures::stream::TryStreamExt;
use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    *,
};
use proto::hello_world;
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::{
    types::{IntoPyDict, PyBytes, PyDict, PyList, PyTuple, PyType},
    AsPyPointer, PyNativeType, PyTypeInfo,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use structopt::StructOpt;
use tokio::net::UnixListener;
use tokio::task;
use tonic::{transport::Server, Request, Response, Status};
use uuid;
use dashmap::DashMap;
#[derive(Debug, Clone)]
struct IrisServer {
    modules: Arc<DashMap<String, Py<PyModule>>>,
    objects: Arc<DashMap<u64, Py<PyAny>>>,
    pickle: Py<PyModule>,
    torch_rpc: Py<PyModule>,
    torch_handlers: Arc<HashMap<&'static str, Py<PyAny>>>,
    profile: Arc<dashmap::DashMap<&'static str, (usize, std::time::Duration)>>
}

fn dbg_py<T>(py: Python<'_>, x: PyResult<T>) -> PyResult<T> {
    if let Err(err) = &x {
        let err = err.clone_ref(py);
        err.print(py);
    }
    x
}

impl IrisServer {
    fn import_modules(
        &self,
        py: Python<'_>,
        modules: Vec<String>,
        paths: Vec<String>,
    ) -> PyResult<()> {
        let sys = py.import("sys")?;
        let path = sys.get("path")?.cast_as::<PyList>()?;

        for p in paths {
            path.append(p.as_str())?;
        }
        sys.add("path", path).unwrap();
        let path = sys.get("path")?.cast_as::<PyList>()?;
        println!("{:?}", path.repr());

        let mut state = &self.modules;
        for module_name in modules {
            println!("import.. {}", module_name);
            let py = py.import(module_name.as_str())?;
            state.insert(module_name, Py::from(py));
        }

        Ok(())
    }

    fn serialize<T>(&self, py: Python<'_>, err: T) -> PyResult<Vec<u8>>
    where
        T: IntoPy<PyObject>,
    {
        let pickle = self.pickle.to_object(py);
        let result = pickle.call_method1(py, "dumps", (err,))?;
        let bytes: &PyBytes = result.cast_as(py)?;
        let bytes = bytes.as_bytes().to_vec();
        Ok(bytes)
    }

    fn torch_remote<'a>(&'a self, py: Python<'a>, request: TorchRpcCallRequest) -> &'a PyAny {
        let rpc = self.torch_rpc.as_ref(py);
        // let (args, kwargs) = if let Some(arg) = request.arg {
        //     let pickle = self.pickle.to_object(py);
        //     (
        //         map_args_to_local(py, arg.args, &pickle),
        //         map_kwargs_to_local(py, arg.kwargs, &pickle),
        //     )
        // } else {
        //     (PyTuple::empty(py).to_object(py), None)
        // };
        unimplemented!()
        // let args:&[PyAny] = args.cast_as(py).unwrap().as_slice();
        // let new_args: &[PyObject] = &[request.object_id.into_py(py), request.attr.into_py(py)];
        // let key = request.torch_func.as_str();
        // let a:&[PyObject] = &[request.target_node.as_str().into_py(py), self.torch_handlers.get(&key).unwrap().to_object(py)];
        // let mut obj = rpc
        //     .call(
        //         "remote",
        //         PyTuple::new(py, a.as_ref()),
        //         Some(vec![("args", PyTuple::new(py, [new_args, &args].concat()))].into_py_dict(py)),
        //     )
        //     .unwrap();
        // if request.to_here {
        //     obj = dbg_py(py, obj.call_method0("to_here")).unwrap();
        // }
        // obj
    }

    // fn torch_rref(&self, py:Python<'_>, obj: PyAny) -> &PyAny {
    //     let rpc = self.torch_rpc.as_ref(py);

    // }
}

fn dumps<T>(pickle: &PyObject, py: Python<'_>, err: T) -> PyResult<Vec<u8>>
where
    T: IntoPy<PyObject>,
{
    let result = pickle.call_method1(py, "dumps", (err,))?;
    let bytes: &PyBytes = result.cast_as(py)?;
    let bytes = bytes.as_bytes().to_vec();
    Ok(bytes)
}

fn loads(pickle: &PyObject, py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject>
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

fn map_result(pickle: &PyObject, py: Python<'_>, result: PyResult<NodeObject>) -> NodeObject {
    match result {
        Ok(r) => r,
        Err(e) => {
            let err = dumps(pickle, py, e).unwrap();
            NodeObject {
                exception: err,
                ..Default::default()
            }
        }
    }
}

fn map_kwargs_to_local<'a>(
    object_map: &DashMap<u64, Py<PyAny>>,
    py: Python<'a>,
    args: Option<ProtoPyDict>,
    recursive: &PyObject,
) -> Option<PyObject> {
    let tuple = args;
    // let maps = object_map.lock().unwrap();

    if let Some(tuple) = tuple {
        Some(map_kwargs_to_local_impl(&object_map, py, tuple, recursive))
    } else {
        None
    }
}

fn map_args_to_local<'a>(
    object_map: &DashMap<u64, Py<PyAny>>,
    py: Python<'a>,
    args: Option<ProtoPyTuple>,
    recursive: &PyObject,
) -> PyObject {
    let tuple = args;
    // let maps = object_map.lock().unwrap();

    if let Some(tuple) = tuple {
        map_args_to_local_impl(&object_map, py, tuple, recursive)
    } else {
        PyTuple::empty(py).to_object(py)
    }
}

fn map_kwargs_to_local_impl<'a>(
    maps: &DashMap<u64, Py<PyAny>>,
    py: Python<'a>,
    args: ProtoPyDict,
    recursive: &PyObject,
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
                let o = loads(recursive, py, bytes.as_ref()).unwrap();
                tuple_args.push((key, o));
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push((key, map_kwargs_to_local_impl(maps, py, dict, recursive)));
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
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = maps.get(&id).unwrap().to_object(py);
                tuple_args.push((key, o));
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push((key, map_list_to_local_impl(maps, py, list, recursive)));
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push((key, map_args_to_local_impl(maps, py, tuple, recursive)));
            }
            None => {}
        }
    }

    tuple_args.into_py_dict(py).to_object(py)
}

fn map_list_to_local_impl<'a>(
    maps: &DashMap<u64, Py<PyAny>>,
    py: Python<'a>,
    args: ProtoPyList,
    recursive: &PyObject,
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
                let o = loads(recursive, py, bytes.as_ref()).unwrap();
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, py, dict, recursive));
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
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = maps.get(&id).unwrap();
                let o = o.to_object(py);
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, py, list, recursive))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, py, tuple, recursive));
            }
            None => {}
        }
    }

    PyList::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}
fn map_args_to_local_impl<'a>(
    maps: &DashMap<u64, Py<PyAny>>,
    py: Python<'a>,
    args: ProtoPyTuple,
    recursive: &PyObject,
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
                let o = loads(recursive, py, bytes.as_ref()).unwrap();
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::Dict(dict)) => {
                tuple_args.push(map_kwargs_to_local_impl(maps, py, dict, recursive));
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
            Some(proto_py_any::Data::ObjectId(id)) => {
                let o = maps.get(&id).unwrap();
                let o = o.to_object(py);
                tuple_args.push(o);
            }
            Some(proto_py_any::Data::List(list)) => {
                tuple_args.push(map_list_to_local_impl(maps, py, list, recursive))
            }
            Some(proto_py_any::Data::Tuple(tuple)) => {
                tuple_args.push(map_args_to_local_impl(maps, py, tuple, recursive));
            }
            None => {}
        }
    }

    PyTuple::new(py, tuple_args.iter().map(|x| x.as_ref(py))).to_object(py)
}

fn _call(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    py: Python<'_>,
    request: CallRequest,
    pickle: &PyObject,
) -> PyResult<NodeObject> {
    let mut maps = object_map;
    let (args, kwargs) = if let Some(arg) = request.arg {
        (
            map_args_to_local(&maps, py, arg.args, &pickle),
            map_kwargs_to_local(&maps, py, arg.kwargs, &pickle),
        )
    } else {
        (PyTuple::empty(py).to_object(py), None)
    };
    let o = maps.get(&request.object_id).unwrap().clone();
    let mut o = o.as_ref(py);
    if !request.attr.is_empty() {
        o = dbg_py(py, o.getattr(&request.attr)).unwrap();
    }
    let ret = if let Some(k) = kwargs {
        o.call(args.cast_as(py)?, Some(k.cast_as(py)?))?
    } else {
        o.call(args.cast_as(py)?, None)?
    };

    let mut hasher = DefaultHasher::new();
    let id = uuid::Uuid::new_v4();
    id.hash(&mut hasher);
    let id = hasher.finish();

    maps.insert(id, Py::from(ret));

    return Ok(NodeObject {
        id,
        ..Default::default()
    });
}

fn create_object(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    modules: Arc<DashMap<String, Py<PyModule>>>,
    py:Python<'_>,
    request: CreateRequest,
    pickle: &PyObject,
) -> PyResult<NodeObject> {
    let module = modules;
    if let Some(m) = module.get(&request.module) {
        let m = m.to_object(py);
        let m = m.getattr(py, request.qualname)?;
        let mut maps = object_map;
        let (args, kwargs) = if let Some(arg) = request.arg {
            let pickle = pickle.to_object(py);
            (
                map_args_to_local(&maps, py, arg.args, &pickle),
                map_kwargs_to_local(&maps, py, arg.kwargs, &pickle),
            )
        } else {
            (PyTuple::empty(py).to_object(py), None)
        };

        let new_object = m.call(
            py,
            args.cast_as(py)?,
            kwargs.as_ref().map(|x| x.cast_as(py).unwrap()),
        )?;
        let new_object = Py::from(new_object.as_ref(py));
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        maps.insert(id, new_object);

        return Ok(NodeObject {
            location: "".to_owned(),
            id,
            ..Default::default()
        });
    } else {
        let err =
            PyErr::new::<exceptions::KeyError, _>(format!("Module {} not found.", request.module));
        return Err(err);
    }
}

fn apply(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    modules: Arc<DashMap<String, Py<PyModule>>>,
    py: Python<'_>,
    request: ApplyRequest,
    pickle: &PyObject,
) -> PyResult<NodeObject> {
    let mut maps = object_map;
    let (args, kwargs) = if let Some(arg) = request.arg {
        (
            map_args_to_local(&maps, py, arg.args, &pickle),
            map_kwargs_to_local(&maps, py, arg.kwargs, &pickle),
        )
    } else {
        (PyTuple::empty(py).to_object(py), None)
    };

    let func = loads(&pickle, py, request.func.as_ref()).unwrap();

    let func = func.as_ref(py);
    let obj = func
        .call(
            args.cast_as(py).unwrap(),
            kwargs.as_ref().map(|x| x.cast_as(py).unwrap()),
        )
        .unwrap();

    let mut hasher = DefaultHasher::new();
    let id = uuid::Uuid::new_v4();
    id.hash(&mut hasher);
    let id = hasher.finish();

    maps.insert(id, Py::from(obj));

    return Ok(NodeObject {
        id,
        ..Default::default()
    });
}

#[tonic::async_trait]
impl Greeter for IrisServer {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let request = request.into_inner();
        unimplemented!()
    }

    async fn init(&self, request: Request<InitRequest>) -> Result<Response<NodeObject>, Status> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let request = request.into_inner();
        let modules: Vec<String> = request.modules;
        let paths: Vec<String> = request.paths;
        if let Err(err) = self.import_modules(py, modules, paths) {
            return Ok(Response::new(NodeObject {
                exception: self.serialize(py, err).unwrap(),
                location: "".to_owned(),
                ..Default::default()
            }));
        }
        return Ok(Response::new(NodeObject {
            r#type: "init".to_owned(),
            location: "".to_owned(),
            ..Default::default()
        }));
    }

    async fn call(&self, request: Request<CallRequest>) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request: CallRequest = request.into_inner();
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let call_task = task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let pickle = pickle.to_object(py);
            let result = _call(object_map, py, request, &pickle);
            map_result(&pickle, py, result)
        })
        .await
        .unwrap();
        let end = std::time::Instant::now();
        let d:std::time::Duration = end - start;
        self.profile.update("call", move|key, (count, value)|{
            (count+1, *value + d)
        });
        return Ok(Response::new(call_task));
    }

    async fn create_object(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let modules = self.modules.clone();
        let result = task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let pickle = pickle.to_object(py);
            let result = create_object(object_map, modules, py, request, &pickle);
            map_result(&pickle, py, result)
        })
        .await
        .unwrap();
        let end = std::time::Instant::now();
        let d:std::time::Duration = end - start;
        self.profile.update("create", move|key, (count, value)|{
            (count+1, *value + d)
        });
        return Ok(Response::new(result));
    }

    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let modules = self.modules.clone();
        let result = task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let pickle = pickle.to_object(py);
            let result = apply(object_map, modules, py, request, &pickle);
            map_result(&pickle, py, result)
        })
        .await
        .unwrap();
        let end = std::time::Instant::now();
        let d:std::time::Duration = end - start;
        self.profile.update("apply", move|key, (count, value)|{
            (count+1, *value + d)
        });
        return Ok(Response::new(result));
    }

    async fn torch_call(
        &self,
        request: Request<TorchRpcCallRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let obj = self.torch_remote(py, request);

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        let mut maps = &self.objects; //.lock().unwrap();
        maps.insert(id, Py::from(obj));

        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        let mut maps = &self.objects; //.lock().unwrap();
        let obj = maps.get(&request.object_id).unwrap();
        let obj = obj.to_object(py);
        let obj = obj.as_ref(py);
        let obj = obj.getattr(request.attr.as_str()).unwrap();
        maps.insert(id, Py::from(obj));

        let end = std::time::Instant::now();
        let d:std::time::Duration = end - start;
        self.profile.update("getattr", move|key, (count, value)|{
            (count+1, *value + d)
        });
        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn get_parameter(
        &self,
        request: Request<GetParameterRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut maps = &self.objects; //.lock().unwrap();
        let rpc: &PyModule = self.torch_rpc.as_ref(py);
        let model:&Py<PyAny> = &maps.get(&request.object_id).unwrap();
        let model: PyObject = model.into_py(py);
        // let parameters = model.call_method0(py, "parameters").unwrap();
        // let parameters:&pyo3::types::PySequence = parameters.cast_as(py).unwrap();
        // let ret = PyList::empty(py);
        // for p in parameters.list().unwrap() {
        //     ret.append(rpc.call_method1("RRef", (p,)).unwrap()).unwrap();
        // }
        let ret = py.eval(
            "[RRef(p) for p in model.parameters()]",
            Some(
                vec![
                    ("model", model.as_ref(py)),
                    ("RRef", rpc.get("RRef").unwrap()),
                ]
                .into_py_dict(py),
            ),
            None,
        );
        let ret = dbg_py(py, ret).unwrap();

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        maps.insert(id, Py::from(ret));
        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn del_object(
        &self,
        request: Request<GetParameterRequest>,
    ) -> Result<Response<ObjectId>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();

        let mut maps = &self.objects;//.lock().unwrap();
        maps.remove(&request.object_id);

        let end = std::time::Instant::now();
        let d:std::time::Duration = end - start;
        self.profile.update("del", move|key, (count, value)|{
            (count+1, *value + d)
        });
        return Ok(Response::new(ObjectId {
            id: request.object_id,
        }));
    }

    async fn get_value(
        &self,
        request: Request<GetParameterRequest>,
    ) -> Result<Response<Value>, Status> {
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let maps = &self.objects;//.lock().unwrap();
        let obj = maps.get(&request.object_id).unwrap();

        let data = self.serialize(py, obj.to_object(py)).unwrap();
        return Ok(Response::new(Value { data }));
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(short, long)]
    pub debug: bool,

    /// Set speed
    // we don't want to name it "speed", need to look smart
    #[structopt(short = "r", long = "rank", default_value = "0")]
    pub rank: u64,

    #[structopt(short = "w", long = "world_size", default_value = "2")]
    pub world_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt: Opt = Opt::from_args();

    let pickle = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let pickle = py.import("dill").unwrap();
        Py::from(pickle)
    };

    let torch_handlers = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let module = dbg_py(
            py,
            PyModule::from_code(
                py,
                r#"
def torch_GetParameters(object_id, attr, *args, **kwargs):
    return None
    global service
    # print("torch_GetParameters", object_id)
    model = service.object_map[object_id]
    return [rpc.RRef(p) for p in model.parameters()]


def torch_CallWithObject(object_id, attr, *args, **kwargs):
    return None
    global service
    o = service.object_map[object_id]
    if attr:
        o = getattr(o, attr)
    result = o(*args, **kwargs)
    return result


def torch_GetObject(object_id, attr, *args, **kwargs):
    return None
    global service
    o = service.object_map[object_id]
    return o
        "#,
                "torch_handlers.py",
                "torch_handlers",
            ),
        )
        .unwrap();
        let torch_CallWithObject_handler = module.get("torch_CallWithObject").unwrap();
        let torch_GetParameters_handler = module.get("torch_GetParameters").unwrap();
        let torch_GetObject_handler = module.get("torch_GetObject").unwrap();
        let mut map = HashMap::new();
        map.insert("torch_GetParameters", Py::from(torch_GetParameters_handler));
        map.insert("torch_GetObject", Py::from(torch_GetObject_handler));
        map.insert(
            "torch_CallWithObject",
            Py::from(torch_CallWithObject_handler),
        );
        // map.insert("test", Py::from(double));
        map
    };

    let torch_rpc = {
        #[derive(FromPyObject, IntoPyObject)]
        struct Args {
            rank: u64,
            world_size: u64,
        }

        let gil = Python::acquire_gil();
        let py = gil.python();
        let rpc = py.import("torch.distributed.rpc").unwrap();
        let kwargs: PyObject = Args {
            rank: opt.rank,
            world_size: opt.world_size,
        }
        .into_py(py);
        let name = format!("node{}", opt.rank);
        // let result = rpc.call_method("init_rpc", (&name,), Some(
        //     kwargs.cast_as(py).unwrap()
        // ));
        // dbg_py(py, result);
        Py::from(rpc)
    };

    let path = format!("/tmp/iris-tmp-node{}-{}.sock", opt.rank, opt.rank);

    tokio::fs::create_dir_all(Path::new(&path).parent().unwrap()).await?;

    let mut uds = UnixListener::bind(&path)?;

    let profile = dashmap::DashMap::new();
    profile.insert("call", (0, std::time::Duration::default()));
    profile.insert("apply", (0, std::time::Duration::default()));
    profile.insert("getattr", (0, std::time::Duration::default()));
    profile.insert("create", (0, std::time::Duration::default()));
    profile.insert("del", (0, std::time::Duration::default()));
    let profile = Arc::new(profile);

    let greeter = IrisServer {
        modules: Arc::new(DashMap::new()),
        objects: Arc::new(DashMap::new()),
        pickle,
        torch_rpc,
        torch_handlers: Arc::new(torch_handlers),
        profile: profile.clone()
    };
    let p = profile.clone();
    let (tx, mut rx) = tokio::sync::broadcast::channel(1);
    ctrlc::set_handler(move || {
        tx.send(()).unwrap();
    })
    .expect("Error setting Ctrl-C handler");

    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            t.tick().await;
            println!("{:?}", p);
        }
    });

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        // .serve_with_shutdown("127.0.0.1:12345".parse().unwrap(), rx.recv().map(|_|()))
        .serve_with_incoming_shutdown(
            uds.incoming().map_ok(unix::UnixStream),
            rx.recv().map(|_| ()),
        )
        .await?;

    tokio::fs::remove_file(Path::new(&path)).await?;

    Ok(())
}

#[cfg(unix)]
mod unix {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite};
    use tonic::transport::server::Connected;

    #[derive(Debug)]
    pub struct UnixStream(pub tokio::net::UnixStream);

    impl Connected for UnixStream {}

    impl AsyncRead for UnixStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for UnixStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
