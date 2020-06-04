#![feature(async_closure)]
use futures::future::join_all;
use futures::prelude::*;
use hello_world::greeter_client::GreeterClient;
use hello_world::*;
use pyo3::prelude::*;
use pyo3::pyclass::PyClass;
use pyo3::types::{PyBytes, PyList};
use pyo3::types::{PyDict, PyTuple};
use pyo3::wrap_pyfunction;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UnixStream;
use tokio::prelude::*;
use tokio::runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use uuid;

type RpcClient = GreeterClient<tonic::transport::channel::Channel>;
type tonicResponseResult<T> = Result<tonic::Response<T>, tonic::Status>;

use proto::hello_world;

pub mod context;
use context::IrisContextInternal;

/// Represents a file that can be searched
#[pyclass(module = "client")]
struct WordCounter {
    path: PathBuf,
}

#[pyclass(module = "client")]
struct IrisClientInternal {
    pub runtime_handle: tokio::runtime::Handle,
    pub client: RpcClient,
    pub async_tasks: HashMap<uuid::Uuid, AsyncIrisObjectTask>,
}

#[pyclass(module = "client")]
struct AsyncIrisObjectTask {
    pub inner: JoinHandle<IrisObjectInternal>,
}

#[pyclass(module = "client")]
#[derive(Clone)]
struct AsyncTaskKey {
    uuid: uuid::Uuid,
}
#[pyclass(module = "client")]
#[derive(Clone)]
struct IrisObjectInternal {
    pub inner: Arc<GuardedIrisObject>,
}

struct GuardedIrisObject {
    pub runtime_handle: tokio::runtime::Handle,
    pub client: RpcClient,
    pub node_ref: NodeObject,
    pub time_cost: Duration,
}

impl GuardedIrisObject {
    fn id(self: &Arc<Self>) -> u64 {
        self.as_ref().node_ref.id
    }

    fn time_cost_as_sec(self: &Arc<Self>) -> f64 {
        self.as_ref().time_cost.as_secs_f64()
    }

    fn exception<'p>(self: &'p Arc<Self>) -> &'p [u8] {
        self.node_ref.exception.as_ref()
    }

    fn _del(&mut self, request: GetParameterRequest) {
        let mut client = self.client.clone();
        self.runtime_handle.spawn(async move {
            client
                .del_object(tonic::Request::new(request))
                .await
                .unwrap();
        });
    }
}

impl Drop for GuardedIrisObject {
    fn drop(&mut self) {
        let request = GetParameterRequest {
            object_id: self.node_ref.id,
        };
        // println!("drop object {}, type {}", self.node_ref.id, self.node_ref.r#type);
        self._del(request)
    }
}

#[pymethods]
impl IrisObjectInternal {
    fn get_value<'p>(&mut self, py: Python<'p>) -> Option<&'p PyBytes> {
        let request = GetParameterRequest {
            object_id: self.inner.id(),
        };
        let data = py.allow_threads(|| self._get_value(request));
        return data.map(|x| PyBytes::new(py, x.as_ref()));
    }

    fn call(
        &mut self,
        py: Python<'_>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        recursive: Option<bool>,
        attr: Option<String>,
        pickle: &PyAny,
    ) -> Option<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive, py, &pickle.to_object(py));

        py.allow_threads(|| self._call(arg, attr))
    }

    fn get_attr(&mut self, py: Python<'_>, attr: String) -> Option<IrisObjectInternal> {
        let request = GetAttrRequest {
            attr,
            object_id: self.inner.id(),
        };
        Some(py.allow_threads(|| self._get_attr(request)))
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn exception<'p>(&self, py: Python<'p>) -> Option<&'p PyBytes> {
        let bytes = self.inner.exception();
        if bytes.len() == 0 {
            return None;
        } else {
            Some(PyBytes::new(py, self.inner.exception()))
        }
    }

    fn time_cost_as_sec(&self) -> f64 {
        self.inner.time_cost_as_sec()
    }
}

impl IrisObjectInternal {
    fn _del(&mut self, request: GetParameterRequest) {
        let mut client = self.inner.client.clone();
        self.inner.runtime_handle.spawn(async move {
            client
                .del_object(tonic::Request::new(request))
                .await
                .unwrap();
        });
    }

    fn _get_value(&mut self, request: GetParameterRequest) -> Option<Vec<u8>> {
        let task_handle = self.inner.runtime_handle.block_on(
            self.inner
                .client
                .clone()
                .get_value(tonic::Request::new(request)),
        );
        let ret = task_handle.map(|x| x.into_inner()).unwrap();
        Some(ret.data)
    }

    fn _call(&mut self, arg: Option<CallArgs>, attr: Option<String>) -> Option<IrisObjectInternal> {
        let start = Instant::now();
        let mut client = self.inner.client.clone();
        let task_handle = self
            .inner
            .runtime_handle
            .block_on(client.call(tonic::Request::new(CallRequest {
                object_id: self.id(),
                arg,
                attr: attr.unwrap_or_default(),
            })));
        Some(IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.inner.runtime_handle.clone(),
                client,
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        })
    }

    fn _get_attr(&mut self, request: GetAttrRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let mut client = self.inner.client.clone();
        let task_handle = self
            .inner
            .runtime_handle
            .block_on(client.get_attr(tonic::Request::new(request)));

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.inner.runtime_handle.clone(),
                client,
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        }
    }
}

impl IrisClientInternal {
    fn _create_object(&mut self, request: CreateRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let task_handle = self
            .runtime_handle
            .block_on(self.client.create_object(tonic::Request::new(request)));

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        }
    }

    fn _init(&mut self, request: InitRequest) {
        self.runtime_handle
            .block_on(self.client.init(tonic::Request::new(request)))
            .unwrap();
    }

    fn _torch_call(&mut self, request: TorchRpcCallRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let task_handle = self
            .runtime_handle
            .block_on(self.client.torch_call(tonic::Request::new(request)));

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        }
    }

    fn _torch_call_async(
        &mut self,
        request: TorchRpcCallRequest,
    ) -> JoinHandle<IrisObjectInternal> {
        let mut client = self.client.clone();
        let runtime_handle = self.runtime_handle.clone();
        let task_handle = self.runtime_handle.spawn(async move {
            let start = Instant::now();
            let result = client.torch_call(tonic::Request::new(request)).await;
            IrisObjectInternal {
                inner: Arc::new(GuardedIrisObject {
                    client,
                    runtime_handle,
                    node_ref: result.map(|x| x.into_inner()).unwrap(),
                    time_cost: Instant::now() - start,
                }),
            }
        });
        task_handle
    }

    fn _get_parameter(&mut self, request: GetParameterRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let task_handle = self
            .runtime_handle
            .block_on(self.client.get_parameter(tonic::Request::new(request)));

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        }
    }

    fn _get_parameter_async(
        &mut self,
        request: GetParameterRequest,
    ) -> JoinHandle<IrisObjectInternal> {
        let start = Instant::now();
        let mut client = self.client.clone();
        let runtime_handle = self.runtime_handle.clone();
        let task_handle = self.runtime_handle.spawn(async move {
            let result = client.get_parameter(tonic::Request::new(request)).await;
            IrisObjectInternal {
                inner: Arc::new(GuardedIrisObject {
                    client,
                    runtime_handle,
                    node_ref: result.map(|x| x.into_inner()).unwrap(),
                    time_cost: Instant::now() - start,
                }),
            }
        });
        task_handle
    }

    fn _apply(&mut self, request: ApplyRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let task_handle = self
            .runtime_handle
            .block_on(self.client.apply(tonic::Request::new(request)));

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref: task_handle.map(|x| x.into_inner()).unwrap(),
                time_cost: Instant::now() - start,
            }),
        }
    }
}

#[pymethods]
impl IrisClientInternal {
    fn add_set_result(&mut self, py: Python<'_>, async_task: AsyncTaskKey, callback: &PyAny) {
        let object: PyObject = callback.into();
        if let Some(task) = self.async_tasks.remove(&async_task.uuid) {
            let t = task.inner.then(|x| async move {
                let result = x.unwrap();
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pycell = PyCell::new(py, result).unwrap();
                let args = PyTuple::new(py, &[pycell]);
                object.call1(py, args).unwrap();
            });
            self.runtime_handle.spawn(t);
        }
    }

    fn batch_wait<'p>(
        &mut self,
        py: Python<'p>,
        tasks: Vec<AsyncTaskKey>,
    ) -> Vec<IrisObjectInternal> {
        let mut t = vec![];
        for key in tasks {
            if let Some(task) = self.async_tasks.remove(&key.uuid) {
                t.push(task);
            }
        }
        py.allow_threads(|| {
            self.runtime_handle.block_on(
                join_all(t.drain(0..).map(|x| x.inner))
                    .map(|mut x| x.drain(0..).map(|i| i.unwrap()).collect()),
            )
        })
    }

    fn apply(
        &mut self,
        py: Python<'_>,
        func: Vec<u8>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        recursive: Option<bool>,
        pickle: &PyAny,
    ) -> IrisObjectInternal {
        let arg = new_arg_request(b_args, b_kwargs, recursive, py, &pickle.to_object(py));
        let request = ApplyRequest { arg, func };
        py.allow_threads(|| self._apply(request))
    }

    fn get_parameter(&mut self, py: Python<'_>, object_id: u64) -> IrisObjectInternal {
        let request = GetParameterRequest { object_id };
        py.allow_threads(|| self._get_parameter(request))
    }

    fn get_parameter_async(&mut self, py: Python<'_>, object_id: u64) -> AsyncTaskKey {
        let request = GetParameterRequest { object_id };
        let task = AsyncIrisObjectTask {
            inner: self._get_parameter_async(request),
        };
        let key = uuid::Uuid::new_v4();
        self.async_tasks.insert(key, task);
        AsyncTaskKey { uuid: key }
    }

    fn create_object(
        &mut self,
        py: Python<'_>,
        module: &str,
        qualname: &str,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        recursive: Option<bool>,
        pickle: &PyAny,
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive, py, &pickle.to_object(py));
        let request = CreateRequest {
            module: module.to_owned(),
            qualname: qualname.to_owned(),
            arg: arg,
        };
        Ok(py.allow_threads(|| self._create_object(request)))
    }

    fn init(
        &mut self,
        py: Python<'_>,
        modules: Vec<String>,
        path: Vec<String>,
        rank: u32,
    ) -> PyResult<()> {
        let request = InitRequest {
            modules,
            paths: path,
            rank,
        };
        py.allow_threads(|| {
            self._init(request);
        });
        Ok(())
    }

    fn torch_call(
        &mut self,
        py: Python<'_>,
        target_node: &str,
        object_id: u64,
        attr: Option<String>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        recursive: Option<bool>,
        torch_func: Option<&str>,
        to_here: bool,
        pickle: &PyAny,
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive, py, &pickle.to_object(py));
        let request = TorchRpcCallRequest {
            target_node: target_node.to_owned(),
            object_id,
            attr: attr.unwrap_or_default(),
            arg,
            torch_func: torch_func.unwrap_or_default().to_owned(),
            to_here,
        };
        Ok(py.allow_threads(|| self._torch_call(request)))
    }

    fn torch_call_async(
        &mut self,
        py: Python<'_>,
        target_node: &str,
        object_id: u64,
        attr: Option<String>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        recursive: Option<bool>,
        torch_func: Option<&str>,
        to_here: bool,
        pickle: &PyAny,
    ) -> AsyncTaskKey {
        let arg = new_arg_request(b_args, b_kwargs, recursive, py, &pickle.to_object(py));
        let request = TorchRpcCallRequest {
            target_node: target_node.to_owned(),
            object_id,
            attr: attr.unwrap_or_default(),
            arg,
            torch_func: torch_func.unwrap_or_default().to_owned(),
            to_here,
        };
        let task = AsyncIrisObjectTask {
            inner: self._torch_call_async(request),
        };
        let key = uuid::Uuid::new_v4();
        self.async_tasks.insert(key, task);
        AsyncTaskKey { uuid: key }
    }
}

#[pymethods]
impl WordCounter {
    #[new]
    fn new(path: String) -> Self {
        WordCounter {
            path: PathBuf::from(path),
        }
    }

    /// Searches for the word, parallelized by rayon
    fn search(&self, py: Python<'_>, search: String) -> PyResult<usize> {
        let contents = fs::read_to_string(&self.path)?;

        let count = py.allow_threads(move || {
            contents
                .par_lines()
                .map(|line| count_line(line, &search))
                .sum()
        });
        Ok(count)
    }

    /// Searches for a word in a classic sequential fashion
    fn search_sequential(&self, needle: String) -> PyResult<usize> {
        let contents = fs::read_to_string(&self.path)?;

        let result = contents.lines().map(|line| count_line(line, &needle)).sum();

        Ok(result)
    }
}

fn matches(word: &str, needle: &str) -> bool {
    let mut needle = needle.chars();
    for ch in word.chars().skip_while(|ch| !ch.is_alphabetic()) {
        match needle.next() {
            None => {
                return !ch.is_alphabetic();
            }
            Some(expect) => {
                if ch.to_lowercase().next() != Some(expect) {
                    return false;
                }
            }
        }
    }
    return needle.next().is_none();
}

/// Count the occurences of needle in line, case insensitive
#[pyfunction]
fn count_line(line: &str, needle: &str) -> usize {
    let mut total = 0;
    for word in line.split(' ') {
        if matches(word, needle) {
            total += 1;
        }
    }
    total
}

#[pymodule]
fn client(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(count_line))?;
    // m.add_wrapped(wrap_pyfunction!(create_iris_client))?;
    m.add_class::<WordCounter>()?;
    m.add_class::<IrisContextInternal>()?;
    m.add_class::<IrisClientInternal>()?;
    m.add_class::<IrisObjectInternal>()?;
    m.add_class::<common::IrisObjectId>()?;
    // m.add_class::<AsyncTest>()?;

    Ok(())
}

fn new_arg_request(
    b_args: Option<&PyTuple>,
    b_kwargs: Option<&PyDict>,
    recursive: Option<bool>,
    py: Python<'_>,
    pickle: &PyObject,
) -> Option<CallArgs> {
    match (b_args, b_kwargs) {
        (None, None) => None,
        (Some(args), None) => Some(CallArgs {
            args: Some(tuple_to_proto(&args, py, pickle)),
            kwargs: None,
            recursive: recursive.unwrap_or_default(),
        }),
        (None, Some(kwargs)) => Some(CallArgs {
            args: None,
            kwargs: Some(dict_to_proto(&kwargs, py, pickle)),
            recursive: recursive.unwrap_or_default(),
        }),
        (Some(args), Some(kwargs)) => Some(CallArgs {
            args: Some(tuple_to_proto(&args, py, pickle)),
            kwargs: Some(dict_to_proto(&kwargs, py, pickle)),
            recursive: recursive.unwrap_or_default(),
        }),
    }
}

fn tuple_to_proto(arg: &PyTuple, py: Python<'_>, pickle: &PyObject) -> ProtoPyTuple {
    let mut vec = vec![];
    for a in arg.iter() {
        if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Boolean(x));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat,_>(a) {
            vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt,_>(a) {
            vec.push(proto_py_any::Data::I64(a.extract().unwrap()));
        } else if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Str(x));
        } else if let Ok(x) = a.extract::<common::IrisObjectId>() {
            vec.push(proto_py_any::Data::ObjectId(x.id));
        } else if let Ok(list) = a.cast_as() {
            vec.push(proto_py_any::Data::List(list_to_proto(list, py, pickle)))
        } else if let Ok(tuple) = a.cast_as() {
            vec.push(proto_py_any::Data::Tuple(tuple_to_proto(tuple, py, pickle)));
        } else if let Ok(dict) = a.cast_as() {
            vec.push(proto_py_any::Data::Dict(dict_to_proto(dict, py, pickle)));
        } else {
            let bytes = serialize(pickle, py, a).unwrap();
            vec.push(proto_py_any::Data::Bytes(bytes));
        }
    }
    ProtoPyTuple {
        items: vec
            .drain(0..)
            .map(|x| ProtoPyAny { data: Some(x) })
            .collect(),
    }
}

fn list_to_proto(arg: &PyList, py: Python<'_>, pickle: &PyObject) -> ProtoPyList {
    let mut vec = vec![];
    for a in arg.iter() {
        if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Boolean(x));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat,_>(a) {
            vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt,_>(a) {
            vec.push(proto_py_any::Data::I64(a.extract().unwrap()));
        } else if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Str(x));
        } else if let Ok(x) = a.extract::<common::IrisObjectId>() {
            vec.push(proto_py_any::Data::ObjectId(x.id));
        } else if let Ok(list) = a.cast_as() {
            vec.push(proto_py_any::Data::List(list_to_proto(list, py, pickle)));
        } else if let Ok(tuple) = a.cast_as() {
            vec.push(proto_py_any::Data::Tuple(tuple_to_proto(tuple, py, pickle)));
        } else if let Ok(dict) = a.cast_as() {
            vec.push(proto_py_any::Data::Dict(dict_to_proto(dict, py, pickle)));
        } else {
            let bytes = serialize(pickle, py, a).unwrap();
            vec.push(proto_py_any::Data::Bytes(bytes));
        }
    }
    ProtoPyList {
        items: vec
            .drain(0..)
            .map(|x| ProtoPyAny { data: Some(x) })
            .collect(),
    }
}

fn dict_to_proto(arg: &PyDict, py: Python<'_>, pickle: &PyObject) -> ProtoPyDict {
    let mut vec = HashMap::new();
    for (key, a) in arg.iter() {
        let key: String = key.extract().unwrap();
        if let Ok(x) = a.extract() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Boolean(x)),
                },
            );
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat,_>(a) {
            let x = a.extract().unwrap();
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::F32(x)),
                },
            );
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt,_>(a) {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::I64(a.extract().unwrap())),
                },
            );
        } else if let Ok(x) = a.extract() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Str(x)),
                },
            );
        } else if let Ok(x) = a.extract::<common::IrisObjectId>() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::ObjectId(x.id)),
                },
            );
        } else if let Ok(list) = a.cast_as() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::List(list_to_proto(list, py, pickle))),
                },
            );
        } else if let Ok(tuple) = a.cast_as() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Tuple(tuple_to_proto(tuple, py, pickle))),
                },
            );
        } else if let Ok(dict) = a.cast_as() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Dict(dict_to_proto(dict, py, pickle))),
                },
            );
        } else {
            let bytes = serialize(pickle, py, a).unwrap();
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Bytes(bytes)),
                },
            );
        }
    }
    ProtoPyDict { map: vec }
}

fn serialize<T>(pickle: &PyObject, py: Python<'_>, err: T) -> PyResult<Vec<u8>>
where
    T: IntoPy<PyObject>,
{
    let result = pickle.call_method1(py, "dumps", (err,))?;
    let bytes: &PyBytes = result.cast_as(py)?;
    let bytes = bytes.as_bytes().to_vec();
    Ok(bytes)
}