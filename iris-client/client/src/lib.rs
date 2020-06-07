#![feature(async_closure)]
use common::IrisObjectId;
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

#[derive(Clone)]
struct ClientMem {}

// impl ClientMem {
//     pub fn insert_fetch_list(&self, fetch_list:)
// }

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
    pub node: String,
    pub mem: ClientMem,
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
    pub mem_ref: ClientMem,
}

impl GuardedIrisObject {
    fn id(self: &Arc<Self>) -> IrisObjectId {
        IrisObjectId {
            id: self.node_ref.id,
            location: self.node_ref.location.clone(),
            attr: vec![],
        }
    }

    fn time_cost_as_sec(self: &Arc<Self>) -> f64 {
        self.as_ref().time_cost.as_secs_f64()
    }

    fn exception<'p>(self: &'p Arc<Self>) -> &'p [u8] {
        self.node_ref.exception.as_ref()
    }

    fn _del(&mut self, request: NodeObjectRef) {
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
        let request = NodeObjectRef {
            id: self.node_ref.id,
            attr: vec![],
            location: Default::default(),
        };
        // println!("drop object {}, type {}", self.node_ref.id, self.node_ref.r#type);
        self._del(request)
    }
}

#[pymethods]
impl IrisObjectInternal {
    fn get_value<'p>(&mut self, py: Python<'p>, attrs: Vec<String>) -> Option<&'p PyBytes> {
        let request = NodeObjectRef {
            id: self.inner.node_ref.id,
            location: Default::default(),
            attr: attrs,
        };
        let data = py.allow_threads(|| self._get_value(request));
        return data.map(|x| PyBytes::new(py, x.as_ref()));
    }

    fn call(
        &mut self,
        py: Python<'_>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        attr: Option<Vec<String>>,
        pickle: &PyAny,
    ) -> Option<IrisObjectInternal> {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.inner.node_ref.location.as_str(),
        );

        py.allow_threads(|| self._call(arg, attr))
    }

    fn get_attr(&mut self, py: Python<'_>, attr: Vec<String>) -> Option<IrisObjectInternal> {
        let request = GetAttrRequest {
            attr,
            object_id: self.inner.node_ref.id,
        };
        Some(py.allow_threads(|| self._get_attr(request)))
    }

    fn id(&self) -> IrisObjectId {
        self.inner.id()
    }

    fn get_native_value(&self, py: Python<'_>) -> PyObject {
        match &self.inner.node_ref.value {
            Some(ProtoPyAny {
                data: Some(proto_py_any::Data::Boolean(x)),
            }) => x.to_object(py),
            Some(ProtoPyAny {
                data: Some(proto_py_any::Data::I64(x)),
            }) => x.to_object(py),
            Some(ProtoPyAny {
                data: Some(proto_py_any::Data::F32(x)),
            }) => x.to_object(py),
            Some(ProtoPyAny {
                data: Some(proto_py_any::Data::Str(x)),
            }) => x.to_object(py),
            Some(ProtoPyAny {
                data: Some(proto_py_any::Data::Tuple(tuple)),
            }) => tuple_to_obj(py, tuple, self).unwrap(),
            _ => py.None(),
            None => py.None(),
        }
    }

    fn get_type(&self) -> String {
        self.inner.node_ref.r#type.to_string()
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
    fn _del(&mut self, request: NodeObjectRef) {
        let mut client = self.inner.client.clone();
        self.inner.runtime_handle.spawn(async move {
            client
                .del_object(tonic::Request::new(request))
                .await
                .unwrap();
        });
    }

    fn _get_value(&mut self, request: NodeObjectRef) -> Option<Vec<u8>> {
        let task_handle = self.inner.runtime_handle.block_on(
            self.inner
                .client
                .clone()
                .get_value(tonic::Request::new(request)),
        );
        let ret = task_handle.map(|x| x.into_inner()).unwrap();
        Some(ret.data)
    }

    fn _call(
        &mut self,
        arg: Option<CallArgs>,
        attr: Option<Vec<String>>,
    ) -> Option<IrisObjectInternal> {
        let start = Instant::now();
        let mut client = self.inner.client.clone();
        let task_handle = self
            .inner
            .runtime_handle
            .block_on(client.call(tonic::Request::new(CallRequest {
                object_id: self.inner.node_ref.id,
                arg,
                attr: attr.unwrap_or_default(),
            })));
        let result = task_handle.unwrap().into_inner();
        let node_ref = result.obj.unwrap();
        let fetch_list = result.fetch_result;
        Some(IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.inner.runtime_handle.clone(),
                client,
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.inner.mem_ref.clone(),
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
                mem_ref: self.inner.mem_ref.clone(),
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
        let result = task_handle.unwrap().into_inner();
        let node_ref = result.obj.unwrap();
        let fetch_list = result.fetch_result;
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
            }),
        }
    }

    fn _init(&mut self, request: InitRequest) {
        self.runtime_handle
            .block_on(self.client.init(tonic::Request::new(request)))
            .unwrap();
    }

    // fn _torch_call_async(
    //     &mut self,
    //     request: TorchRpcCallRequest,
    // ) -> JoinHandle<IrisObjectInternal> {
    //     let mut client = self.client.clone();
    //     let runtime_handle = self.runtime_handle.clone();
    //     unimplemented!()
    //     // let task_handle = self.runtime_handle.spawn(async move {
    //     //     let start = Instant::now();
    //     //     let result = client.torch_call(tonic::Request::new(request)).await;
    //     //     IrisObjectInternal {
    //     //         inner: Arc::new(GuardedIrisObject {
    //     //             client,
    //     //             runtime_handle,
    //     //             node_ref: result.map(|x| x.into_inner()).unwrap(),
    //     //             time_cost: Instant::now() - start,
    //     //         }),
    //     //     }
    //     // });
    //     // task_handle
    // }

    fn _apply(&mut self, request: ApplyRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let task_handle = self
            .runtime_handle
            .block_on(self.client.apply(tonic::Request::new(request)));
        let result = task_handle.unwrap().into_inner();
        let node_ref = result.obj.unwrap();
        let fetch_list = result.fetch_result;
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
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
        pickle: &PyAny,
    ) -> IrisObjectInternal {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.node.as_str(),
        );
        let request = ApplyRequest { arg, func };
        py.allow_threads(|| self._apply(request))
    }

    fn get_remote_object(
        &mut self,
        py: Python<'_>,
        obj: &IrisObjectInternal,
        attr: Option<Vec<String>>,
    ) -> IrisObjectInternal {
        let start = std::time::Instant::now();
        let id = obj.inner.id();
        let request = NodeObjectRef {
            id: id.id,
            location: id.location,
            attr: attr.unwrap_or_default(),
        };
        let node_ref = py
            .allow_threads(|| {
                self.runtime_handle
                    .block_on(self.client.get_remote_object(tonic::Request::new(request)))
            })
            .unwrap()
            .into_inner();
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
            }),
        }
    }

    fn create_object(
        &mut self,
        py: Python<'_>,
        module: &str,
        qualname: &str,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        pickle: &PyAny,
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.node.as_str(),
        );
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

    // fn torch_call_async(
    //     &mut self,
    //     py: Python<'_>,
    //     target_node: &str,
    //     object_id: u64,
    //     attr: Option<String>,
    //     b_args: Option<&PyTuple>,
    //     b_kwargs: Option<&PyDict>,
    //     torch_func: Option<&str>,
    //     to_here: bool,
    //     pickle: &PyAny,
    // ) -> AsyncTaskKey {
    //     let arg = new_arg_request(
    //         b_args,
    //         b_kwargs,
    //         py,
    //         &pickle.to_object(py),
    //         self.node.as_str(),
    //     );
    //     let request = TorchRpcCallRequest {
    //         target_node: target_node.to_owned(),
    //         object_id,
    //         attr: attr.unwrap_or_default(),
    //         arg,
    //         torch_func: torch_func.unwrap_or_default().to_owned(),
    //         to_here,
    //     };
    //     let task = AsyncIrisObjectTask {
    //         inner: self._torch_call_async(request),
    //     };
    //     let key = uuid::Uuid::new_v4();
    //     self.async_tasks.insert(key, task);
    //     AsyncTaskKey { uuid: key }
    // }

    fn connect_nodes(&mut self, py: Python<'_>, mut nodes: HashMap<String, String>) {
        let request = ConnectRequest {
            nodes: nodes
                .drain()
                .map(|(name, address)| Node { name, address })
                .collect(),
        };
        let mut client = self.client.clone();
        py.allow_threads(|| {
            self.runtime_handle
                .block_on(async move { client.connect_nodes(tonic::Request::new(request)).await })
                .unwrap()
        });
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
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> Option<CallArgs> {
    match (b_args, b_kwargs) {
        (None, None) => None,
        (Some(args), None) => {
            let (tuple, f) = tuple_to_proto(&args, py, pickle, current_location);
            Some(CallArgs {
                args: Some(tuple),
                kwargs: None,
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f,
            })
        }
        (None, Some(kwargs)) => {
            let (dict, f) = dict_to_proto(&kwargs, py, pickle, current_location);
            Some(CallArgs {
                args: None,
                kwargs: Some(dict),
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f,
            })
        }
        (Some(args), Some(kwargs)) => {
            let (dict, mut f) = dict_to_proto(&kwargs, py, pickle, current_location);
            let (tuple, mut f2) = tuple_to_proto(&args, py, pickle, current_location);
            f2.append(&mut f);
            Some(CallArgs {
                args: Some(tuple),
                kwargs: Some(dict),
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f2,
            })
        }
    }
}

fn tuple_to_proto(
    arg: &PyTuple,
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> (ProtoPyTuple, Vec<NodeObjectRef>) {
    let mut vec = vec![];
    let mut fetch_list = vec![];
    for a in arg.iter() {
        if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Boolean(x));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat, _>(a) {
            vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt, _>(a) {
            vec.push(proto_py_any::Data::I64(a.extract().unwrap()));
        } else if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Str(x));
        } else if let Ok(x) = a.extract::<common::IrisObjectId>() {
            if x.location != current_location {
                fetch_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr.clone(),
                });
            }
            vec.push(proto_py_any::Data::ObjectId(NodeObjectRef {
                id: x.id,
                location: x.location.clone(),
                attr: x.attr,
            }));
        } else if let Ok(list) = a.cast_as() {
            vec.push(proto_py_any::Data::List(list_to_proto(
                list,
                py,
                pickle,
                current_location,
            )))
        } else if let Ok(tuple) = a.cast_as() {
            let (tuple, mut f) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.push(proto_py_any::Data::Tuple(tuple));
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.push(proto_py_any::Data::Dict(dict));
        } else {
            let bytes = serialize(pickle, py, a).unwrap();
            vec.push(proto_py_any::Data::Bytes(bytes));
        }
    }
    (
        ProtoPyTuple {
            items: vec
                .drain(0..)
                .map(|x| ProtoPyAny { data: Some(x) })
                .collect(),
        },
        fetch_list,
    )
}

fn list_to_proto(
    arg: &PyList,
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> ProtoPyList {
    let mut vec = vec![];
    let mut fetch_list = vec![];
    for a in arg.iter() {
        if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Boolean(x));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat, _>(a) {
            vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt, _>(a) {
            vec.push(proto_py_any::Data::I64(a.extract().unwrap()));
        } else if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Str(x));
        } else if let Ok(x) = a.extract::<common::IrisObjectId>() {
            vec.push(proto_py_any::Data::ObjectId(NodeObjectRef {
                id: x.id,
                location: x.location.clone(),
                attr: x.attr.clone(),
            }));
            if x.location != current_location {
                fetch_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr,
                });
            }
        } else if let Ok(list) = a.cast_as() {
            vec.push(proto_py_any::Data::List(list_to_proto(
                list,
                py,
                pickle,
                current_location,
            )));
        } else if let Ok(tuple) = a.cast_as() {
            let (tuple, mut f) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.push(proto_py_any::Data::Tuple(tuple));
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.push(proto_py_any::Data::Dict(dict));
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

fn dict_to_proto(
    arg: &PyDict,
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> (ProtoPyDict, Vec<NodeObjectRef>) {
    let mut vec = HashMap::new();
    let mut fetch_list = vec![];
    for (key, a) in arg.iter() {
        let key: String = key.extract().unwrap();
        if let Ok(x) = a.extract() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Boolean(x)),
                },
            );
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat, _>(a) {
            let x = a.extract().unwrap();
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::F32(x)),
                },
            );
        } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt, _>(a) {
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
            if x.location != current_location {
                fetch_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr.clone(),
                });
            }
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::ObjectId(NodeObjectRef {
                        id: x.id,
                        location: x.location.clone(),
                        attr: x.attr,
                    })),
                },
            );
        } else if let Ok(list) = a.cast_as() {
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::List(list_to_proto(
                        list,
                        py,
                        pickle,
                        current_location,
                    ))),
                },
            );
        } else if let Ok(tuple) = a.cast_as() {
            let (tuple, mut f) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Tuple(tuple)),
                },
            );
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Dict(dict)),
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
    (ProtoPyDict { map: vec }, fetch_list)
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

fn tuple_to_obj(
    py: Python<'_>,
    tuple: &ProtoPyTuple,
    src: &IrisObjectInternal,
) -> PyResult<PyObject> {
    let mut vec = vec![];
    for t in &tuple.items {
        match &t.data {
            Some(proto_py_any::Data::Boolean(x)) => {
                vec.push(x.to_object(py));
            }
            Some(proto_py_any::Data::F32(x)) => {
                vec.push(x.to_object(py));
            }
            Some(proto_py_any::Data::I64(x)) => {
                vec.push(x.to_object(py));
            }
            Some(proto_py_any::Data::Str(x)) => {
                vec.push(x.to_object(py));
            }
            Some(proto_py_any::Data::ObjectId(id)) => vec.push(
                IrisObjectInternal {
                    inner: Arc::new(GuardedIrisObject {
                        runtime_handle: src.inner.runtime_handle.clone(),
                        client: src.inner.client.clone(),
                        node_ref: NodeObject {
                            id: id.id,
                            ..Default::default()
                        },
                        time_cost: Duration::default(),
                        mem_ref: src.inner.mem_ref.clone(),
                    }),
                }
                .into_py(py),
            ),
            Some(proto_py_any::Data::Tuple(tuple)) => {
                vec.push(tuple_to_obj(py, &tuple, src).unwrap());
            }
            _ => unreachable!(),
        }
    }
    Ok(PyTuple::new(py, vec).to_object(py))
}
