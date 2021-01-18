// #![feature(async_closure)]
use common::IrisObjectId;
use futures::future::join_all;
use futures::prelude::*;
use hello_world::greeter_client::GreeterClient;
use hello_world::*;
use pyo3::prelude::*;

use pyo3::types::{PyBytes, PyList};
use pyo3::types::{PyDict, PyTuple};
use pyo3::wrap_pyfunction;
use rayon::prelude::*;
use std::collections::HashMap;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::prelude::*;


use tokio::task::JoinHandle;
use tonic;


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
    pub r#async: bool
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

    // fn _del(&self, request: NodeObjectRef) {
    //     let mut client = self.client.clone();
    //     self.runtime_handle.spawn(async move {
    //         client
    //             .del_object(tonic::Request::new(request))
    //             .await
    //             .unwrap();
    //     });
    // }
}

impl Drop for GuardedIrisObject {
    fn drop(&mut self) {
        // if !self.r#async {
        //     let request = NodeObjectRef {
        //         id: self.node_ref.id,
        //         attr: vec![],
        //         location: Default::default(),
        //     };
        //     // println!("drop object {}, type {}", self.node_ref.id, self.node_ref.r#type);
        //     self._del(request)
        // }
        tracing::debug!("should send del request {} at {}", self.node_ref.id, self.node_ref.location);
        // let request = NodeObjectRef {
        //     id: self.node_ref.id,
        //     attr: vec![],
        //     location: Default::default(),
        // };
        // // println!("drop object {}, type {}", self.node_ref.id, self.node_ref.r#type);
        // self._del(request)
    }
}

#[pymethods]
impl IrisObjectInternal {
    fn log(&self, _py: Python<'_>, source_file: &str, lineno: i32, msg: &str) {
        tracing::debug!("{}:{} [{}:{}] {}", self.inner.node_ref.id, self.inner.node_ref.location ,source_file, lineno, msg);
    }

    fn get_value<'p>(&self, py: Python<'p>, attrs: Vec<String>) -> Option<&'p PyBytes> {
        let request = NodeObjectRef {
            id: self.inner.node_ref.id,
            location: Default::default(),
            attr: attrs,
        };
        let data = py.allow_threads(|| self._get_value(request));
        return data.map(|x| PyBytes::new(py, x.as_ref()));
    }

    fn call(
        &self,
        py: Python<'_>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        attr: Option<Vec<String>>,
        pickle: &PyAny,
        go_async: bool,
        after_list: Option<&PyList>
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.inner.node_ref.location.as_str(),
        );

        let options = Some(RequestOption {
            r#async:go_async,
            after: list_to_after_list(after_list, py)?
        });

        py.allow_threads(|| self._call(arg, attr, options))
    }

    fn get_attr(&self, py: Python<'_>, attr: Vec<String>, go_async: bool, after_list: Option<&PyList>) -> PyResult<IrisObjectInternal> {
        let request = GetAttrRequest {
            attr,
            object_id: self.inner.node_ref.id,
            options: Some(RequestOption {
                r#async:go_async,
                after: list_to_after_list(after_list, py)?
            })
        };
        Ok(py.allow_threads(|| self._get_attr(request)))
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

    fn del_obj(&self, py: Python<'_>,go_async: bool,after_list: Option<&PyList>) -> PyResult<()> {
        if Arc::strong_count(&self.inner) == 1 {
            let request = DelRequest {
                object_id: self.inner.id().id,
                options: Some(RequestOption {
                    r#async: go_async,
                    after: list_to_after_list(after_list, py)?,
                })
            };
            py.allow_threads(|| self._del(request));
        }

        Ok(())
    } 

    fn clone(&self) -> Self {
        IrisObjectInternal {
            inner: self.inner.clone()
        }
    }
}

impl IrisObjectInternal {
    fn _del(&self, request: DelRequest) {
        tracing::debug!("send del request {} at {}", self.inner.id().id, self.inner.node_ref.location);
        let mut client = self.inner.client.clone();
        self.inner.runtime_handle.spawn(async move {
            client
                .del_object(tonic::Request::new(request))
                .await
                .unwrap();
        });
    }

    fn _get_value(&self, request: NodeObjectRef) -> Option<Vec<u8>> {
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
        &self,
        arg: Option<CallArgs>,
        attr: Option<Vec<String>>,
        options: Option<RequestOption>
    ) -> PyResult<IrisObjectInternal> {
        let start = Instant::now();
        let r#async = options.as_ref().map(|x|x.r#async).unwrap_or(false);
        let mut client = self.inner.client.clone();
        let task_handle = self
            .inner
            .runtime_handle
            .block_on(client.call(tonic::Request::new(CallRequest {
                object_id: self.inner.node_ref.id,
                arg,
                attr: attr.unwrap_or_default(),
                options
            })));
        let node_ref = task_handle.unwrap().into_inner();
        Ok(IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.inner.runtime_handle.clone(),
                client,
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.inner.mem_ref.clone(),
                r#async
            }),
        })
    }

    fn _get_attr(&self, request: GetAttrRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let a = request.options.as_ref().map(|x|x.r#async).unwrap_or(false);
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
                r#async: a
            }),
        }
    }

    fn _sync(&self) {
        let start = Instant::now();
        let mut client = self.inner.client.clone();
        let task_handle = self
            .inner
            .runtime_handle
            .block_on(client.sync_object(tonic::Request::new(SyncRequest {
                id: self.id().id
            })));
    }
}

impl IrisClientInternal {
    fn _create_object(&self, request: CreateRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let a = request.options.as_ref().map(|x|x.r#async).unwrap_or(false);
        let task_handle = self
            .runtime_handle
            .block_on(self.client.clone().create_object(tonic::Request::new(request)));
        let node_ref = task_handle.unwrap().into_inner();
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
                r#async: a
            }),
        }
    }

    fn _init(&self, request: InitRequest) {
        self.runtime_handle
            .block_on(self.client.clone().init(tonic::Request::new(request)))
            .unwrap();
    }

    // fn _torch_call_async(
    //     &self,
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

    fn _apply(&self, request: ApplyRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let a = request.options.as_ref().map(|x|x.r#async).unwrap_or(false);
        let task_handle = self
            .runtime_handle
            .block_on(self.client.clone().apply(tonic::Request::new(request)));
        let node_ref = task_handle.unwrap().into_inner();
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
                r#async: a
            }),
        }
    }

    fn _send(&self, request: SendRequest) -> IrisObjectInternal {
        let start = Instant::now();
        let a = request.options.as_ref().map(|x|x.r#async).unwrap_or(false);
        let task_handle = self
            .runtime_handle
            .block_on(self.client.clone().send(tonic::Request::new(request)));
        let node_ref = task_handle.unwrap().into_inner();
        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
                r#async: a
            }),
        }
    }
}

#[pymethods]
impl IrisClientInternal {
    // fn add_set_result(&self, _py: Python<'_>, async_task: AsyncTaskKey, callback: &PyAny) {
    //     let object: PyObject = callback.into();
    //     if let Some(task) = self.async_tasks.remove(&async_task.uuid) {
    //         let t = task.inner.then(|x| async move {
    //             let result = x.unwrap();
    //             let gil = Python::acquire_gil();
    //             let py = gil.python();
    //             let pycell = PyCell::new(py, result).unwrap();
    //             let args = PyTuple::new(py, &[pycell]);
    //             object.call1(py, args).unwrap();
    //         });
    //         self.runtime_handle.spawn(t);
    //     }
    // }

    // fn batch_wait<'p>(
    //     &self,
    //     py: Python<'p>,
    //     tasks: Vec<AsyncTaskKey>,
    // ) -> Vec<IrisObjectInternal> {
    //     let mut t = vec![];
    //     for key in tasks {
    //         if let Some(task) = self.async_tasks.remove(&key.uuid) {
    //             t.push(task);
    //         }
    //     }
    //     py.allow_threads(|| {
    //         self.runtime_handle.block_on(
    //             join_all(t.drain(0..).map(|x| x.inner))
    //                 .map(|mut x| x.drain(0..).map(|i| i.unwrap()).collect()),
    //         )
    //     })
    // }

    fn apply(
        &self,
        py: Python<'_>,
        func: Vec<u8>,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        pickle: &PyAny,
        go_async: bool,
        after_list: Option<&PyList>
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.node.as_str(),
        );
        let options = Some(RequestOption {
            r#async:go_async,
            after: list_to_after_list(after_list, py)?
        });
        let request = ApplyRequest { arg, func, options };
        Ok(py.allow_threads(|| self._apply(request)))
    }

    fn send(
        &self,
        py: Python<'_>,
        func: Vec<u8>,
        go_async: bool,
        after_list: Option<&PyList>
    ) -> PyResult<IrisObjectInternal> {
        let options = Some(RequestOption {
            r#async:go_async,
            after: list_to_after_list(after_list, py)?
        });
        let request = SendRequest { func, options };
        Ok(py.allow_threads(|| self._send(request)))
    }

    fn get_remote_object(
        &self,
        py: Python<'_>,
        obj: &IrisObjectInternal,
        attr: Option<Vec<String>>,
        go_async: bool,
        after_list: Option<&PyList>
    ) -> PyResult<IrisObjectInternal> {
        let start = std::time::Instant::now();
        let id = obj.inner.id();
        let options = Some(RequestOption {
            r#async:go_async,
            after: list_to_after_list(after_list, py)?
        });
        let object = NodeObjectRef {
            id: id.id,
            location: id.location,
            attr: attr.unwrap_or_default(),
        };
        let request = GetRemoteObjectRequest {
            object: Some(object),
            options
        };
        let node_ref = py
            .allow_threads(|| {
                self.runtime_handle
                    .block_on(self.client.clone().get_remote_object(tonic::Request::new(request)))
            })
            .unwrap()
            .into_inner();
        Ok(IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client: self.client.clone(),
                node_ref,
                time_cost: Instant::now() - start,
                mem_ref: self.mem.clone(),
                r#async: false
            }),
        })
    }

    fn create_object(
        &self,
        py: Python<'_>,
        module: &str,
        qualname: &str,
        b_args: Option<&PyTuple>,
        b_kwargs: Option<&PyDict>,
        pickle: &PyAny,
        go_async: bool,
        after_list: Option<&PyList>
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(
            b_args,
            b_kwargs,
            py,
            &pickle.to_object(py),
            self.node.as_str(),
        );
        let options = Some(RequestOption {
            r#async:go_async,
            after: list_to_after_list(after_list, py)?
        });
        let request = CreateRequest {
            module: module.to_owned(),
            qualname: qualname.to_owned(),
            arg: arg,
            options
        };
        Ok(py.allow_threads(|| self._create_object(request)))
    }

    fn init(
        &self,
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

    fn close(&self, py: Python<'_>) {
        py.allow_threads(||{
            let task_handle = self
            .runtime_handle
            .block_on(self.client.clone().close(tonic::Request::new(Null {})));
        })
    }

    // fn torch_call_async(
    //     &self,
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

    fn connect_nodes(&self, py: Python<'_>, mut nodes: HashMap<String, String>) {
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
            let (tuple, f, l) = tuple_to_proto(&args, py, pickle, current_location);
            Some(CallArgs {
                args: Some(tuple),
                kwargs: None,
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f,
                local_lists: l
            })
        }
        (None, Some(kwargs)) => {
            let (dict, f, l) = dict_to_proto(&kwargs, py, pickle, current_location);
            Some(CallArgs {
                args: None,
                kwargs: Some(dict),
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f,
                local_lists: l
            })
        }
        (Some(args), Some(kwargs)) => {
            let (dict, mut f, mut l) = dict_to_proto(&kwargs, py, pickle, current_location);
            let (tuple, mut f2, mut l2) = tuple_to_proto(&args, py, pickle, current_location);
            f2.append(&mut f);
            l2.append(&mut l);
            Some(CallArgs {
                args: Some(tuple),
                kwargs: Some(dict),
                // recursive: recursive.unwrap_or_default(),
                fetch_lists: f2,
                local_lists: l2
            })
        }
    }
}

fn tuple_to_proto(
    arg: &PyTuple,
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> (ProtoPyTuple, Vec<NodeObjectRef>, Vec<NodeObjectRef>) {
    let mut vec = vec![];
    let mut fetch_list = vec![];
    let mut local_list = vec![];
    for a in arg.iter() {
        if let Ok(x) = a.extract() {
            vec.push(proto_py_any::Data::Boolean(x));
        } else if let Ok(true) = a.is_instance::<pyo3::types::PyFloat>() {
            vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
        } else if let Ok(true) = a.is_instance::<pyo3::types::PyInt>() {
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
            else {
                local_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr.clone(),
                })
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
            let (tuple, mut f, mut l) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
            vec.push(proto_py_any::Data::Tuple(tuple));
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f, mut l) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
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
        local_list
    )
}

fn list_to_after_list(
    arg: Option<&PyList>,
    py: Python<'_>,
) -> PyResult<Vec<NodeObjectRef>> {
    let arg = if let Some(x) = arg {
        x
    } else {
        return Ok(vec![]);
    };
    let mut list = vec![];
    for a in arg {
        let x = a.extract::<common::IrisObjectId>()?;
        list.push(NodeObjectRef {
            id: x.id,
            location: x.location.clone(),
            attr: vec![]
        });
    }
    Ok(list)
}

fn list_to_proto(
    arg: &PyList,
    py: Python<'_>,
    pickle: &PyObject,
    current_location: &str,
) -> ProtoPyList {
    let mut vec = vec![];
    let mut fetch_list = vec![];
    let mut local_list = vec![];
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
            } else {
                local_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr.clone(),
                })
            }
        } else if let Ok(list) = a.cast_as() {
            vec.push(proto_py_any::Data::List(list_to_proto(
                list,
                py,
                pickle,
                current_location,
            )));
        } else if let Ok(tuple) = a.cast_as() {
            let (tuple, mut f, mut l) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
            vec.push(proto_py_any::Data::Tuple(tuple));
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f, mut l) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
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
) -> (ProtoPyDict, Vec<NodeObjectRef>, Vec<NodeObjectRef>) {
    let mut vec = HashMap::new();
    let mut fetch_list = vec![];
    let mut local_list = vec![];
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
            } else {
                local_list.push(NodeObjectRef {
                    id: x.id,
                    location: x.location.clone(),
                    attr: x.attr.clone(),
                })
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
            let (tuple, mut f, mut l) = tuple_to_proto(tuple, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
            vec.insert(
                key,
                ProtoPyAny {
                    data: Some(proto_py_any::Data::Tuple(tuple)),
                },
            );
        } else if let Ok(dict) = a.cast_as() {
            let (dict, mut f, mut l) = dict_to_proto(dict, py, pickle, current_location);
            fetch_list.append(&mut f);
            local_list.append(&mut l);
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
    (ProtoPyDict { map: vec }, fetch_list, local_list)
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
                        r#async: src.inner.r#async
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
