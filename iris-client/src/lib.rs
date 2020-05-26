#![feature(async_closure)]
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::PyBytes;
use rayon::prelude::*;
use std::fs;
use std::path::PathBuf;
use tokio::runtime;
use hello_world::greeter_client::GreeterClient;
use hello_world::*;
use std::{collections::HashMap};
use tokio::task::JoinHandle;
use tonic;
use tokio::prelude::*;
use tokio::sync::oneshot;
use futures::prelude::*;
use tokio::net::UnixStream;
use tower::service_fn;
use tonic::transport::{Endpoint, Uri};
use std::convert::TryFrom;
use std::sync::Arc;

type RpcClient = GreeterClient<tonic::transport::channel::Channel>;
type tonicResponseResult<T> = Result<tonic::Response<T>, tonic::Status>;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

pub mod context;
use context::IrisContextInternal;


/// Represents a file that can be searched
#[pyclass(module = "word_count")]
struct WordCounter {
    path: PathBuf,
}

#[pyclass(module = "word_count")]
struct IrisClientInternal {
    pub runtime_handle: tokio::runtime::Handle,
    pub client: RpcClient
}

#[pyclass(module = "word_count")]
struct IrisObjectInternal {
    pub inner: Arc<GuardedIrisObject>
}

struct GuardedIrisObject {
    pub runtime_handle: tokio::runtime::Handle,
    pub client: RpcClient,
    pub node_ref: NodeObject
}

impl GuardedIrisObject {
    fn id(self: &Arc<Self>) -> u64 {
        self.as_ref().node_ref.id
    }

    fn exception<'p>(self: &'p Arc<Self>) -> &'p [u8] {
        self.node_ref.exception.as_ref()
    }

    fn _del(&mut self, request:GetParameterRequest) {
        let mut client = self.client.clone();
        self.runtime_handle.spawn(async move {
            client.del_object(tonic::Request::new(request)).await.unwrap();
        });
    }
}

impl Drop for GuardedIrisObject {
    fn drop(&mut self) {
        let request = GetParameterRequest {
            object_id: self.node_ref.id
        };
        // println!("drop object {}, type {}", self.node_ref.id, self.node_ref.r#type);
        self._del(request)
    }
}

#[pymethods]
impl IrisObjectInternal {
    fn get_value<'p>(&mut self, py:Python<'p>) -> Option<&'p PyBytes> {
        let request = GetParameterRequest {
            object_id: self.inner.id()
        };
        let data = py.allow_threads(||{
            self._get_value(request)
        });
        return data.map(|x|PyBytes::new(py, x.as_ref()))
    }

    fn call(&mut self, py:Python<'_>, b_args: Option<&[u8]>, b_kwargs: Option<&[u8]>, recursive: Option<bool>, attr: Option<String>) -> Option<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive);
        py.allow_threads(|| {
            self._call(arg, attr)
        })
    }

    fn get_attr(&mut self, py:Python<'_>, attr: String) -> Option<IrisObjectInternal> {
        let request = GetAttrRequest {
            attr,
            object_id: self.inner.id()
        };
        Some(py.allow_threads(|| {
            self._get_attr(request)
        }))
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn exception<'p>(&self, py:Python<'p>) -> Option<&'p PyBytes> {
        let bytes = self.inner.exception();
        if bytes.len() == 0 {
            return None;
        }
        else {
            Some(PyBytes::new(py,self.inner.exception()))
        }
    }
}

impl IrisObjectInternal {
    fn _del(&mut self, request: GetParameterRequest) {
        let mut client = self.inner.client.clone();
        self.inner.runtime_handle.spawn(async move {
            client.del_object(tonic::Request::new(request)).await.unwrap();
        });
    }

    fn _get_value(&mut self, request: GetParameterRequest) -> Option<Vec<u8>> {
        let task_handle = self.inner.runtime_handle.block_on(
            self.inner.client.clone().get_value(tonic::Request::new(request)));
        let ret = task_handle.map(|x|x.into_inner()).unwrap();
        Some(ret.data)
    }

    fn _call(&mut self, arg: Option<CallArgs>, attr: Option<String>) -> Option<IrisObjectInternal> {
        let mut client = self.inner.client.clone();
        let task_handle = self.inner.runtime_handle.block_on(client.call(tonic::Request::new(CallRequest {
            object_id: self.id(),
            arg,
            attr: attr.unwrap_or_default()
        })));
        Some(IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.inner.runtime_handle.clone(),
                client,
                node_ref:task_handle.map(|x|x.into_inner()).unwrap()
            })
        })
    }

    fn _get_attr(&mut self, request:GetAttrRequest) -> IrisObjectInternal {
        let mut client = self.inner.client.clone();
        let task_handle = self.inner.runtime_handle.block_on(
            client.get_attr(tonic::Request::new(request)));

            IrisObjectInternal {
                inner: Arc::new(GuardedIrisObject {
                    runtime_handle: self.inner.runtime_handle.clone(),
                    client,
                    node_ref:task_handle.map(|x|x.into_inner()).unwrap()
                })
            }
    }
}

impl IrisClientInternal {
    fn _create_object(&mut self, request: CreateRequest) -> IrisObjectInternal {
        let task_handle = self.runtime_handle.block_on(
            self.client.create_object(tonic::Request::new(request)));

            IrisObjectInternal {
                inner: Arc::new(GuardedIrisObject {
                    runtime_handle: self.runtime_handle.clone(),
                    client:self.client.clone(),
                    node_ref:task_handle.map(|x|x.into_inner()).unwrap()
                })
            }
    }

    fn _init(&mut self, request: InitRequest) {
        self.runtime_handle.block_on(
            self.client.init(tonic::Request::new(request))
        ).unwrap();
    }

    fn _torch_call(&mut self, request: TorchRpcCallRequest) -> IrisObjectInternal {
        let task_handle = self.runtime_handle.block_on(
            self.client.torch_call(tonic::Request::new(request))
        );

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client:self.client.clone(),
                node_ref:task_handle.map(|x|x.into_inner()).unwrap()
            })
        }
    }

    fn _get_parameter(&mut self, request: GetParameterRequest) -> IrisObjectInternal {
        let task_handle = self.runtime_handle.block_on(
            self.client.get_parameter(tonic::Request::new(request))
        );

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client:self.client.clone(),
                node_ref:task_handle.map(|x|x.into_inner()).unwrap()
            })
        }
    }

    fn _apply(&mut self, request: ApplyRequest) -> IrisObjectInternal {
        let task_handle = self.runtime_handle.block_on(
            self.client.apply(tonic::Request::new(request))
        );

        IrisObjectInternal {
            inner: Arc::new(GuardedIrisObject {
                runtime_handle: self.runtime_handle.clone(),
                client:self.client.clone(),
                node_ref:task_handle.map(|x|x.into_inner()).unwrap()
            })
        }
    }
}

#[pymethods]
impl IrisClientInternal {
    fn apply(&mut self, py:Python<'_>, func:Vec<u8>, b_args: Option<&[u8]>, b_kwargs: Option<&[u8]>, recursive:Option<bool>) -> IrisObjectInternal {
        let arg = new_arg_request(b_args, b_kwargs, recursive);
        let request = ApplyRequest {
            arg,
            func
        };
        py.allow_threads(||{
            self._apply(request)
        })
    }

    fn get_parameter(&mut self, py:Python<'_>, object_id:u64) -> IrisObjectInternal {
        let request = GetParameterRequest {
            object_id
        };
        py.allow_threads(||{
            self._get_parameter(request)
        })
    }

    fn create_object(&mut self, py:Python<'_>,module: &str, qualname: &str, b_args: Option<&[u8]>, b_kwargs: Option<&[u8]>, recursive: Option<bool>) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive);
        let request = CreateRequest {
            module: module.to_owned(),
            qualname: qualname.to_owned(),
            arg: arg
        };
        Ok(py.allow_threads(||self._create_object(request)))
    }

    fn init(&mut self, py:Python<'_>, modules: Vec<String>, path: Vec<String>, rank: u32) -> PyResult<()> {
        let request = InitRequest {
            modules,
            paths: path,
            rank
        };
        py.allow_threads(||{
            self._init(request);
        });
        Ok(())
    }

    fn torch_call(
        &mut self, py:Python<'_>, 
        target_node: &str, 
        object_id: u64, 
        attr: Option<String>, 
        b_args: Option<&[u8]>, 
        b_kwargs: Option<&[u8]>, 
        recursive: Option<bool>,
        torch_func: Option<&str>,
        to_here: bool
    ) -> PyResult<IrisObjectInternal> {
        let arg = new_arg_request(b_args, b_kwargs, recursive);
        let request = TorchRpcCallRequest {
            target_node: target_node.to_owned(),
            object_id,
            attr: attr.unwrap_or_default(),
            arg,
            torch_func: torch_func.unwrap_or_default().to_owned(),
            to_here
        };
        Ok(py.allow_threads(||self._torch_call(request)))
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
fn word_count(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(count_line))?;
    // m.add_wrapped(wrap_pyfunction!(create_iris_client))?;
    m.add_class::<WordCounter>()?;
    m.add_class::<IrisContextInternal>()?;
    m.add_class::<IrisClientInternal>()?;
    m.add_class::<IrisObjectInternal>()?;

    Ok(())
}

fn new_arg_request(b_args: Option<&[u8]>, b_kwargs: Option<&[u8]>, recursive: Option<bool>) -> Option<CallArgs> {
        match (b_args, b_kwargs) {
            (None, None) => { None }
            (Some(args), None) => {
                Some(CallArgs {
                    args: Vec::from(args),
                    kwargs: vec![],
                    recursive: recursive.unwrap_or_default()
                })
            }
            (None, Some(kwargs)) => {
                Some(CallArgs {
                    args: vec![],
                    kwargs: Vec::from(kwargs),
                    recursive: recursive.unwrap_or_default()
                })
            }
            (Some(args), Some(kwargs)) => {
                Some(CallArgs {
                    args: Vec::from(args),
                    kwargs: Vec::from(kwargs),
                    recursive: recursive.unwrap_or_default()
                })
            }
        }
}