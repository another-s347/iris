use pyo3::prelude::*;
use pyo3::{PyNativeType, types::{PyDict, IntoPyDict, PyList, PyTuple, PyBytes}, AsPyPointer, PyTypeInfo};
use tonic::{transport::Server, Request, Response, Status};
use tokio::net::UnixListener;
use futures::stream::TryStreamExt;
use uuid;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use common::{hello_world, IrisObjectId};

use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    *
};
use std::{sync::{Arc, Mutex}, collections::HashMap, path::Path};
#[derive(Debug, Clone)]
struct IrisServer {
    modules: Arc<Mutex<HashMap<String, Py<PyModule>>>>,
    objects: Arc<Mutex<HashMap<u64, Py<PyAny>>>>,
    pickle: Py<PyModule>,
    torch_rpc: Py<PyModule>
}

fn dbg_py<T>(py: Python<'_>, x: PyResult<T>) -> PyResult<T> {
    if let Err(err) = &x {
        let err = err.clone_ref(py);
        err.print(py);
    }
    x
}


impl IrisServer {
    fn import_modules(&self,py:Python<'_>, modules: Vec<String>, paths: Vec<String>) -> PyResult<()> {
        let sys = py.import("sys")?;
        let path = sys.get("path")?.cast_as::<PyList>()?;

        for p in paths {
            path.append(p.as_str())?;
        }

        let mut state = self.modules.lock().unwrap();
        for module_name in modules {
            println!("import.. {}", module_name);
            let py = py.import(module_name.as_str())?;
            state.insert(module_name, Py::from(py));
        }
        
        Ok(())
    }

    fn serialize<T>(&self, py:Python<'_>, err:T) -> PyResult<Vec<u8>> 
    where T:IntoPy<PyObject>
    {
        let pickle = self.pickle.to_object(py);
        let result = pickle.call_method1(py, "dumps", (err,))?;
        let bytes:&PyBytes = result.cast_as(py)?;
        let bytes = bytes.as_bytes().to_vec();
        Ok(bytes)
    }
    
    fn loads(&self, py:Python<'_>, bytes: &[u8]) -> PyResult<PyObject> 
    // where T:AsPyPointer + PyNativeType + PyTypeInfo
    {
        let pickle = self.pickle.to_object(py);
        let result = pickle.call_method1(py, "loads", (bytes,))?;
        Ok(result)
        // let result:&T = result.cast_as(py)?;
        // Ok(Py::from(result))
    }

    fn map_args_to_local<'a>(&self, py:Python<'a>, args:&'a PyTuple, recursive:bool) -> &'a PyTuple {
        let tuple = args;
        let maps = self.objects.lock().unwrap();

        map_args_to_local_impl(&maps, py,tuple, recursive)
    }

    fn torch_remote<'a>(&'a self, py:Python<'a>, request: TorchRpcCallRequest) -> &'a PyAny {
        let rpc = self.torch_rpc.as_ref(py);
        let args= self.loads(py, request.arg.unwrap().args.as_ref()).unwrap();
        let args:&PyTuple = args.cast_as(py).unwrap();
        let args = args.as_slice();
        let new_args:&[PyObject] = &[
            request.object_id.into_py(py),
            request.attr.into_py(py)
        ];
        let mut obj = rpc.call("remote", PyTuple::new(py, &[
            request.target_node.as_str(), ""
        ]), Some(vec![
            ("args", PyTuple::new(py, [new_args, &args].concat()))
        ].into_py_dict(py))).unwrap();
        if request.to_here {
            obj = obj.call_method0("to_here").unwrap();
        }
        obj
    }

    // fn torch_rref(&self, py:Python<'_>, obj: PyAny) -> &PyAny {
    //     let rpc = self.torch_rpc.as_ref(py);

    // }
}

fn map_args_to_local_impl<'a>(maps:&HashMap<u64, Py<PyAny>>, py:Python<'a>, args:&'a PyTuple, recursive:bool) -> &'a PyTuple {
    let mut tuple_args = vec![];
    for x in args {
        if py.is_instance::<IrisObjectId,_>(x).unwrap_or(false) {
            if let Some(e) = maps.get(&x.extract::<IrisObjectId>().unwrap().id) {
                tuple_args.push(e.as_ref(py))
            }
        }
        else if recursive && py.is_instance::<PyTuple, _>(x).unwrap_or(false) {
            tuple_args.push(map_args_to_local_impl(maps,py,x.cast_as().unwrap(), recursive))
        }
        else {
            tuple_args.push(x);
        }
    }

    PyTuple::new(py, tuple_args.iter())
}

#[tonic::async_trait]
impl Greeter for IrisServer {
    async fn say_hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let request = request.into_inner();
        unimplemented!()
    }

    async fn init(&self, request: Request<InitRequest>) -> Result<Response<NodeObject>, Status> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let request = request.into_inner();
        let modules:Vec<String> = request.modules;
        let paths:Vec<String> = request.paths;
        if let Err(err) = self.import_modules(py, modules, paths) {
            return Ok(Response::new(NodeObject {
                exception: self.serialize(py, err).unwrap(),
                location: "".to_owned(),
                ..Default::default()
            }))
        }
        return Ok(Response::new(NodeObject {
            r#type:"init".to_owned(),
            location:"".to_owned(),
            ..Default::default()
        }));
    }

    async fn call(&self, request: Request<CallRequest>) -> Result<Response<NodeObject>, Status> {
        let request:CallRequest = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();
        let args = self.loads(py, request.arg.unwrap().args.as_ref()).unwrap();
        let args:&PyTuple = args.cast_as(py).unwrap();
        let args = self.map_args_to_local(py, args, false);
        let mut maps = self.objects.lock().unwrap();
        let o = maps.get(&request.object_id).unwrap().clone();
        let mut o = o.as_ref(py);
        if !request.attr.is_empty() {
            o = o.getattr(&request.attr).unwrap();
        }
        if o.is_callable() {
            let ret = o.call(args, None).unwrap();

            let mut hasher = DefaultHasher::new();
            let id = uuid::Uuid::new_v4();
            id.hash(&mut hasher);
            let id = hasher.finish();

            maps.insert(id, Py::from(ret));

            return Ok(Response::new(NodeObject {
                id,
                ..Default::default()
            }))
        }
        unimplemented!()
    }

    async fn create_object(&self, request: Request<CreateRequest>) -> Result<Response<NodeObject>, Status> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let request:CreateRequest = request.into_inner();
        
        let module = self.modules.lock().unwrap();
        if let Some(m) = module.get(&request.module) {
            let m = m.to_object(py);
            let m = m.getattr(py, request.qualname).unwrap();
            let args = self.loads(py, request.arg.unwrap().args.as_ref()).unwrap();
            let args:&PyTuple = args.cast_as(py).unwrap();
            let args = self.map_args_to_local(py, args, false);

            let new_object = m.call(py, args, None).unwrap();
            let new_object = Py::from(new_object.as_ref(py));
            let mut hasher = DefaultHasher::new();
            let id = uuid::Uuid::new_v4();
            id.hash(&mut hasher);
            let id = hasher.finish();
            let mut maps = self.objects.lock().unwrap();
            maps.insert(id, new_object);

            return Ok(Response::new(NodeObject {
                location: "".to_owned(),
                id,
                ..Default::default()
            }))
        }

        unimplemented!()
    }

    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let args = self.loads(py, request.arg.unwrap().args.as_ref()).unwrap();
        let args:&PyTuple = args.cast_as(py).unwrap();
        let args = self.map_args_to_local(py, args, false);

        let func = self.loads(py, request.func.as_ref()).unwrap();

        let func = func.as_ref(py);
        let obj = func.call(args, None).unwrap();

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();
        
        let mut maps = self.objects.lock().unwrap();
        maps.insert(id, Py::from(obj)).unwrap();

        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn torch_call(&self, request: Request<TorchRpcCallRequest>) -> Result<Response<NodeObject>, Status> { 
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let obj = self.torch_remote(py, request);

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();
        
        let mut maps = self.objects.lock().unwrap();
        maps.insert(id, Py::from(obj)).unwrap();

        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn get_attr(&self, request: Request<GetAttrRequest>) -> Result<Response<NodeObject>, Status> { 
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        let mut maps = self.objects.lock().unwrap();
        let obj = maps.get(&request.object_id).unwrap();
        let obj = obj.to_object(py);
        let obj = obj.as_ref(py);
        let obj = obj.getattr( request.attr.as_str()).unwrap();
        maps.insert(id, Py::from(obj)).unwrap();

        return Ok(Response::new(NodeObject {
            id,
            ..Default::default()
        }));
    }

    async fn get_parameter(&self, request: Request<GetParameterRequest>) -> Result<Response<NodeObject>, Status> { 
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut maps = self.objects.lock().unwrap();

        unimplemented!()
    }

    async fn del_object(&self, request: Request<GetParameterRequest>) -> Result<Response<ObjectId>, Status> { 
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut maps = self.objects.lock().unwrap();
        let obj = maps.remove(&request.object_id).unwrap();

        return Ok(Response::new(ObjectId{
            id: request.object_id
        }));
    }

    async fn get_value(&self, request: Request<GetParameterRequest>) -> Result<Response<Value>, Status> { 
        let request = request.into_inner();
        let gil = Python::acquire_gil();
        let py = gil.python();

        let maps = self.objects.lock().unwrap();
        let obj = maps.get(&request.object_id).unwrap();
        
        let data = self.serialize(py, obj.to_object(py)).unwrap();
        return Ok(Response::new(Value {
            data
        }));
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pickle = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let pickle = py.import("dill").unwrap();
        Py::from(pickle)
    };

    let torch_rpc = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let rpc = py.import("torch.distributed.rpc").unwrap();
        Py::from(rpc)
    };

    let path = "/tmp/iris-tmp-node0-0.sock";

    tokio::fs::create_dir_all(Path::new(path).parent().unwrap()).await?;

    let mut uds = UnixListener::bind(path)?;

    let greeter = IrisServer {
        modules: Arc::new(Mutex::new(HashMap::new())),
        objects: Arc::new(Mutex::new(HashMap::new())),
        pickle,
        torch_rpc
    };

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve_with_incoming(uds.incoming().map_ok(unix::UnixStream))
        .await?;

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
