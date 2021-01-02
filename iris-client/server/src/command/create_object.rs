use pyo3::{PyObject, PyResult, Python};
use crate::utils::dbg_py;
use crate::{Opt, distributed, hello_world::{greeter_server::Greeter, *}, mem::LazyPyObject, utils::LocalObject};
use super::{CommandTarget, args::PrepareArgsResult};

pub struct CreateObjectCommand {
    id: u64,
    module: PyObject,
    qualname: String,
    args: Option<PrepareArgsResult>
}

impl super::ControlCommandRequest for CreateRequest {
    fn get_async(&self) -> bool {
        self.r#async
    }

    fn get_args(&self) -> Option<CallArgs> {
        self.arg.clone()
    }

    fn get_target_object(&self) -> CommandTarget {
        CommandTarget::Module(self.module.clone())
    }
}

impl super::ControlCommand for CreateObjectCommand {
    const NAME:&'static str = "CreateObject";
    type Request = CreateRequest;

    fn new(request: Self::Request, args: Option<PrepareArgsResult>, id: u64, object: Option<PyObject>) -> Self {
        Self {
            id,
            module: object.unwrap(),
            qualname: request.qualname.clone(),
            args,
        }
    }

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject> {
        let mut o = self.module;
        let mut o = dbg_py(py, o.getattr(py, &self.qualname).map_err(|x|x.into()))?;

        let ret = match self.args.map(|x|x.to_pyobject(py, pickle)) {
            Some(Ok((args,kwargs))) => {
                o.call(py, args.cast_as(py).map_err(|x| anyhow::anyhow!("cast args error: {:#?}", x))?, Some(kwargs.cast_as(py).map_err(|x| anyhow::anyhow!("cast kwargs error: {:#?}", x))?))?
            }
            Some(Err(err)) => {
                return Err(err.into());
            }
            None => {
                o.call0(py)?
            }
        };

        Ok(ret)
    }
}