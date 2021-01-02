use super::{args::PrepareArgsResult, CommandTarget};
use crate::utils::dbg_py;
use crate::{
    distributed,
    hello_world::{greeter_server::Greeter, *},
    mem::LazyPyObject,
    utils::LocalObject,
    Opt,
};
use anyhow::Context;
use pyo3::{PyObject, PyResult, Python};

pub struct CallCommand {
    id: u64,
    object: PyObject,
    attr: Vec<String>,
    args: Option<PrepareArgsResult>,
}

impl super::ControlCommandRequest for CallRequest {
    fn get_async(&self) -> bool {
        self.r#async
    }

    fn get_args(&self) -> Option<CallArgs> {
        self.arg.clone()
    }

    fn get_target_object(&self) -> CommandTarget {
        CommandTarget::Object(self.object_id)
    }
}

impl super::ControlCommand for CallCommand {
    const NAME:&'static str = "Call";
    type Request = CallRequest;

    fn new(
        request: Self::Request,
        args: Option<PrepareArgsResult>,
        id: u64,
        object: Option<PyObject>,
    ) -> Self {
        Self {
            id,
            object: object.unwrap(),
            attr: request.attr,
            args,
        }
    }

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject> {
        let mut o = self.object;
        for attr in self.attr {
            o = dbg_py(py, o.getattr(py, &attr).map_err(Into::into))?;
        }

        let ret = match self.args.map(|x| x.to_pyobject(py, pickle)) {
            Some(Ok((args, kwargs))) => o.call(
                py,
                args.cast_as(py)
                    .map_err(|x| anyhow::anyhow!("cast args error: {:#?}", x))?,
                Some(
                    kwargs
                        .cast_as(py)
                        .map_err(|x| anyhow::anyhow!("cast kwargs error: {:#?}", x))?,
                ),
            )?,
            Some(Err(err)) => {
                return Err(err.into());
            }
            None => o.call0(py)?,
        };

        Ok(ret)
    }
}
