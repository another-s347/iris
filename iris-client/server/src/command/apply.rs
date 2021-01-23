use super::{args::PrepareArgsResult, CommandTarget};
use crate::utils::{dbg_py, loads};
use crate::{
    distributed,
    hello_world::{greeter_server::Greeter, *},
    mem::LazyPyObject,
    utils::LocalObject,
    Opt,
};
use anyhow::Context;
use pyo3::{PyObject, PyResult, Python};

pub struct ApplyCommand {
    id: u64,
    body: bytes::Bytes,
    args: Option<PrepareArgsResult>,
}

impl super::ControlCommandRequest for ApplyRequest {
    fn get_option(&self) -> Option<&RequestOption> {
        self.options.as_ref()
    }

    fn get_args(&self) -> Option<CallArgs> {
        self.arg.clone()
    }

    fn get_target_object(&self) -> CommandTarget {
        CommandTarget::None
    }
}

impl super::ControlCommand for ApplyCommand {
    const NAME:&'static str = "Apply";
    type Request = ApplyRequest;

    fn new(
        request: Self::Request,
        args: Option<PrepareArgsResult>,
        id: u64,
        _object: Option<PyObject>,
    ) -> Self {
        Self {
            id,
            body: request.func,
            args,
        }
    }

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject> {
        let o = loads(pickle, py, self.body.as_ref())?;

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
