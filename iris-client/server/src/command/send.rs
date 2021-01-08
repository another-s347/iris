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

pub struct SendCommand {
    id: u64,
    body: Vec<u8>,
}

impl super::ControlCommandRequest for SendRequest {
    fn get_option(&self) -> Option<&RequestOption> {
        self.options.as_ref()
    }

    fn get_args(&self) -> Option<CallArgs> {
        None
    }

    fn get_target_object(&self) -> CommandTarget {
        CommandTarget::None
    }
}

impl super::ControlCommand for SendCommand {
    const NAME:&'static str = "Send";
    type Request = SendRequest;

    fn new(
        request: Self::Request,
        args: Option<PrepareArgsResult>,
        id: u64,
        _object: Option<PyObject>,
    ) -> Self {
        Self {
            id,
            body: request.func,
        }
    }

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject> {
        let o = loads(pickle, py, self.body.as_ref())?;

        Ok(o)
    }
}
