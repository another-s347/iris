use pyo3::{PyObject, PyResult, Python};
use crate::utils::dbg_py;
use crate::{Opt, distributed, hello_world::{greeter_server::Greeter, *}, mem::LazyPyObject, utils::LocalObject};
use super::{CommandTarget, args::PrepareArgsResult};

pub struct GetAttrCommand {
    id: u64,
    object: PyObject,
    attr: Vec<String>
}

impl super::ControlCommandRequest for GetAttrRequest {
    fn get_async(&self) -> bool {
        self.r#async
    }

    fn get_args(&self) -> Option<CallArgs> {
        None
    }

    fn get_target_object(&self) -> CommandTarget {
        CommandTarget::Object(self.object_id)
    }
}

impl super::ControlCommand for GetAttrCommand {
    const NAME:&'static str = "GetAttr";
    type Request = GetAttrRequest;

    fn new(request: Self::Request, args: Option<PrepareArgsResult>, id: u64, object: Option<PyObject>) -> Self {
        Self {
            id,
            object: object.unwrap(),
            attr: request.attr,
        }
    }

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject> {
        let mut o = self.object;
        for attr in self.attr {
            o = dbg_py(py, o.getattr(py, &attr).map_err(Into::into))?;
        }

        Ok(o)
    }
}