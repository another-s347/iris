use pyo3::prelude::*;
use pyo3::types::PyInt;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[pyclass(module="client")]
#[derive(Clone)]
pub struct IrisObjectId {
    #[pyo3(get)]
    pub id: u64
}

#[pymethods]
impl IrisObjectId {
    #[new]
    pub fn new(id:Option<u64>) -> IrisObjectId {
        IrisObjectId {
            id: id.unwrap_or(0)
        }
    }

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyInt>(py) {
            Ok(s) => {
                self.id = s.extract().unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.id.into_py(py))
        // Ok(PyInt::new(py, &serialize(&self.foo).unwrap()).to_object(py))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
