use pyo3::prelude::*;
use pyo3::types::PyInt;
use pyo3::type_object::PyTypeInfo;

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

impl IrisObjectId {
    pub unsafe fn unsound_extract(x: &PyAny) -> Option<Self> {
        if x.get_type().name() != "client.IrisObjectId" {
            return None;
        }
        println!("{}, {}", Self::is_instance(x), x.get_type().name());
        let cell: &PyCell<IrisObjectId> = unsafe { PyTryFrom::try_from_unchecked(x) };
        let y = cell.try_borrow().ok()?.clone();
        std::mem::forget(cell);
        Some(y)
    }
}

// #[pymodule]
// fn client(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
//     // m.add_wrapped(wrap_pyfunction!(count_line))?;
//     // m.add_wrapped(wrap_pyfunction!(create_iris_client))?;
//     // m.add_class::<WordCounter>()?;
//     // m.add_class::<IrisContextInternal>()?;
//     // m.add_class::<IrisClientInternal>()?;
//     // m.add_class::<IrisObjectInternal>()?;
//     m.add_class::<IrisObjectId>()?;
//     // m.add_class::<AsyncTest>()?;

//     Ok(())
// }