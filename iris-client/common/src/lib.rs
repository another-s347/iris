use pyo3::prelude::*;
use pyo3::types::PyInt;
use pyo3::type_object::PyTypeInfo;

#[pyclass(module="client")]
#[derive(Clone)]
pub struct IrisObjectId {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub location: String,
    #[pyo3(get)]
    pub attr: Vec<String>
}

#[pymethods]
impl IrisObjectId {
    #[new]
    pub fn new(id:u64, location:String, attr: Vec<String>) -> IrisObjectId {
        IrisObjectId {
            id: id,
            location: location,
            attr
        }
    }

    pub fn add_attr(&self, attr: Vec<String>) -> IrisObjectId {
        let mut attrs = self.attr.clone();
        attrs.extend(attr);
        IrisObjectId {
            id: self.id,
            location: self.location.clone(),
            attr: attrs
        }
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