use pyo3::prelude::*;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[pyclass]
#[derive(Clone)]
pub struct IrisObjectId {
    pub id: u64
}

#[pymethods]
impl IrisObjectId {
    #[new]
    pub fn new(id:u64) -> IrisObjectId {
        IrisObjectId {
            id
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
