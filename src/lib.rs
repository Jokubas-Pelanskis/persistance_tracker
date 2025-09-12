mod v1;


use pyo3::prelude::*;

#[pymodule]
fn graphrlib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    
    // Create and configure v1 submodule
    let v1_module = PyModule::new(py, "v1")?;
    v1_module.add_class::<v1::Database>()?;
    v1_module.add_class::<v1::DatabaseTemplate>()?;
    m.add_submodule(&v1_module)?;
    Ok(())
}