use pyo3::prelude::*;

#[pyfunction]
fn hello_world() {
    println!("Hello, world")
}

#[pymodule]
fn _inbq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;
    Ok(())
}
