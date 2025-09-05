use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};

#[pyfunction]
fn rs_parse_sql(sql: &str) -> PyResult<String> {
    let rs_ast = inbq::parser::parse_sql(sql).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let json_ast =
        serde_json::to_string(&rs_ast).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(json_ast)
}

#[pymodule]
fn _inbq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rs_parse_sql, m)?)?;
    Ok(())
}
