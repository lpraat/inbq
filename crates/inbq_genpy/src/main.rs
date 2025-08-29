use std::{env, fs::File, io::Write};

use inbq_genpy::generator::PyCodeGenerator;

fn main() -> anyhow::Result<()> {
    let current_dir = env::current_dir()?;
    let source_file_path = current_dir.join("crates/inbq/src/ast.rs");
    let output_file_path = current_dir.join("crates/py_inbq/python/inbq/ast_nodes.py");

    let py_code_generator = PyCodeGenerator::new(&source_file_path)?;
    let code = py_code_generator.generate_ast_nodes_file_str()?;

    let mut output_file = File::create(output_file_path)?;
    output_file.write_all(code.as_bytes())?;
    Ok(())
}
