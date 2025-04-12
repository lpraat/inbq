use std::{env, fs, time::Instant};

use anyhow::anyhow;
use inbq::lineage::{lineage, SchemaObject, SchemaObjectKind};
use inbq::parser::{parse_sql, Ast, Parser};
use inbq::scanner::Scanner;

// interface:
// lib:
// - parse_sql
// - sql_lineage
// main:
// - parse from file/files
// - lineage from file/files

fn main() -> anyhow::Result<()> {
    let input_args = env::args().collect::<Vec<String>>();

    let now = Instant::now();

    match input_args.len() {
        2 => {
            let sql_path = env::current_dir()?.join(input_args[1].clone());
            let sql_str = fs::read_to_string(sql_path)?;
            let ast = parse_sql(&sql_str)?;
            println!("Parsed AST: {:?}\n\n", ast);

            // TODO: read schema objects from file
            let schema_objects = vec![
                SchemaObject {
                    name: "tmp".to_owned(),
                    kind: SchemaObjectKind::Table,
                    columns: vec!["z".to_owned()],
                },
                SchemaObject {
                    name: "D".to_owned(),
                    kind: SchemaObjectKind::Table,
                    columns: vec!["z".to_owned()],
                },
            ];

            let output_lineage = lineage(&ast, &schema_objects);
            // println!("Output lineage: {:?}.", output_lineage);
        }
        _ => println!("Usage: inbq [sql_file_path]"),
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}
