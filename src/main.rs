use std::{env, fs, time::Instant};

use anyhow::anyhow;
use lineage::compute_lineage;
use parser::{AstPrinter, Parser, Query};
use scanner::Scanner;

pub mod arena;
pub mod lineage;
pub mod parser;
pub mod scanner;

fn parse(sql: &str) -> anyhow::Result<Query> {
    println!("Parsing {}", &sql[..std::cmp::min(50, sql.len())]);

    let mut scanner = Scanner::new(sql);
    let tokens = scanner.scan();
    if scanner.had_error {
        println!("Exiting. Found error while scanning.");
        return Err(anyhow!("scanner error"));
    }

    for token in &tokens {
        println!("{:?}", token);
    }

    let mut parser = Parser::new(&tokens);
    // NOTE: we should return an Err also in the scanner scan method
    parser.parse()
}

fn main() -> anyhow::Result<()> {
    let input_args = env::args().collect::<Vec<String>>();

    let now = Instant::now();

    match input_args.len() {
        2 => {
            let sql_path = env::current_dir()?.join(input_args[1].clone());
            let sql_str = fs::read_to_string(sql_path)?;
            let ast = parse(&sql_str)?;
            println!("Parsed AST: {:?}\n\n", ast);
            let output_lineage = compute_lineage(&ast);
            println!("Output lineage: {:?}.", output_lineage);
        }
        _ => println!("Usage: inbq [sql_file_path]")
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}
