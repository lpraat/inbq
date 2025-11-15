use std::mem;
use std::mem::MaybeUninit;
use std::path::PathBuf;

use anyhow::anyhow;
use clap::Parser as ClapParser;
use clap::Subcommand;
use inbq::ast::Ast;
use inbq::lineage::Lineage;
use inbq::lineage::extract_lineage;
use inbq::parser::parse_sql;
use indexmap::IndexMap;
use rayon::prelude::*;
use serde::Serialize;
use std::time::Instant;

#[derive(clap::Parser)]
#[command(name = "inbq")]
#[command(about = "BigQuery SQL parser and lineage extractor", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract lineage from one or more SQL files.
    ExtractLineage(LineageCommand),
}

#[derive(clap::Args)]
struct LineageCommand {
    /// Path to the input file containing schema objects.
    #[arg(short, long)]
    catalog: PathBuf,
    /// Path to the SQL file or directory containing SQL files.
    #[arg(value_name = "SQL_[FILE|DIR]")]
    sql: PathBuf,
    /// Include raw lineage objects in the output.
    #[arg(long)]
    include_raw: bool,
    /// Pretty-print the output lineage.
    #[arg(long)]
    pretty: bool,
    /// Process the SQLs in parallel
    #[arg(long)]
    parallel: bool,
}

#[derive(Serialize)]
#[serde(untagged)]
enum OutLineage {
    Ok(Lineage),
    Err { error: String },
}

fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    env_logger::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::ExtractLineage(lineage_command) => {
            let sql_file_or_dir = &lineage_command.sql;
            let catalog = serde_json::from_str(
                &std::fs::read_to_string(&lineage_command.catalog).map_err(|_| {
                    anyhow!(
                        "Failed to read catalog file: {}",
                        lineage_command.catalog.display()
                    )
                })?,
            )
            .map_err(|err| {
                anyhow!(
                    "Failed to parse JSON catalog in file {} due to error: {}",
                    lineage_command.catalog.display(),
                    err
                )
            })?;

            let sql_file_paths = if sql_file_or_dir.is_dir() {
                let sql_in_dir: Vec<_> = std::fs::read_dir(sql_file_or_dir)?
                    .filter_map(|res| res.ok())
                    .map(|entry| entry.path())
                    .filter_map(|file| {
                        if file.extension().is_some_and(|ext| ext == "sql") {
                            Some(file)
                        } else {
                            None
                        }
                    })
                    .collect();
                sql_in_dir
            } else {
                vec![sql_file_or_dir.clone()]
            };

            let out_str = {
                let mut sqls = vec![];
                for sql_file_path in &sql_file_paths {
                    let sql = std::fs::read_to_string(sql_file_path).map_err(|_| {
                        anyhow!("Failed to read sql file {}", sql_file_path.display())
                    })?;
                    sqls.push(sql);
                }

                let asts: Vec<anyhow::Result<Ast>> = if lineage_command.parallel {
                    sqls.par_iter().map(|sql| parse_sql(sql)).collect()
                } else {
                    sqls.iter().map(|sql| parse_sql(sql)).collect()
                };

                let closure = |asts: &[anyhow::Result<Ast>]| -> Vec<anyhow::Result<Lineage>> {
                    let ok_asts: Vec<(usize, &Ast)> = asts
                        .iter()
                        .map(|r| r.as_ref())
                        .enumerate()
                        .filter(|(_, ast)| ast.is_ok())
                        .map(|(idx, el)| (idx, el.unwrap()))
                        .collect();

                    let ko_asts: Vec<(usize, anyhow::Result<Lineage>)> = asts
                        .iter()
                        .map(|r| r.as_ref())
                        .enumerate()
                        .filter(|(_, ast)| ast.is_err())
                        .map(|(idx, res)| match res {
                            Err(err) => (idx, Err(anyhow!(err.to_string()))),
                            _ => unreachable!(),
                        })
                        .collect();

                    let lineages = extract_lineage(
                        &ok_asts.iter().map(|(_, ast)| *ast).collect::<Vec<&Ast>>(),
                        &catalog,
                        lineage_command.include_raw,
                        false,
                    );

                    let mut output: Vec<MaybeUninit<anyhow::Result<Lineage>>> =
                        Vec::with_capacity(asts.len());
                    unsafe { output.set_len(asts.len()) };

                    for (index, result) in ko_asts {
                        output[index].write(result);
                    }
                    for ((index, _), lin) in ok_asts.into_iter().zip(lineages) {
                        output[index].write(lin);
                    }

                    unsafe { mem::transmute::<_, Vec<anyhow::Result<Lineage>>>(output) }
                };

                let lineages: Vec<anyhow::Result<Lineage>> = if lineage_command.parallel {
                    let n_chunks = std::cmp::max(
                        1,
                        asts.len() / std::thread::available_parallelism().unwrap().get(),
                    );
                    asts.par_chunks(n_chunks).flat_map(closure).collect()
                } else {
                    closure(&asts)
                };

                let mut file_lineages: IndexMap<String, OutLineage> = IndexMap::new();

                for (sql_file, lin) in sql_file_paths.into_iter().zip(lineages) {
                    let path_name = std::path::absolute(sql_file)?.display().to_string();
                    match lin {
                        Ok(lin) => {
                            file_lineages.insert(path_name, OutLineage::Ok(lin));
                        }
                        Err(err) => {
                            file_lineages.insert(
                                path_name,
                                OutLineage::Err {
                                    error: err.to_string(),
                                },
                            );
                        }
                    }
                }

                if lineage_command.pretty {
                    serde_json::to_string_pretty(&file_lineages)?
                } else {
                    serde_json::to_string(&file_lineages)?
                }
            };
            println!("{}", out_str);
        }
    }

    let elapsed = now.elapsed();
    log::debug!("Elapsed: {:.2?}", elapsed);

    Ok(())
}
