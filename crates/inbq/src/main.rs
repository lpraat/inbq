use std::path::PathBuf;

use anyhow::anyhow;
use clap::Parser as ClapParser;
use clap::Subcommand;
use inbq::lineage::{Catalog, RawLineage, ReadyLineage, extract_lineage};
use inbq::parser::parse_sql;
use indexmap::IndexMap;
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
}

#[derive(Serialize)]
#[serde(untagged)]
enum OutLineage {
    Ok(OkLineage),
    ErrLineage { error: String },
}

#[derive(Serialize)]
struct OkLineage {
    lineage: ReadyLineage,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw: Option<RawLineage>,
}

fn output_lineage(
    lineage_command: &LineageCommand,
    catalog: &Catalog,
    sql_file_path: &PathBuf,
) -> anyhow::Result<OutLineage> {
    let sql = std::fs::read_to_string(sql_file_path).map_err(|_| {
        anyhow!(
            "Failed to read sql file {}",
            sql_file_path.display().to_string()
        )
    })?;
    let lineage = extract_lineage(&parse_sql(&sql)?, catalog);
    let out_lineage = match lineage {
        Ok(lineage) => OutLineage::Ok(OkLineage {
            lineage: lineage.ready,
            raw: if lineage_command.include_raw {
                Some(lineage.raw)
            } else {
                None
            },
        }),
        Err(err) => OutLineage::ErrLineage {
            error: format!(
                "Could not extract lineage from SQL in file {} due to error: {}",
                sql_file_path.display(),
                err
            ),
        },
    };
    Ok(out_lineage)
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
                        lineage_command.catalog.display().to_string()
                    )
                })?,
            )
            .map_err(|err| {
                anyhow!(
                    "Failed to parse JSON catalog in file {} due to error: {}",
                    lineage_command.catalog.display().to_string(),
                    err
                )
            })?;
            let out_str = if sql_file_or_dir.is_dir() {
                let mut file_lineages: IndexMap<String, OutLineage> = IndexMap::new();
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

                for sql_file in sql_in_dir {
                    let output_lineage = output_lineage(lineage_command, &catalog, &sql_file)?;
                    file_lineages.insert(
                        std::path::absolute(sql_file)?.display().to_string(),
                        output_lineage,
                    );
                }

                if lineage_command.pretty {
                    serde_json::to_string_pretty(&file_lineages)?
                } else {
                    serde_json::to_string(&file_lineages)?
                }
            } else {
                let output_lineage = output_lineage(lineage_command, &catalog, sql_file_or_dir)?;
                if lineage_command.pretty {
                    serde_json::to_string_pretty(&output_lineage)?
                } else {
                    serde_json::to_string(&output_lineage)?
                }
            };
            println!("{}", out_str);
        }
    }

    let elapsed = now.elapsed();
    log::info!("Elapsed: {:.2?}", elapsed);

    Ok(())
}
