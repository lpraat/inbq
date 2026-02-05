//! # inbq
//!
//! A library for parsing BigQuery queries and extracting schema-aware, column-level lineage.
//!
//! # Features
//!
//! - Parse BigQuery queries into well-structured ASTs with easy-to-navigate nodes.
//! - Extract schema-aware, column-level lineage.
//! - Trace data flow through nested structs and arrays.
//! - Capture referenced columns and the specific query components (e.g., select, where, join) they appear in.
//! - Process both single and multi-statement queries with procedural language constructs.
//! - Combines the performance of a Rust core with the ease of a Python API through efficient, low-overhead bindings.
//!
//! # Example
//!
//! ```rust,no_run
//! use inbq::{
//!     lineage::{
//!         catalog::{Catalog, Column, SchemaObject, SchemaObjectKind},
//!         extract_lineage,
//!     },
//!     parser::Parser,
//!     scanner::Scanner,
//! };
//!
//! fn column(name: &str, dtype: &str) -> Column {
//!     Column {
//!         name: name.to_owned(),
//!         dtype: dtype.to_owned(),
//!     }
//! }
//!
//! fn main() -> anyhow::Result<()> {
//!     env_logger::init();
//!
//!     let sql = r#""
//!         declare default_val float64 default (select min(val) from project.dataset.out);
//!
//!         insert into `project.dataset.out`
//!         select
//!             id,
//!             if(x is null or s.x is null, default_val, x + s.x)
//!         from `project.dataset.t1` inner join `project.dataset.t2` using (id)
//!         where s.source = "baz";
//!     ""#;
//!     let mut scanner = Scanner::new(sql);
//!     scanner.scan()?;
//!     let mut parser = Parser::new(scanner.tokens());
//!     let ast = parser.parse()?;
//!     println!("Syntax Tree: {:?}", ast);
//!
//!     let data_catalog = Catalog {
//!         schema_objects: vec![
//!             SchemaObject {
//!                 name: "project.dataset.out".to_owned(),
//!                 kind: SchemaObjectKind::Table {
//!                     columns: vec![column("id", "int64"), column("val", "int64")],
//!                 },
//!             },
//!             SchemaObject {
//!                 name: "project.dataset.t1".to_owned(),
//!                 kind: SchemaObjectKind::Table {
//!                     columns: vec![column("id", "int64"), column("x", "float64")],
//!                 },
//!             },
//!             SchemaObject {
//!                 name: "project.dataset.t2".to_owned(),
//!                 kind: SchemaObjectKind::Table {
//!                     columns: vec![
//!                         column("id", "int64"),
//!                         column("s", "struct<source string, x float64>"),
//!                     ],
//!                 },
//!             },
//!         ],
//!     };
//!
//!     let lineage = extract_lineage(&[&ast], &data_catalog, false, true)
//!         .pop()
//!         .unwrap()?;
//!
//!     println!("\nLineage: {:?}", lineage.lineage);
//!     println!("\nReferenced columns: {:?}", lineage.referenced_columns);
//!     Ok(())
//! }
//! ```
mod arena;
pub mod ast;
pub mod lineage;
pub mod parser;
pub mod scanner;
