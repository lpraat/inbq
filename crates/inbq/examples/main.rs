use inbq::{
    lineage::{Catalog, Column, SchemaObject, SchemaObjectKind, lineage},
    parser::Parser,
    scanner::Scanner,
};

fn main() -> anyhow::Result<()> {
    let sql = r#"
        select (select 1);
    "#;
    env_logger::init();

    let mut scanner = Scanner::new(sql);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let ast = parser.parse()?;

    let s = serde_json::to_string_pretty(&ast)?;
    println!("{}", s);

    // println!("Syntax Tree: {:?}", ast);

    // let data_catalog = Catalog {
    //     schema_objects: vec![
    //         SchemaObject {
    //             name: "proj.dat.in_table".to_owned(),
    //             kind: SchemaObjectKind::Table,
    //             columns: vec![
    //                 // dtype is case insensitive and can be retrieved, for example, using
    //                 // the INFORMATION_SCHEMA.COLUMNS view (https://cloud.google.com/bigquery/docs/information-schema-columns)
    //                 Column {
    //                     name: "ix".to_owned(),
    //                     dtype: "int64".to_owned(),
    //                 },
    //                 Column {
    //                     name: "iy".to_owned(),
    //                     dtype: "int64".to_owned(),
    //                 },
    //             ],
    //         },
    //         SchemaObject {
    //             name: "proj.dat.out_table".to_owned(),
    //             kind: SchemaObjectKind::Table,
    //             columns: vec![
    //                 Column {
    //                     name: "ox".to_owned(),
    //                     dtype: "int64".to_owned(),
    //                 },
    //                 Column {
    //                     name: "oy".to_owned(),
    //                     dtype: "int64".to_owned(),
    //                 },
    //             ],
    //         },
    //     ],
    // };

    // let output_lineage = lineage(&ast, &data_catalog)?;

    // println!();
    // println!("Raw lineage: {:?}\n", output_lineage.raw);
    // println!();
    // println!("Ready (human-friendly) lineage: {:?}", output_lineage.ready);
    // -> Ready (human-friendly) lineage: ReadyLineage { objects: [ReadyLineageObject { name: "proj.dat.out_table", kind: "table", nodes: [
    // ReadyLineageNode { name: "a", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "a" }] },
    // ReadyLineageNode { name: "f1", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "s.f1" }]},
    // ReadyLineageNode { name: "f2", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "s.f2" }] }] }] }
    Ok(())
}
