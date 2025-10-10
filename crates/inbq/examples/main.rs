use inbq::{
    lineage::{Catalog, Column, SchemaObject, SchemaObjectKind, extract_lineage},
    parser::Parser,
    scanner::Scanner,
};

fn main() -> anyhow::Result<()> {
    let sql = r#"
        insert into proj.dat.out_table
        select
            a,
            s.f1,
            s.f2,
        from proj.dat.in_table
    "#;
    let mut scanner = Scanner::new(sql);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let ast = parser.parse()?;
    println!("Syntax Tree: {:?}", ast);

    let data_catalog = Catalog {
        schema_objects: vec![
            SchemaObject {
                name: "proj.dat.in_table".to_owned(),
                kind: SchemaObjectKind::Table {
                    columns: vec![
                        // dtype is case insensitive and can be retrieved, for example, using
                        // the INFORMATION_SCHEMA.COLUMNS view (https://cloud.google.com/bigquery/docs/information-schema-columns)
                        Column {
                            name: "a".to_owned(),
                            dtype: "STRING".to_owned(),
                        },
                        Column {
                            name: "s".to_owned(),
                            dtype: "STRUCT<f1 INT64, f2 INT64>".to_owned(),
                        },
                    ],
                },
            },
            SchemaObject {
                name: "proj.dat.out_table".to_owned(),
                kind: SchemaObjectKind::Table {
                    columns: vec![
                        Column {
                            name: "a".to_owned(),
                            dtype: "STRING".to_owned(),
                        },
                        Column {
                            name: "f1".to_owned(),
                            dtype: "INT64".to_owned(),
                        },
                        Column {
                            name: "f2".to_owned(),
                            dtype: "INT64".to_owned(),
                        },
                    ],
                },
            },
        ],
    };

    let output_lineage = extract_lineage(&[&ast], &data_catalog, false, true)
        .pop()
        .unwrap()?;

    println!();
    println!("Raw lineage: {:?}\n", output_lineage.raw_lineage);
    println!();
    println!(
        "Ready (human-friendly) lineage: {:?}",
        output_lineage.lineage
    );
    Ok(())
}
