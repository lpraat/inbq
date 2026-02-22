use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use inbq::{
    ast::Ast,
    lineage::{catalog::Catalog, extract_lineage},
    parser::parse_sql,
    test_utils::{LINEAGE_TESTS_FILE, TestLineageData},
};

fn extract_lineage_tests(asts: &[Ast], catalogs: &[Catalog]) {
    for (ast, catalog) in asts.iter().zip(catalogs) {
        let _ = extract_lineage(&[ast], catalog, false, false);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let lineage_data_file =
        std::fs::read_to_string(LINEAGE_TESTS_FILE).expect("Cannot open lineage test cases");
    let test_lineage_data: TestLineageData =
        toml::from_str(&lineage_data_file).expect("Cannot parse test cases defined in toml");

    let mut asts = vec![];
    let mut catalogs = vec![];
    for test in &test_lineage_data.tests {
        asts.push(
            parse_sql(&test.sql)
                .unwrap_or_else(|err| panic!("Could not parse sql due to: {:?}", &err)),
        );
        catalogs.push(Catalog {
            schema_objects: test.schema_objects.clone(),
        })
    }

    c.bench_function("bench lineage tests", |b| {
        b.iter(|| extract_lineage_tests(black_box(&asts), black_box(&catalogs)))
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(1000);
    targets = criterion_benchmark
);
criterion_main!(benches);
