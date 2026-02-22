use criterion::{Criterion, criterion_group, criterion_main};
use inbq::{
    parser::parse_sql,
    test_utils::{PARSING_TESTS_FILE, TestParsingData},
};
use std::hint::black_box;

fn bench_parsing(sqls: &[&String]) {
    for sql in sqls {
        let _ = parse_sql(sql);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let parsing_test_file =
        std::fs::read_to_string(PARSING_TESTS_FILE).expect("Cannot open parsing test cases");
    let test_parsing_data: TestParsingData =
        toml::from_str(&parsing_test_file).expect("Cannot parse test cases defined in toml");

    let sqls = test_parsing_data
        .tests
        .iter()
        .map(|t| &t.sql)
        .collect::<Vec<_>>();

    c.bench_function("bench parsing tests", |b| {
        b.iter(|| bench_parsing(black_box(&sqls)))
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(1000);
    targets = criterion_benchmark
);
criterion_main!(benches);
