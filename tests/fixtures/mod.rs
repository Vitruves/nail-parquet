use std::path::Path;

pub fn setup_test_fixtures() {
	let fixtures_dir = Path::new("tests/fixtures");
	std::fs::create_dir_all(fixtures_dir).unwrap();
	
	let sample_parquet = fixtures_dir.join("sample.parquet");
	if !sample_parquet.exists() {
		crate::common::create_sample_parquet(&sample_parquet).unwrap();
	}
}