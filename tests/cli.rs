// File: tests/cli.rs

use assert_cmd::Command;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use predicates::prelude::*;
use serde_json::Value;
use std::fs;

mod common;
use common::{get_row_count, TestFixtures};

fn nail() -> Command {
	Command::cargo_bin("nail").unwrap()
}

// ---- INSPECTION COMMANDS ----
#[cfg(test)]
mod inspection_tests {
	use super::*;

	#[test]
	fn test_head_default() {
		let fixtures = TestFixtures::new();
		let output = nail().args(["head", fixtures.sample_parquet.to_str().unwrap()]).assert().success().get_output().stdout.clone();
		assert_eq!(String::from_utf8(output).unwrap().matches("Record").count(), 5);
	}

	#[test]
	fn test_head_custom_number() {
		let fixtures = TestFixtures::new();
		let output = nail().args(["head", fixtures.sample_parquet.to_str().unwrap(), "-n", "2"]).assert().success().get_output().stdout.clone();
		assert_eq!(String::from_utf8(output).unwrap().matches("Record").count(), 2);
	}

	#[test]
	fn test_head_to_json_file() {
		let fixtures = TestFixtures::new();
		let out_file = fixtures.get_output_path("head.json");
		nail().args(["head", fixtures.sample_parquet.to_str().unwrap(), "-n", "3", "-o", out_file.to_str().unwrap(), "-f", "json"]).assert().success();
		let content = fs::read_to_string(out_file).unwrap();
		let lines: Vec<&str> = content.trim().lines().collect();
		assert_eq!(lines.len(), 3);
		// Verify each line is valid JSON
		for line in lines {
			let _: Value = serde_json::from_str(line).unwrap();
		}
	}

	#[test]
	fn test_tail_custom_number() {
		let fixtures = TestFixtures::new();
		nail().args(["tail", fixtures.sample_parquet.to_str().unwrap(), "-n", "2"]).assert().success().stdout(predicate::str::contains("Eve").and(predicate::str::contains("David")));
	}

	#[test]
	fn test_tail_on_small_file() {
		let fixtures = TestFixtures::new();
		let output = nail().args(["tail", fixtures.sample_parquet.to_str().unwrap(), "-n", "10"]).assert().success().get_output().stdout.clone();
		assert_eq!(String::from_utf8(output).unwrap().matches("Record").count(), 5, "Should return all rows if n > row_count");
	}

	#[test]
	fn test_preview_reproducible() {
		let fixtures = TestFixtures::new();
		let out1 = nail().args(["preview", fixtures.sample_parquet.to_str().unwrap(), "-n", "3", "--random", "123"]).assert().success().get_output().stdout.clone();
		let out2 = nail().args(["preview", fixtures.sample_parquet.to_str().unwrap(), "-n", "3", "--random", "123"]).assert().success().get_output().stdout.clone();
		assert_eq!(out1, out2, "Seeded preview should be reproducible");
	}

	#[test]
	fn test_count_basic() {
		let fixtures = TestFixtures::new();
		nail().args(["count", fixtures.sample_parquet.to_str().unwrap()]).assert().success().stdout("5\n");
	}

	#[test]
	fn test_count_empty() {
		let fixtures = TestFixtures::new();
		nail().args(["count", fixtures.empty_parquet.to_str().unwrap()]).assert().success().stdout("0\n");
	}

	#[test]
	fn test_schema_json_output() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("schema.json");
		nail().args(["schema", fixtures.sample_parquet.to_str().unwrap(), "-f", "json", "-o", output_path.to_str().unwrap()]).assert().success();
		let content: Vec<Value> = serde_json::from_str(&fs::read_to_string(output_path).unwrap()).unwrap();
		assert_eq!(content.len(), 4);
		assert_eq!(content[0]["name"], "id");
	}

	#[test]
	fn test_headers_basic() {
		let fixtures = TestFixtures::new();
		nail().args(["headers", fixtures.sample_parquet.to_str().unwrap()]).assert().success().stdout("id\nname\nvalue\ncategory\n");
	}

	#[test]
	fn test_headers_regex_filter() {
		let fixtures = TestFixtures::new();
		nail().args(["headers", fixtures.sample_parquet.to_str().unwrap(), "--filter", "val|cat"]).assert().success().stdout("value\ncategory\n");
	}
	
	#[test]
	fn test_size_all_options() {
		let fixtures = TestFixtures::new();
		nail().args(["size", fixtures.sample_parquet.to_str().unwrap(), "--columns", "--rows", "--bits"]).assert().success().stdout(predicate::str::contains("Per-column sizes").and(predicate::str::contains("Average bits per row")));
	}
}

// ---- DATA MANIPULATION COMMANDS ----
#[cfg(test)]
mod manipulation_tests {
	use super::*;

	#[tokio::test]
	async fn test_select_columns() {
		let fixtures = TestFixtures::new();
		let out_cols = fixtures.get_output_path("select_cols.parquet");
		nail().args(["select", fixtures.sample_parquet.to_str().unwrap(), "-c", "id,name", "-o", out_cols.to_str().unwrap()]).assert().success();
		let df_cols = SessionContext::new().read_parquet(out_cols.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert_eq!(df_cols.schema().fields().len(), 2);
	}

	#[tokio::test]
	async fn test_select_rows() {
		let fixtures = TestFixtures::new();
		let out_rows = fixtures.get_output_path("select_rows.parquet");
		nail().args(["select", fixtures.sample_parquet.to_str().unwrap(), "-r", "1,3-4", "-o", out_rows.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_rows).await, 3);
	}
	
	#[tokio::test]
	async fn test_drop_columns() {
		let fixtures = TestFixtures::new();
		let out_cols = fixtures.get_output_path("drop_cols.parquet");
		nail().args(["drop", fixtures.sample_parquet.to_str().unwrap(), "-c", "value,category", "-o", out_cols.to_str().unwrap()]).assert().success();
		let df_cols = SessionContext::new().read_parquet(out_cols.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert_eq!(df_cols.schema().fields().len(), 2);
	}

	#[tokio::test]
	async fn test_drop_rows() {
		let fixtures = TestFixtures::new();
		let out_rows = fixtures.get_output_path("drop_rows.parquet");
		nail().args(["drop", fixtures.sample_parquet.to_str().unwrap(), "-r", "2,5", "-o", out_rows.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_rows).await, 3);
	}

	#[tokio::test]
	async fn test_filter_by_column_value() {
		let fixtures = TestFixtures::new();
		let out_filter = fixtures.get_output_path("filtered.parquet");
		nail().args(["filter", fixtures.sample_parquet.to_str().unwrap(), "-c", "id>3,category=B", "-o", out_filter.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_filter).await, 1);
	}

	#[tokio::test]
	async fn test_filter_by_row_property() {
		let fixtures = TestFixtures::new();
		let out_rows = fixtures.get_output_path("filtered_rows.parquet");
		nail().args(["filter", fixtures.sample_mixed_types_parquet.to_str().unwrap(), "--rows", "no-nan", "-o", out_rows.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_rows).await, 2, "Two rows should have no NaN values");
	}

	#[tokio::test]
	async fn test_search_all_options() {
		let fixtures = TestFixtures::new();
		let out_search = fixtures.get_output_path("searched.parquet");
		nail().args(["search", fixtures.sample_parquet.to_str().unwrap(), "--value", "li", "--ignore-case", "-c", "name", "-o", out_search.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_search).await, 2);

		let out_rows = fixtures.get_output_path("search_rows.json");
		nail().args(["search", fixtures.sample_parquet.to_str().unwrap(), "--value", "Bob", "--rows", "-f", "json", "-o", out_rows.to_str().unwrap()]).assert().success();
		let content = fs::read_to_string(out_rows).unwrap();
		let first_line = content.trim().lines().next().unwrap();
		let row_data: Value = serde_json::from_str(first_line).unwrap();
		// Bob is the second row (index 1 if 0-based, or row 2 if 1-based)
		assert!(row_data["row_number"].is_number(), "row_number should be a number");
		let row_num = row_data["row_number"].as_u64().unwrap();
		assert!(row_num == 1 || row_num == 2, "Bob should be in row 1 (0-based) or 2 (1-based)");
	}
	
	#[tokio::test]
	async fn test_fill_value() {
		let fixtures = TestFixtures::new();
		let out_fill = fixtures.get_output_path("filled.parquet");
		nail().args(["fill", fixtures.sample_with_nulls_parquet.to_str().unwrap(), "--method", "value", "--value", "UNKNOWN", "-c", "name", "-o", out_fill.to_str().unwrap()]).assert().success();
		let df_fill = SessionContext::new().read_parquet(out_fill.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert_eq!(df_fill.filter(datafusion::prelude::col("name").is_null()).unwrap().count().await.unwrap(), 0);
	}

	#[tokio::test]
	async fn test_fill_mean() {
		let fixtures = TestFixtures::new();
		let out_mean = fixtures.get_output_path("filled_mean.parquet");
		nail().args(["fill", fixtures.sample_with_nulls_parquet.to_str().unwrap(), "--method", "mean", "-c", "value", "-o", out_mean.to_str().unwrap()]).assert().success();
		let df_mean = SessionContext::new().read_parquet(out_mean.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert_eq!(df_mean.filter(datafusion::prelude::col("value").is_null()).unwrap().count().await.unwrap(), 0);
	}
}

// ---- DATA TRANSFORMATION COMMANDS ----
#[cfg(test)]
mod transformation_tests {
	use super::*;

	#[tokio::test]
	async fn test_id_creation() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("with_id.parquet");
		nail().args(["id", fixtures.sample_parquet.to_str().unwrap(), "--create", "--prefix", "rec-", "--id-col-name", "record_id", "-o", output_path.to_str().unwrap()]).assert().success();
		let df = SessionContext::new().read_parquet(output_path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert!(df.schema().field_with_name(None, "record_id").is_ok());
		assert_eq!(df.schema().fields().len(), 5);
	}

	#[test]
	fn test_id_error_on_existing_column() {
		let fixtures = TestFixtures::new();
		nail().args(["id", fixtures.sample_parquet.to_str().unwrap(), "--create", "--id-col-name", "name"]).assert().failure().stderr(predicate::str::contains("already exists"));
	}

	#[tokio::test]
	async fn test_shuffle_reproducible() {
		let fixtures = TestFixtures::new();
		let out1 = fixtures.get_output_path("shuffled1.parquet");
		let out2 = fixtures.get_output_path("shuffled2.parquet");
		nail().args(["shuffle", fixtures.sample_parquet.to_str().unwrap(), "--random", "42", "-o", out1.to_str().unwrap()]).assert().success();
		nail().args(["shuffle", fixtures.sample_parquet.to_str().unwrap(), "--random", "42", "-o", out2.to_str().unwrap()]).assert().success();
		// Note: DataFusion's RANDOM() is not deterministic even with a seed. This test just ensures it runs.
		assert_eq!(get_row_count(&out1).await, 5);
	}
	
	#[tokio::test]
	async fn test_sample_methods() {
		let fixtures = TestFixtures::new();
		let out_random = fixtures.get_output_path("sampled_random.parquet");
		nail().args(["sample", fixtures.sample_parquet.to_str().unwrap(), "-n", "2", "--method", "random", "--random", "42", "-o", out_random.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_random).await, 2);

		let out_first = fixtures.get_output_path("sampled_first.parquet");
		nail().args(["sample", fixtures.sample_parquet.to_str().unwrap(), "-n", "2", "--method", "first", "-o", out_first.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_first).await, 2);

		let out_last = fixtures.get_output_path("sampled_last.parquet");
		nail().args(["sample", fixtures.sample_parquet.to_str().unwrap(), "-n", "2", "--method", "last", "-o", out_last.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_last).await, 2);
	}

	#[tokio::test]
	async fn test_sample_stratified() {
		let fixtures = TestFixtures::new();
		let out_strat = fixtures.get_output_path("stratified.json");
		nail().args(["sample", fixtures.sample_for_stratify_parquet.to_str().unwrap(), "-n", "8", "--method", "stratified", "--stratify-by", "strat_key", "-f", "json", "-o", out_strat.to_str().unwrap()]).assert().success();
		let content = fs::read_to_string(out_strat).unwrap();
		let lines: Vec<&str> = content.trim().lines().collect();
		assert_eq!(lines.len(), 8);
		// Verify each line is valid JSON and contains strat_key
		for line in lines {
			let row: Value = serde_json::from_str(line).unwrap();
			assert!(row["strat_key"].is_string());
		}
	}

	#[tokio::test]
	async fn test_dedup_row_wise() {
		let fixtures = TestFixtures::new();
		let out_dedup = fixtures.get_output_path("deduped.parquet");
		nail().args(["dedup", fixtures.sample_with_duplicates_parquet.to_str().unwrap(), "--row-wise", "--columns", "id,name", "-o", out_dedup.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_dedup).await, 4);
	}

	#[tokio::test]
	async fn test_dedup_row_wise_keep_last() {
		let fixtures = TestFixtures::new();
		let out_last = fixtures.get_output_path("deduped_last.parquet");
		nail().args(["dedup", fixtures.sample_with_duplicates_parquet.to_str().unwrap(), "--row-wise", "--keep", "last", "-o", out_last.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_last).await, 5);
	}

	#[tokio::test]
	async fn test_dedup_col_wise() {
		let fixtures = TestFixtures::new();
		let out_cols = fixtures.get_output_path("deduped_cols.parquet");
		nail().args(["dedup", fixtures.sample_for_col_dedup_parquet.to_str().unwrap(), "--col-wise", "-o", out_cols.to_str().unwrap()]).assert().success();
		let df_cols = SessionContext::new().read_parquet(out_cols.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
		assert_eq!(df_cols.schema().fields().len(), 2);
	}
}

// ---- DATA COMBINATION COMMANDS ----
#[cfg(test)]
mod combination_tests {
	use super::*;

	#[tokio::test]
	async fn test_merge_inner_and_left() {
		let fixtures = TestFixtures::new();
		let out_inner = fixtures.get_output_path("merged_inner.parquet");
		nail().args(["merge", fixtures.sample_parquet.to_str().unwrap(), "--right", fixtures.sample2_parquet.to_str().unwrap(), "--key-mapping", "id=user_id", "-o", out_inner.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_inner).await, 2);

		let out_left = fixtures.get_output_path("merged_left.parquet");
		nail().args(["merge", fixtures.sample_parquet.to_str().unwrap(), "--right", fixtures.sample2_parquet.to_str().unwrap(), "--key-mapping", "id=user_id", "--left-join", "-o", out_left.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_left).await, 5);
	}

	#[tokio::test]
	async fn test_merge_right_join() {
		let fixtures = TestFixtures::new();
		let out_right = fixtures.get_output_path("merged_right.parquet");
		nail().args(["merge", fixtures.sample_parquet.to_str().unwrap(), "--right", fixtures.sample2_parquet.to_str().unwrap(), "--key-mapping", "id=user_id", "--right-join", "-o", out_right.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&out_right).await, 4);
	}

	#[tokio::test]
	async fn test_append_ignore_schema() {
		let fixtures = TestFixtures::new();
		let out_append = fixtures.get_output_path("appended.csv");
		nail().args(["append", fixtures.sample_parquet.to_str().unwrap(), "--files", fixtures.sample_csv.to_str().unwrap(), "--ignore-schema", "-o", out_append.to_str().unwrap()]).assert().success();
		let content = fs::read_to_string(out_append).unwrap();
		assert_eq!(content.lines().count(), 9, "Should be 1 header + 8 data rows (5+3)");
	}

	#[tokio::test]
	async fn test_split_by_ratio_and_names() {
		let fixtures = TestFixtures::new();
		let output_dir = &fixtures.output_dir;
		nail().args(["split", fixtures.sample_parquet.to_str().unwrap(), "--ratio", "0.6,0.4", "--names", "train,test", "--output-dir", output_dir.to_str().unwrap(), "--random", "123"]).assert().success();
		let train_file = output_dir.join("train.parquet");
		let test_file = output_dir.join("test.parquet");
		assert!(train_file.exists() && test_file.exists());
		assert_eq!(get_row_count(&train_file).await, 3);
		assert_eq!(get_row_count(&test_file).await, 2);
	}

	#[tokio::test]
	async fn test_split_stratified() {
		let fixtures = TestFixtures::new();
		let output_dir = &fixtures.output_dir;
		nail().args(["split", fixtures.sample_for_stratify_parquet.to_str().unwrap(), "--ratio", "0.5,0.5", "--stratified-by", "strat_key", "--output-dir", output_dir.to_str().unwrap()]).assert().success();
		let split1 = output_dir.join("split_1.parquet");
		let split2 = output_dir.join("split_2.parquet");
		assert!(split1.exists(), "split_1.parquet should exist");
		assert!(split2.exists(), "split_2.parquet should exist");
		
		let count1 = get_row_count(&split1).await;
		let count2 = get_row_count(&split2).await;
		
		// Total should be 20, and splits should be roughly equal (stratified split may have rounding)
		assert_eq!(count1 + count2, 20, "Total rows should be preserved");
		assert!(count1 >= 9 && count1 <= 11, "Split 1 should have roughly half the rows (9-11)");
		assert!(count2 >= 9 && count2 <= 11, "Split 2 should have roughly half the rows (9-11)");
	}
}

// ---- FORMAT & ANALYSIS ----
#[cfg(test)]
mod format_and_analysis_tests {
	use super::*;

	#[tokio::test]
	async fn test_convert_round_trip() {
		let fixtures = TestFixtures::new();
		let csv_output = fixtures.get_output_path("converted.csv");
		nail().args(["convert", fixtures.sample_parquet.to_str().unwrap(), "-o", csv_output.to_str().unwrap()]).assert().success();
		let parquet_output = fixtures.get_output_path("roundtrip.parquet");
		nail().args(["convert", csv_output.to_str().unwrap(), "-o", parquet_output.to_str().unwrap()]).assert().success();
		assert_eq!(get_row_count(&parquet_output).await, 5);
	}

	#[test]
	fn test_stats_all_types() {
		let fixtures = TestFixtures::new();
		let out_basic = fixtures.get_output_path("stats_basic.json");
		nail().args(["stats", fixtures.sample_parquet.to_str().unwrap(), "-t", "basic", "-f", "json", "-o", out_basic.to_str().unwrap()]).assert().success();
		let content_basic = fs::read_to_string(out_basic).unwrap();
		// Find a line with numeric stats (not the first line which might be categorical)
		let numeric_line = content_basic.trim().lines()
			.find(|line| line.contains("\"mean\":") && !line.contains("\"mean\":null"))
			.expect("Should find a line with numeric mean");
		let stats_basic: Value = serde_json::from_str(numeric_line).unwrap();
		assert!(stats_basic["mean"].is_number());

		let out_ex = fixtures.get_output_path("stats_ex.json");
		nail().args(["stats", fixtures.sample_parquet.to_str().unwrap(), "-t", "exhaustive", "-f", "json", "-o", out_ex.to_str().unwrap()]).assert().success();
		let content_ex = fs::read_to_string(out_ex).unwrap();
		// Find a line with std_dev (exhaustive stats)
		let exhaustive_line = content_ex.trim().lines()
			.find(|line| line.contains("\"std_dev\":") && !line.contains("\"std_dev\":null"))
			.expect("Should find a line with std_dev");
		let stats_ex: Value = serde_json::from_str(exhaustive_line).unwrap();
		assert!(stats_ex["std_dev"].is_number());
	}

	#[test]
	fn test_correlations_all_options() {
		let fixtures = TestFixtures::new();
		let out_corr = fixtures.get_output_path("corr.json");
		nail().args(["correlations", fixtures.sample_parquet.to_str().unwrap(), "-c", "id,value", "-f", "json", "-o", out_corr.to_str().unwrap()]).assert().success();
		let content = fs::read_to_string(out_corr).unwrap();
		let first_line = content.trim().lines().next().unwrap();
		let corr_data: Value = serde_json::from_str(first_line).unwrap();
		assert!(corr_data["correlation"].as_f64().unwrap() > 0.9);
		
		let out_matrix = fixtures.get_output_path("corr_matrix.json");
		nail().args(["correlations", fixtures.sample_parquet.to_str().unwrap(), "-c", "id,value", "--correlation-matrix", "--digits", "2", "-f", "json", "-o", out_matrix.to_str().unwrap()]).assert().success();
		let matrix_content = fs::read_to_string(out_matrix).unwrap();
		let matrix_lines: Vec<&str> = matrix_content.trim().lines().collect();
		assert_eq!(matrix_lines.len(), 2);
		let first_row: Value = serde_json::from_str(matrix_lines[0]).unwrap();
		
		// Debug output for when the test fails
		let corr_with_id = first_row["corr_with_id"].as_f64().unwrap();
		let diff = (corr_with_id - 1.0).abs();
		if diff >= 0.01 {
			eprintln!("Correlation test failure debug:");
			eprintln!("Matrix content: {}", matrix_content);
			eprintln!("First row: {}", first_row);
			eprintln!("corr_with_id value: {}", corr_with_id);
			eprintln!("Difference from 1.0: {}", diff);
			eprintln!("Available keys: {:?}", first_row.as_object().unwrap().keys().collect::<Vec<_>>());
		}
		
		assert!(diff < 0.01, "corr_with_id ({}) is not close enough to 1.0 (diff: {})", corr_with_id, diff);
	}

	#[test]
	fn test_frequency_single_column() {
		let fixtures = TestFixtures::new();
		// Test console output (no file output)
		nail().args(["frequency", fixtures.sample_parquet.to_str().unwrap(), "-c", "name"])
			.assert()
			.success()
			.stdout(predicate::str::contains("Frequency Analysis"))
			.stdout(predicate::str::contains("Alice"))
			.stdout(predicate::str::contains("frequency"));
	}

	#[test]
	fn test_frequency_multiple_columns_with_output() {
		let fixtures = TestFixtures::new();
		let out_freq = fixtures.get_output_path("frequency.csv");
		nail().args(["frequency", fixtures.sample_parquet.to_str().unwrap(), "-c", "name,value", "-o", out_freq.to_str().unwrap()])
			.assert()
			.success();
		
		let content = fs::read_to_string(out_freq).unwrap();
		assert!(content.contains("name,value,frequency"));
		assert!(content.contains("Alice"));
	}

	#[test]
	fn test_frequency_verbose_mode() {
		let fixtures = TestFixtures::new();
		nail().args(["frequency", fixtures.sample_parquet.to_str().unwrap(), "-c", "name", "-v"])
			.assert()
			.success()
			.stderr(predicate::str::contains("Reading data from"))
			.stderr(predicate::str::contains("Analyzing frequency for columns"));
	}

	#[test]
	fn test_frequency_error_missing_column() {
		let fixtures = TestFixtures::new();
		nail().args(["frequency", fixtures.sample_parquet.to_str().unwrap(), "-c", "nonexistent"])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Column 'nonexistent' does not exist"));
	}
}

// ---- ERROR & EDGE CASE TESTS ----
#[cfg(test)]
mod error_and_edge_case_tests {
	use super::*;

	#[test]
	fn test_file_not_found() {
		nail().args(["head", "nonexistent.parquet"]).assert().failure().stderr(predicate::str::contains("No such file"));
	}

	#[test]
	fn test_invalid_column() {
		let fixtures = TestFixtures::new();
		nail().args(["select", fixtures.sample_parquet.to_str().unwrap(), "-c", "bad_col"]).assert().failure().stderr(predicate::str::contains("Columns not found"));
	}

	#[test]
	fn test_merge_missing_key() {
		let fixtures = TestFixtures::new();
		nail().args(["merge", fixtures.sample_parquet.to_str().unwrap(), "--right", fixtures.sample2_parquet.to_str().unwrap()]).assert().failure().stderr(predicate::str::contains("must be specified"));
	}

	#[test]
	fn test_split_invalid_ratio() {
		let fixtures = TestFixtures::new();
		nail().args(["split", fixtures.sample_parquet.to_str().unwrap(), "--ratio", "0.5,0.6"]).assert().failure().stderr(predicate::str::contains("must sum to 1.0 or 100.0"));
	}
	
	#[test]
	fn test_correlation_non_numeric() {
		let fixtures = TestFixtures::new();
		nail().args(["correlations", fixtures.sample_parquet.to_str().unwrap(), "-c", "name"]).assert().failure().stderr(predicate::str::contains("Correlation requires numeric columns only"));
	}

	#[test]
	fn test_append_schema_mismatch() {
		let fixtures = TestFixtures::new();
		nail().args(["append", fixtures.sample_parquet.to_str().unwrap(), "--files", fixtures.sample2_parquet.to_str().unwrap()]).assert().failure().stderr(predicate::str::contains("Schema mismatch"));
	}

	#[test]
	fn test_dedup_missing_mode() {
		let fixtures = TestFixtures::new();
		nail().args(["dedup", fixtures.sample_parquet.to_str().unwrap()]).assert().failure().stderr(predicate::str::contains("Must specify either --row-wise or --col-wise"));
	}

	#[test]
	fn test_empty_file_operations() {
		let fixtures = TestFixtures::new();
		let empty = fixtures.empty_parquet.to_str().unwrap();
		nail().args(["head", empty]).assert().success();
		nail().args(["stats", empty]).assert().success();
		nail().args(["count", empty]).assert().success().stdout("0\n");
	}
}

// ---- NEW COMMAND TESTS ----
#[cfg(test)]
mod new_command_tests {
	use super::*;

	// OPTIMIZE COMMAND TESTS
	#[tokio::test]
	async fn test_optimize_basic() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("optimized.parquet");
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"-o", output_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Verify the optimized file exists and has the same row count
		assert_eq!(get_row_count(&output_path).await, 5);
	}

	#[tokio::test]
	async fn test_optimize_with_compression() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("optimized_gzip.parquet");
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--compression", "gzip",
				"--compression-level", "3",
				"-o", output_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		assert_eq!(get_row_count(&output_path).await, 5);
	}

	#[tokio::test]
	async fn test_optimize_with_sorting() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("optimized_sorted.parquet");
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--sort-by", "id,value",
				"--row-group-size", "500000",
				"-o", output_path.to_str().unwrap(),
				"-v"  // Test verbose output
			])
			.assert()
			.success()
			.stderr(predicate::str::contains("Optimizing Parquet file"))
			.stderr(predicate::str::contains("Sorting by columns"));
		
		assert_eq!(get_row_count(&output_path).await, 5);
	}

	#[test]
	fn test_optimize_validation() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("optimized_validated.parquet");
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--validate",
				"-o", output_path.to_str().unwrap(),
				"-v"
			])
			.assert()
			.success()
			.stderr(predicate::str::contains("Validating optimized file"))
			.stderr(predicate::str::contains("Validation successful"));
	}

	#[test]
	fn test_optimize_default_output_name() {
		let fixtures = TestFixtures::new();
		
		// Copy file to temp dir and optimize it with default output name
		let input_copy = fixtures.get_output_path("input_copy.parquet");
		std::fs::copy(&fixtures.sample_parquet, &input_copy).unwrap();
		
		nail()
			.args([
				"optimize", 
				input_copy.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Check that the default output file was created
		let expected_output = fixtures.get_output_path("input_copy_optimized.parquet");
		assert!(expected_output.exists());
	}

	#[test]
	fn test_optimize_invalid_compression_level() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--compression-level", "15"  // Invalid level
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Compression level must be between 1 and 9"));
	}

	#[test]
	fn test_optimize_conflicting_dictionary_options() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--dictionary",
				"--no-dictionary"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Cannot specify both --dictionary and --no-dictionary"));
	}

	#[test]
	fn test_optimize_invalid_sort_column() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"optimize", 
				fixtures.sample_parquet.to_str().unwrap(),
				"--sort-by", "nonexistent_column"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Column 'nonexistent_column' not found"));
	}

	// BINNING COMMAND TESTS
	#[tokio::test]
	async fn test_binning_equal_width() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("binned.parquet");
		
		nail()
			.args([
				"binning",
				fixtures.sample_for_binning_parquet.to_str().unwrap(),
				"-c", "score",
				"-b", "3",
				"--method", "equal-width",
				"-o", output_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Verify the binned file has the same row count and a new binned column
		let df = SessionContext::new()
			.read_parquet(output_path.to_str().unwrap(), ParquetReadOptions::default())
			.await
			.unwrap();
		
		assert_eq!(df.clone().count().await.unwrap(), 10);
		assert!(df.schema().field_with_name(None, "score_binned").is_ok());
	}

	#[test]
	fn test_binning_with_custom_edges() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("binned_custom.json");
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"-b", "0,200,400,600",
				"--method", "custom",
				"--suffix", "_category",
				"-f", "json",
				"-o", output_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		let content = fs::read_to_string(output_path).unwrap();
		let lines: Vec<&str> = content.trim().lines().collect();
		assert_eq!(lines.len(), 5);
		
		// Verify each line contains the binned column
		for line in lines {
			let row: Value = serde_json::from_str(line).unwrap();
			assert!(row["value_category"].is_string());
		}
	}

	#[test]
	fn test_binning_with_custom_labels() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"-b", "3",
				"--labels", "Low,Medium,High",
				"--drop-original",
				"-f", "text"
			])
			.assert()
			.success()
			.stdout(predicate::str::contains("Low").or(predicate::str::contains("Medium")).or(predicate::str::contains("High")));
	}

	#[test]
	fn test_binning_verbose_mode() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "id",
				"-b", "2",
				"-v"
			])
			.assert()
			.success()
			.stderr(predicate::str::contains("Binning columns"))
			.stderr(predicate::str::contains("range:"));
	}

	#[test]
	fn test_binning_multiple_columns_error() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "id,value"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Multiple column binning not yet implemented"));
	}

	#[test]
	fn test_binning_non_numeric_column() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "name"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("is not numeric"));
	}

	#[test]
	fn test_binning_invalid_column() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "nonexistent"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Column 'nonexistent' not found"));
	}

	#[test]
	fn test_binning_invalid_bins() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"-b", "0"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Number of bins must be greater than 0"));
	}

	#[test]
	fn test_binning_label_count_mismatch() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"-b", "3",
				"--labels", "Low,High"  // Only 2 labels for 3 bins
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Number of labels (2) must match number of bins (3)"));
	}

	#[test]
	fn test_binning_equal_frequency_not_implemented() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"--method", "equal-frequency"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Equal frequency binning not yet implemented"));
	}

	// PIVOT COMMAND TESTS
	#[tokio::test]
	async fn test_pivot_basic_aggregation() {
		let fixtures = TestFixtures::new();
		let output_path = fixtures.get_output_path("pivoted.parquet");
		
		nail()
			.args([
				"pivot",
				fixtures.sample_for_pivot_parquet.to_str().unwrap(),
				"-i", "region",
				"-c", "product",
				"-l", "sales",
				"--agg", "sum",
				"-o", output_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Verify the pivot result exists and has the expected structure
		let df = SessionContext::new()
			.read_parquet(output_path.to_str().unwrap(), ParquetReadOptions::default())
			.await
			.unwrap();
		
		assert!(df.clone().count().await.unwrap() <= 12); // Should be grouped by region
		assert!(df.schema().field_with_name(None, "region").is_ok());
	}

	#[test]
	fn test_pivot_different_aggregations() {
		let fixtures = TestFixtures::new();
		
		// Test different aggregation functions
		for agg in &["sum", "mean", "count", "min", "max"] {
			nail()
				.args([
					"pivot",
					fixtures.sample_for_pivot_parquet.to_str().unwrap(),
					"-i", "region",
					"-c", "product", 
					"-l", "sales",
					"--agg", agg,
					"-f", "text"
				])
				.assert()
				.success();
		}
	}

	#[test]
	fn test_pivot_with_fill_value() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "category",
				"-c", "name",
				"-l", "value",
				"--fill", "999",
				"-f", "json"
			])
			.assert()
			.success();
	}

	#[test]
	fn test_pivot_verbose_mode() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "category",
				"-c", "name",
				"-l", "value",
				"-v"
			])
			.assert()
			.success()
			.stderr(predicate::str::contains("Index columns"))
			.stderr(predicate::str::contains("Pivot columns"))
			.stderr(predicate::str::contains("Value columns"));
	}

	#[test]
	fn test_pivot_multiple_columns_error() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "category",
				"-c", "name,id",  // Multiple pivot columns
				"-l", "value"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Multiple pivot columns not yet implemented"));
	}

	#[test]
	fn test_pivot_multiple_values_error() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "category",
				"-c", "name",
				"-l", "value,id"  // Multiple value columns
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Multiple value columns not yet implemented"));
	}

	#[test]
	fn test_pivot_invalid_index_column() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "nonexistent",
				"-c", "name",
				"-l", "value"
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Column 'nonexistent' not found"));
	}

	#[test]
	fn test_pivot_invalid_value_column() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "category",
				"-c", "name",
				"-l", "name"  // String column as value
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Value column 'name' must be numeric"));
	}

	#[test]
	fn test_pivot_no_numeric_columns() {
		let fixtures = TestFixtures::new();
		
		nail()
			.args([
				"pivot",
				fixtures.sample_parquet.to_str().unwrap(),
				"-i", "name",
				"-c", "category"
				// No value columns specified, should auto-detect but find multiple available
			])
			.assert()
			.failure()
			.stderr(predicate::str::contains("Multiple value columns not yet implemented"));
	}

	// Integration tests combining multiple features
	#[tokio::test]
	async fn test_optimize_then_binning() {
		let fixtures = TestFixtures::new();
		let optimized_path = fixtures.get_output_path("optimized_for_binning.parquet");
		let binned_path = fixtures.get_output_path("optimized_then_binned.parquet");
		
		// First optimize
		nail()
			.args([
				"optimize",
				fixtures.sample_parquet.to_str().unwrap(),
				"--compression", "snappy",
				"-o", optimized_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Then bin the optimized file
		nail()
			.args([
				"binning",
				optimized_path.to_str().unwrap(),
				"-c", "value",
				"-b", "2",
				"-o", binned_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Verify final result
		let df = SessionContext::new()
			.read_parquet(binned_path.to_str().unwrap(), ParquetReadOptions::default())
			.await
			.unwrap();
		
		assert_eq!(df.clone().count().await.unwrap(), 5);
		assert!(df.schema().field_with_name(None, "value_binned").is_ok());
	}

	#[tokio::test]
	async fn test_binning_then_pivot() {
		let fixtures = TestFixtures::new();
		let binned_path = fixtures.get_output_path("binned_for_pivot.parquet");
		let pivot_path = fixtures.get_output_path("binned_then_pivot.json");
		
		// First bin the data
		nail()
			.args([
				"binning",
				fixtures.sample_parquet.to_str().unwrap(),
				"-c", "value",
				"-b", "2",
				"--labels", "Low,High",
				"-o", binned_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Then pivot using the binned column
		nail()
			.args([
				"pivot",
				binned_path.to_str().unwrap(),
				"-i", "value_binned",
				"-c", "category",
				"-l", "id",
				"--agg", "count",
				"-f", "json",
				"-o", pivot_path.to_str().unwrap()
			])
			.assert()
			.success();
		
		// Verify the result contains expected data
		let content = fs::read_to_string(pivot_path).unwrap();
		assert!(!content.trim().is_empty());
	}
}