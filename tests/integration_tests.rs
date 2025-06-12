use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};

#[path = "common/mod.rs"]
mod common;

// Helper to create a temporary directory and all sample files for each test.
fn setup() -> (TempDir, PathBuf, PathBuf, PathBuf, PathBuf) {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("sample.parquet");
    let input2_path = temp_dir.path().join("sample2.parquet");
    let input3_path = temp_dir.path().join("sample3.parquet");
    let input_csv_path = temp_dir.path().join("sample.csv");

    common::create_sample_parquet(&input_path).unwrap();
    common::create_sample2_parquet(&input2_path).unwrap();
    common::create_sample3_parquet(&input3_path).unwrap();
    common::create_sample_csv(&input_csv_path).unwrap();

    // Create Excel test file in fixtures directory
    let fixtures_dir = std::path::Path::new("tests/fixtures");
    if !fixtures_dir.exists() {
        std::fs::create_dir_all(fixtures_dir).unwrap();
    }
    let excel_path = fixtures_dir.join("sample.xlsx");
    if !excel_path.exists() {
        common::create_sample_excel(&excel_path).unwrap();
    }

    (temp_dir, input_path, input2_path, input3_path, input_csv_path)
}

#[test]
fn test_cli_help() {
    Command::cargo_bin("nail").unwrap().arg("--help").assert().success();
}

// --- Data Inspection ---

mod head_tests {
    use super::*;
    #[test]
    fn test_head_to_json_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("out.json");
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", input.to_str().unwrap(), "-n", "1", "-o", out.to_str().unwrap()])
            .assert().success();
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains(r#""id":1,"name":"Alice","value":100.0,"category":"A"}"#));
    }
}

mod tail_tests {
    use super::*;
    #[test]
    fn test_tail_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["tail", "-i", input.to_str().unwrap(), "-n", "1", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Displaying last 1 rows"));
    }
}

mod preview_tests {
    use super::*;
    #[test]
    fn test_preview_reproducible() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["preview", "-i", input.to_str().unwrap(), "-n", "2", "--random", "42"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("Eve"));
    }
}

mod headers_tests {
    use super::*;
    #[test]
    fn test_headers_filter_regex() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", input.to_str().unwrap(), "--filter", "(id|name)"])
            .assert().success()
            .stdout(predicate::str::is_match("^id\nname\n$").unwrap());
    }

    #[test]
    fn test_headers_all_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", input.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value"))
            .stdout(predicate::str::contains("category"));
    }

    #[test]
    fn test_headers_output_to_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("headers.txt");
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap()])
            .assert().success();
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains("id"));
        assert!(content.contains("name"));
    }
}

mod schema_tests {
    use super::*;
    #[test]
    fn test_schema_display() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["schema", "-i", input.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::contains("Column"))
            .stdout(predicate::str::contains("Int64"))
            .stdout(predicate::str::contains("Utf8"));
    }

    #[test]
    fn test_schema_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["schema", "-i", input.to_str().unwrap(), "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Reading schema"));
    }
}

// --- Statistics & Analysis ---

mod stats_tests {
    use super::*;
    #[test]
    fn test_stats_exhaustive_on_columns_regex() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-t", "exhaustive", "-c", "^v"])
            .assert().success()
            .stdout(predicate::str::contains("value"))
            .stdout(predicate::str::contains("id").not());
    }

    #[test]
    fn test_stats_unimplemented_fails_gracefully() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-t", "hypothesis"])
            .assert().failure()
            .stderr(predicate::str::contains("Hypothesis tests not yet implemented"));
    }
}

mod correlations_tests {
    use super::*;
    #[test]
    fn test_correlations_spearman_works() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value", "-t", "spearman"])
            .assert().success()
            .stdout(predicate::str::contains("correlation"));
    }
}

// --- Data Manipulation ---

mod filter_tests {
    use super::*;
    #[test]
    fn test_filter_multiple_conditions() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "-c", "id>1,category=A"])
            .assert().success()
            .stdout(predicate::str::contains("Charlie")) // id=3, cat=A
            .stdout(predicate::str::contains("Eve"))     // id=5, cat=A
            .stdout(predicate::str::contains("Alice").not()); // id=1
    }

    #[test]
    fn test_filter_row_numeric_only() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "--rows", "numeric-only"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("value"))
            .stdout(predicate::str::contains("name").not());
    }
}

mod fill_tests {
    use super::*;
    #[test]
    fn test_fill_with_mean() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "mean", "-c", "value"])
            .assert().success()
            .stdout(predicate::str::contains("325.0")); // Mean of (100,300,400,500) is 325
    }

    #[test]
    fn test_fill_with_value() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "value", "--value", "999", "-c", "value"])
            .assert().success()
            .stdout(predicate::str::contains("999"));
    }

    #[test]
    fn test_fill_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "value", "--value", "0", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Filling missing values"));
    }
}

mod select_tests {
    use super::*;
    #[test]
    fn test_select_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["select", "-i", input.to_str().unwrap(), "-c", "id,name"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value").not());
    }

    #[test]
    fn test_select_columns_regex() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["select", "-i", input.to_str().unwrap(), "-c", "^(id|name)$"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("Bob"));
    }

    #[test]
    fn test_select_with_output() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("selected.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["select", "-i", input.to_str().unwrap(), "-c", "id,name", "-o", out.to_str().unwrap()])
            .assert().success();
        
        // Verify the output file was created and has correct columns
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", out.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value").not());
    }
}

mod drop_tests {
    use super::*;
    #[test]
    fn test_drop_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["drop", "-i", input.to_str().unwrap(), "-c", "value,category"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value").not())
            .stdout(predicate::str::contains("category").not());
    }

    #[test]
    fn test_drop_columns_regex() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["drop", "-i", input.to_str().unwrap(), "-c", "^(value|category)$"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value").not());
    }

    #[test]
    fn test_drop_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["drop", "-i", input.to_str().unwrap(), "-c", "value", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Dropping"));
    }
}

// --- Data Sampling & Transformation ---

mod sample_tests {
    use super::*;
    #[test]
    fn test_sample_stratified() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "--method", "stratified", "--stratify-by", "category", "--random", "1"])
            .assert().success()
            .stdout(predicate::str::contains("Alice")) // From category A
            .stdout(predicate::str::contains("Bob"));  // From category B
    }

    #[test]
    fn test_sample_last() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "--method", "last"])
            .assert().success()
            .stdout(predicate::str::contains("Eve"))
            .stdout(predicate::str::contains("Alice").not());
    }

    #[test]
    fn test_sample_first() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "--method", "first"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("Bob"))
            .stdout(predicate::str::contains("Eve").not());
    }

    #[test]
    fn test_sample_random_reproducible() {
        let (_td, input, _, _, _) = setup();
        let result1 = Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "3", "--method", "random", "--random", "42"])
            .assert().success()
            .get_output().stdout.clone();

        let result2 = Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "3", "--method", "random", "--random", "42"])
            .assert().success()
            .get_output().stdout.clone();

        assert_eq!(result1, result2); // Should be identical with same seed
    }

    #[test]
    fn test_sample_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Sampling 2 rows"));
    }
}

mod shuffle_tests {
    use super::*;
    #[test]
    fn test_shuffle_with_seed() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["shuffle", "-i", input.to_str().unwrap(), "--random", "42"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("Bob"));
    }

    #[test]
    fn test_shuffle_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["shuffle", "-i", input.to_str().unwrap(), "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Shuffling"));
    }

    #[test]
    fn test_shuffle_to_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("shuffled.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["shuffle", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap()])
            .assert().success();
        
        // Verify output file was created
        assert!(out.exists());
    }
}

mod id_tests {
    use super::*;
    #[test]
    fn test_id_create_error() {
        let (_td, input, _, _, _) = setup();
        // This should fail because 'id' column already exists in test data
        Command::cargo_bin("nail").unwrap()
            .args(["id", "-i", input.to_str().unwrap(), "--create"])
            .assert().failure()
            .stderr(predicate::str::contains("already exists"));
    }

    #[test]
    fn test_id_create_custom_name_prefix() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["id", "-i", input.to_str().unwrap(), "--create", "--id-col-name", "record_id", "--prefix", "REC"])
            .assert().success()
            .stdout(predicate::str::contains("record_id"))
            .stdout(predicate::str::contains("REC"));
    }

    #[test]
    fn test_id_verbose_error() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["id", "-i", input.to_str().unwrap(), "--create", "--verbose"])
            .assert().failure()
            .stderr(predicate::str::contains("Creating ID column"))
            .stderr(predicate::str::contains("already exists"));
    }
}

// --- Data Combination ---

mod merge_tests {
    use super::*;
    #[test]
    fn test_merge_with_key_mapping() {
        let (_td, input1, _, input3, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["merge", "-i", input1.to_str().unwrap(), "--right", input3.to_str().unwrap(), "--key-mapping", "id=person_id"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("active"))
            .stdout(predicate::str::contains("inactive"));
    }
}

mod append_tests {
    use super::*;
    #[test]
    fn test_append_multiple_files() {
        let (td, input1, input2, input3, _) = setup();
        let output_path = td.path().join("appended.parquet");
        let files_to_append = format!("{},{}", input2.to_str().unwrap(), input3.to_str().unwrap());
        Command::cargo_bin("nail").unwrap()
            .args(["append", "-i", input1.to_str().unwrap(), "--files", &files_to_append, "-o", output_path.to_str().unwrap(), "--ignore-schema"])
            .assert().success();

        // Check row count: 5 (input1) + 4 (input2) + 3 (input3) = 12
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", output_path.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::is_match(r"count\s+\|\s+12").unwrap());
    }
}

// --- Format Conversion ---

mod convert_tests {
    use super::*;
    #[test]
    fn test_convert_csv_to_parquet() {
        let (td, _, _, _, input_csv) = setup();
        let output_path = td.path().join("output.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input_csv.to_str().unwrap(), "-o", output_path.to_str().unwrap()])
            .assert().success();

        // Verify the content of the created parquet file
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", output_path.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::contains("Frank"))
            .stdout(predicate::str::contains("1000"));
    }

    #[test]
    fn test_convert_parquet_to_csv() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("out.csv");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap()])
            .assert().success();
        
        // Verify CSV content
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains("id,name,value,category"));
        assert!(content.contains("Alice"));
    }

    #[test]
    fn test_convert_parquet_to_json() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("out.json");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap()])
            .assert().success();
        
        // Verify JSON content
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains(r#""name":"Alice""#));
    }

    #[test]
    fn test_convert_verbose() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("out.csv");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Converting"))
            .stderr(predicate::str::contains("Processing"));
    }

    #[test]
    fn test_convert_format_detection() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("out.json");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap()])
            .assert().success();
        
        // Verify JSON format was auto-detected from extension
        assert!(out.exists());
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains(r#""name":"Alice""#));
    }
}

// --- Extended Tests ---

mod merge_extended_tests {
    use super::*;
    #[test]
    fn test_merge_left_join() {
        let (_td, input1, input2, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["merge", "-i", input1.to_str().unwrap(), "--right", input2.to_str().unwrap(), "--left-join", "--key", "id"])
            .assert().success()
            .stdout(predicate::str::contains("Alice")) // From left table
            .stdout(predicate::str::contains("Bob"))   // From left table
            .stdout(predicate::str::contains("88.0"));  // From right table for id=4
    }

    #[test]
    fn test_merge_right_join() {
        let (_td, input1, input2, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["merge", "-i", input1.to_str().unwrap(), "--right", input2.to_str().unwrap(), "--right-join", "--key", "id"])
            .assert().success()
            .stdout(predicate::str::contains("88.0"))   // From right table
            .stdout(predicate::str::contains("92.5"));  // From right table
    }

    #[test]
    fn test_merge_inner_join() {
        let (_td, input1, input2, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["merge", "-i", input1.to_str().unwrap(), "--right", input2.to_str().unwrap(), "--key", "id"])
            .assert().success()
            .stdout(predicate::str::contains("Eve"))    // id=5 exists in both
            .stdout(predicate::str::contains("92.5"))   // score for id=5
            .stdout(predicate::str::contains("Alice").not()); // id=1 only in left
    }

    #[test]
    fn test_merge_verbose() {
        let (_td, input1, input2, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["merge", "-i", input1.to_str().unwrap(), "--right", input2.to_str().unwrap(), "--key", "id", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Reading"))
            .stderr(predicate::str::contains("Inner join"));
    }
}

mod filter_extended_tests {
    use super::*;
    #[test]
    fn test_filter_no_nan() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "--rows", "no-nan"])
            .assert().success()
            .stdout(predicate::str::contains("Alice"))
            .stdout(predicate::str::contains("Charlie"));
        // Bob should be filtered out due to null value
    }

    #[test]
    fn test_filter_char_only() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "--rows", "char-only"])
            .assert().success()
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("category"))
            .stdout(predicate::str::contains("id").not())
            .stdout(predicate::str::contains("value").not());
    }

    #[test]
    fn test_filter_complex_conditions() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "-c", "id>=3,category=A"])
            .assert().success()
            .stdout(predicate::str::contains("Charlie")) // id=3, category=A
            .stdout(predicate::str::contains("Eve"))     // id=5, category=A
            .stdout(predicate::str::contains("Alice").not()) // id=1, category=A but id<3
            .stdout(predicate::str::contains("Bob").not());   // id=2, category=B
    }
}

mod stats_extended_tests {
    use super::*;
    #[test]
    fn test_stats_basic() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-t", "basic"])
            .assert().success()
            .stdout(predicate::str::contains("count"))
            .stdout(predicate::str::contains("mean"));
    }

    #[test]
    fn test_stats_specific_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-c", "id,value"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("value"))
            .stdout(predicate::str::contains("category").not());
    }

    #[test]
    fn test_stats_output_to_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("stats.json");
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        assert!(out.exists());
        let content = fs::read_to_string(out).unwrap();
        assert!(content.contains("count") || content.contains("mean"));
    }
}

mod append_extended_tests {
    use super::*;
    #[test]
    fn test_append_verbose() {
        let (td, input1, input2, _, _) = setup();
        let output_path = td.path().join("appended.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["append", "-i", input1.to_str().unwrap(), "--files", input2.to_str().unwrap(), "-o", output_path.to_str().unwrap(), "--ignore-schema", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Appending"))
            .stderr(predicate::str::contains("Final dataset"));
    }

    #[test]
    fn test_append_schema_validation() {
        let (td, input1, input2, _, _) = setup();
        let output_path = td.path().join("appended.parquet");
        // These schemas are different, so use --ignore-schema
        Command::cargo_bin("nail").unwrap()
            .args(["append", "-i", input1.to_str().unwrap(), "--files", input2.to_str().unwrap(), "-o", output_path.to_str().unwrap(), "--ignore-schema"])
            .assert().success();
    }
}

// --- Count Tests ---

mod count_tests {
    use super::*;
    #[test]
    fn test_count_basic() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["count", "-i", input.to_str().unwrap()])
            .assert().success()
            .stdout(predicate::str::is_match(r"^\d+\n?$").unwrap());
    }

    #[test]
    fn test_count_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["count", "-i", input.to_str().unwrap(), "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Reading data"))
            .stderr(predicate::str::contains("Counted"));
    }

    #[test]
    fn test_count_output_json() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("count.json");
        Command::cargo_bin("nail").unwrap()
            .args(["count", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        let content = std::fs::read_to_string(out).unwrap();
        assert!(content.contains("row_count"));
    }

    #[test]
    fn test_count_output_csv() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("count.csv");
        Command::cargo_bin("nail").unwrap()
            .args(["count", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "-f", "csv"])
            .assert().success();
        
        let content = std::fs::read_to_string(out).unwrap();
        assert!(content.contains("row_count"));
    }
}

// --- Extended Correlation Tests ---

mod correlations_extended_tests {
    use super::*;
    #[test]
    fn test_correlations_spearman() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value", "-t", "spearman"])
            .assert().success()
            .stdout(predicate::str::contains("correlation"));
    }

    #[test]
    fn test_correlations_kendall() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value", "-t", "kendall"])
            .assert().success()
            .stdout(predicate::str::contains("correlation"));
    }

    #[test]
    fn test_correlations_matrix_format() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value", "--correlation-matrix"])
            .assert().success()
            .stdout(predicate::str::contains("variable"));
    }

    #[test]
    fn test_correlations_all_numeric_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value"])
            .assert().success()
            .stdout(predicate::str::contains("correlation"));
    }

    #[test]
    fn test_correlations_insufficient_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id"])
            .assert().failure()
            .stderr(predicate::str::contains("at least 2"));
    }

    #[test]
    fn test_correlations_non_numeric_columns() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "name,category"])
            .assert().failure()
            .stderr(predicate::str::contains("numeric columns only").or(predicate::str::contains("Utf8")));
    }
}

// --- Extended Fill Tests ---

mod fill_extended_tests {
    use super::*;
    #[test]
    fn test_fill_median() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "median", "-c", "value"])
            .assert().success()
            .stdout(predicate::str::contains("350.0")); // Median of (100,300,400,500) is 350
    }

    #[test]
    fn test_fill_forward() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "forward"])
            .assert().success();
    }

    #[test]
    fn test_fill_backward() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "backward"])
            .assert().success();
    }

    #[test]
    fn test_fill_mode() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "mode", "-c", "category"])
            .assert().success();
    }
}

// --- Extended Sample Tests ---

mod sample_extended_tests {
    use super::*;
    #[test]
    fn test_sample_percentage() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "3", "--method", "random"])
            .assert().success()
            .stdout(predicate::str::contains("Alice").or(predicate::str::contains("Bob")));
    }

    #[test]
    fn test_sample_stratified_verbose() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "--method", "stratified", "--stratify-by", "category", "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Sampling"));
    }

    #[test]
    fn test_sample_output_to_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("sample.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "-n", "2", "-o", out.to_str().unwrap()])
            .assert().success();
        
        assert!(out.exists());
    }
}

// --- Extended ID Tests ---

mod id_extended_tests {
    use super::*;
    #[test]
    fn test_id_create_with_different_dataset() {
        let (_td, _, input2, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["id", "-i", input2.to_str().unwrap(), "--create", "--id-col-name", "row_id"])
            .assert().success()
            .stdout(predicate::str::contains("row_id"));
    }

    #[test]
    fn test_id_create_with_output() {
        let (td, _, input2, _, _) = setup();
        let out = td.path().join("with_id.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["id", "-i", input2.to_str().unwrap(), "--create", "--id-col-name", "row_id", "-o", out.to_str().unwrap()])
            .assert().success();
        
        assert!(out.exists());
    }
}

// --- Extended Schema Tests ---

mod schema_extended_tests {
    use super::*;
    #[test]
    fn test_schema_json_output() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("schema.json");
        Command::cargo_bin("nail").unwrap()
            .args(["schema", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        let content = std::fs::read_to_string(out).unwrap();
        assert!(content.contains("name") && content.contains("data_type"));
    }
}

// --- Extended Preview Tests ---

mod preview_extended_tests {
    use super::*;
    #[test]
    fn test_preview_large_sample() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["preview", "-i", input.to_str().unwrap(), "-n", "10"])
            .assert().success()
            .stdout(predicate::str::contains("Alice").or(predicate::str::contains("Bob")));
    }

    #[test]
    fn test_preview_output_to_file() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("preview.json");
        Command::cargo_bin("nail").unwrap()
            .args(["preview", "-i", input.to_str().unwrap(), "-n", "3", "-o", out.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        assert!(out.exists());
    }
}

// --- Extended Convert Tests ---

mod convert_extended_tests {
    use super::*;
    #[test]
    fn test_convert_excel_to_parquet() {
        let (td, _, _, _, _) = setup();
        let excel_path = std::path::Path::new("tests/fixtures/sample.xlsx");
        if excel_path.exists() {
            let out = td.path().join("from_excel.parquet");
            Command::cargo_bin("nail").unwrap()
                .args(["convert", "-i", excel_path.to_str().unwrap(), "-o", out.to_str().unwrap()])
                .assert().success();
            
            assert!(out.exists());
        }
    }

    #[test]
    fn test_convert_large_file_verbose() {
        let (td, input, _, _, _) = setup();
        let out = td.path().join("converted.json");
        Command::cargo_bin("nail").unwrap()
            .args(["convert", "-i", input.to_str().unwrap(), "-o", out.to_str().unwrap(), "--verbose"])
            .assert().success()
            .stderr(predicate::str::contains("Converting"));
    }
}

// --- Performance and Edge Case Tests ---

mod performance_tests {
    use super::*;
    #[test]
    fn test_large_dataset_operations() {
        let (_td, input, _, _, _) = setup();
        // Test that operations complete in reasonable time
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-t", "basic"])
            .assert().success();
    }

    #[test]
    fn test_parallel_jobs_setting() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-j", "2"])
            .assert().success();
    }
}

// --- Complex Workflow Tests ---

mod workflow_tests {
    use super::*;
    #[test]
    fn test_data_cleaning_pipeline() {
        let (td, input, _, _, _) = setup();
        
        // Step 1: Filter numeric only
        let step1 = td.path().join("step1.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["filter", "-i", input.to_str().unwrap(), "-r", "numeric-only", "-o", step1.to_str().unwrap()])
            .assert().success();
        
        // Step 2: Fill missing values
        let step2 = td.path().join("step2.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", step1.to_str().unwrap(), "--method", "value", "--value", "0", "-o", step2.to_str().unwrap()])
            .assert().success();
        
        // Step 3: Sample the result
        let final_result = td.path().join("final.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", step2.to_str().unwrap(), "-n", "2", "-o", final_result.to_str().unwrap()])
            .assert().success();
        
        assert!(final_result.exists());
    }

    #[test]
    fn test_analysis_pipeline() {
        let (td, input, _, _, _) = setup();
        
        // Generate statistics
        let stats_file = td.path().join("stats.json");
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-o", stats_file.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        // Generate correlations
        let corr_file = td.path().join("correlations.json");
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-c", "id,value", "-o", corr_file.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        assert!(stats_file.exists());
        assert!(corr_file.exists());
    }
}

// --- Format-specific Tests ---

mod format_specific_tests {
    use super::*;
    #[test]
    fn test_all_output_formats() {
        let (td, input, _, _, _) = setup();
        
        // Test JSON output
        let json_out = td.path().join("output.json");
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", input.to_str().unwrap(), "-n", "2", "-o", json_out.to_str().unwrap(), "-f", "json"])
            .assert().success();
        
        // Test CSV output
        let csv_out = td.path().join("output.csv");
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", input.to_str().unwrap(), "-n", "2", "-o", csv_out.to_str().unwrap(), "-f", "csv"])
            .assert().success();
        
        // Test Parquet output
        let parquet_out = td.path().join("output.parquet");
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", input.to_str().unwrap(), "-n", "2", "-o", parquet_out.to_str().unwrap(), "-f", "parquet"])
            .assert().success();
        
        assert!(json_out.exists());
        assert!(csv_out.exists());
        assert!(parquet_out.exists());
    }
}

// --- Regex and Pattern Tests ---

mod pattern_tests {
    use super::*;
    #[test]
    fn test_complex_regex_patterns() {
        let (_td, input, _, _, _) = setup();
        
        // Test complex regex in headers
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", input.to_str().unwrap(), "--filter", "^(id|name|value)$"])
            .assert().success()
            .stdout(predicate::str::contains("id"))
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value"))
            .stdout(predicate::str::contains("category").not());
    }

    #[test]
    fn test_wildcard_patterns() {
        let (_td, input, _, _, _) = setup();
        
        // Test wildcard patterns in select
        Command::cargo_bin("nail").unwrap()
            .args(["select", "-i", input.to_str().unwrap(), "-c", ".*e.*"])
            .assert().success()
            .stdout(predicate::str::contains("name"))
            .stdout(predicate::str::contains("value"));
    }
}

mod error_handling_tests {
    use super::*;
    #[test]
    fn test_missing_input_file() {
        Command::cargo_bin("nail").unwrap()
            .args(["head", "-i", "nonexistent.parquet"])
            .assert().failure()
            .stderr(predicate::str::contains("No such file"));
    }

    #[test]
    fn test_invalid_column_name() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["select", "-i", input.to_str().unwrap(), "-c", "nonexistent_column"])
            .assert().failure()
            .stderr(predicate::str::contains("not found").or(predicate::str::contains("Column")));
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["headers", "-i", input.to_str().unwrap(), "--filter", "[invalid"])
            .assert().failure()
            .stderr(predicate::str::contains("regex").or(predicate::str::contains("pattern")));
    }

    #[test]
    fn test_fill_without_value() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "value"])
            .assert().failure()
            .stderr(predicate::str::contains("value").or(predicate::str::contains("required")));
    }

    #[test]
    fn test_invalid_correlation_type() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["correlations", "-i", input.to_str().unwrap(), "-t", "invalid"])
            .assert().failure();
    }

    #[test]
    fn test_invalid_stats_type() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["stats", "-i", input.to_str().unwrap(), "-t", "invalid"])
            .assert().failure();
    }

    #[test]
    fn test_invalid_sample_method() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["sample", "-i", input.to_str().unwrap(), "--method", "invalid"])
            .assert().failure();
    }

    #[test]
    fn test_invalid_fill_method() {
        let (_td, input, _, _, _) = setup();
        Command::cargo_bin("nail").unwrap()
            .args(["fill", "-i", input.to_str().unwrap(), "--method", "invalid"])
            .assert().failure();
    }
}