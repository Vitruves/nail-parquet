// File: tests/performance.rs

use assert_cmd::Command;
use std::time::Instant;

mod common;
use common::TestFixtures;

fn nail() -> Command {
    Command::cargo_bin("nail").unwrap()
}

#[cfg(test)]
mod performance_tests {
    use super::*;

    #[test]
    fn test_count_performance_parquet() {
        let fixtures = TestFixtures::new();
        
        // Time the count operation
        let start = Instant::now();
        let assert_result = nail()
            .args(["count", fixtures.sample_parquet.to_str().unwrap(), "--verbose"])
            .assert()
            .success();
        let output = assert_result.get_output();
        let duration = start.elapsed();
        
        // Verify it uses fast metadata
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(stderr.contains("Using fast Parquet metadata"));
        
        // Should be reasonably fast (less than 5 seconds for debug build)
        assert!(duration.as_millis() < 5000, "Count took too long: {:?}", duration);
        
        // Verify correct count
        let stdout = String::from_utf8(output.stdout.clone()).unwrap();
        let count: u64 = stdout.trim().parse().unwrap();
        assert!(count > 0, "Count should be positive");
    }

    #[test]
    fn test_tail_performance_parquet() {
        let fixtures = TestFixtures::new();
        
        // Time the tail operation
        let start = Instant::now();
        let assert_result = nail()
            .args(["tail", fixtures.sample_parquet.to_str().unwrap(), "--verbose"])
            .assert()
            .success();
        let output = assert_result.get_output();
        let duration = start.elapsed();
        
        // Verify it uses fast metadata
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(stderr.contains("Total rows (from metadata)"));
        
        // Should be reasonably fast (less than 5 seconds for debug build)
        assert!(duration.as_millis() < 5000, "Tail took too long: {:?}", duration);
        
        // Verify output contains records
        let stdout = String::from_utf8(output.stdout.clone()).unwrap();
        assert!(stdout.contains("Record"), "Should contain record output");
    }

    #[test]
    fn test_count_fallback_csv() {
        let fixtures = TestFixtures::new();
        
        // Test with CSV file (should use fallback)
        let assert_result = nail()
            .args(["count", fixtures.sample_csv.to_str().unwrap(), "--verbose"])
            .assert()
            .success();
        let output = assert_result.get_output();
        
        // Should use DataFusion fallback for CSV
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(!stderr.contains("Using fast Parquet metadata"));
        
        // Verify correct count
        let stdout = String::from_utf8(output.stdout.clone()).unwrap();
        let count: u64 = stdout.trim().parse().unwrap();
        assert!(count > 0, "Count should be positive");
    }

    #[test]
    fn test_parquet_utils_detection() {
        let fixtures = TestFixtures::new();
        
        // Test that parquet files are detected correctly
        let assert_result = nail()
            .args(["tail", fixtures.sample_parquet.to_str().unwrap(), "--verbose"])
            .assert()
            .success();
        let output = assert_result.get_output();
        
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(stderr.contains("Total rows (from metadata)"));
        
        // Test that non-parquet files use fallback
        let assert_result = nail()
            .args(["tail", fixtures.sample_csv.to_str().unwrap(), "--verbose"])
            .assert()
            .success();
        let output = assert_result.get_output();
        
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(!stderr.contains("Total rows (from metadata)"));
    }

    #[test]
    fn test_session_context_optimization() {
        let fixtures = TestFixtures::new();
        
        // Test that operations complete successfully with optimized context
        nail()
            .args(["head", fixtures.sample_parquet.to_str().unwrap(), "-n", "10"])
            .assert()
            .success();
        
        nail()
            .args(["tail", fixtures.sample_parquet.to_str().unwrap(), "-n", "10"])
            .assert()
            .success();
        
        nail()
            .args(["count", fixtures.sample_parquet.to_str().unwrap()])
            .assert()
            .success();
    }
}
