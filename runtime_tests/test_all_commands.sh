#!/bin/bash

# Test script for all nail commands
# This script tests the globally installed nail command with comprehensive test cases

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test data and directories
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DATA="$BASE_DIR/test_data.csv"
TEMP_DIR="$BASE_DIR/temp"
RESULTS_DIR="$BASE_DIR/results"

# Create necessary directories
mkdir -p "$TEMP_DIR" "$RESULTS_DIR"

# Logging function
log() {
    echo -e "${BLUE}$(date '+%H:%M:%S')${NC} - $1"
}

success() {
    echo -e "${GREEN}$(date '+%H:%M:%S')${NC} - SUCCESS: $1"
}

warning() {
    echo -e "${YELLOW}$(date '+%H:%M:%S')${NC} - WARNING: $1"
}

error() {
    echo -e "${RED}$(date '+%H:%M:%S')${NC} - ERROR: $1"
}

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a test
run_test() {
    local test_name="$1"
    local command="$2"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log "Running test: $test_name"
    
    if eval "$command" > /dev/null 2>&1; then
        success "$test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        error "$test_name"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Function to check if nail is installed
check_nail_installation() {
    if ! command -v nail &> /dev/null; then
        error "nail command not found. Please install nail globally first."
        echo "Run: cargo install --path . (from the nail project directory)"
        exit 1
    fi
    success "nail command found: $(which nail)"
}

# Function to convert test data to parquet for parquet-specific tests
setup_test_files() {
    log "Setting up test files..."
    
    # Convert CSV to Parquet for parquet-specific tests
    nail convert "$TEST_DATA" -o "$TEMP_DIR/test_data.parquet"
    
    # Create additional test files
    nail head "$TEST_DATA" -n 10 -o "$TEMP_DIR/small_sample.csv"
    nail convert "$TEMP_DIR/small_sample.csv" -o "$TEMP_DIR/small_sample.parquet"
    
    # Create JSON version
    nail convert "$TEST_DATA" -o "$TEMP_DIR/test_data.json"
    
    success "Test files created"
}

echo "=========================================="
echo "       NAIL COMMAND COMPREHENSIVE TEST"
echo "=========================================="

# Check installation
check_nail_installation

# Setup test files
setup_test_files

log "Starting comprehensive test suite..."

# ==========================================
# DATA INSPECTION COMMANDS
# ==========================================

echo -e "\n${BLUE}=== DATA INSPECTION COMMANDS ===${NC}"

# nail head
run_test "head - basic" "nail head '$TEST_DATA' -n 5"
run_test "head - with output" "nail head '$TEST_DATA' -n 3 -o '$TEMP_DIR/head_output.csv'"
run_test "head - verbose" "nail head '$TEST_DATA' -n 2 --verbose"
run_test "head - JSON output" "nail head '$TEST_DATA' -n 5 -f json -o '$TEMP_DIR/head_output.json'"

# nail tail
run_test "tail - basic" "nail tail '$TEST_DATA' -n 5"
run_test "tail - with output" "nail tail '$TEST_DATA' -n 3 -o '$TEMP_DIR/tail_output.csv'"
run_test "tail - verbose" "nail tail '$TEST_DATA' -n 2 --verbose"

# nail preview
run_test "preview - basic" "nail preview '$TEST_DATA' -n 5"
run_test "preview - with seed" "nail preview '$TEST_DATA' -n 3 --random 42"
run_test "preview - output to file" "nail preview '$TEST_DATA' -n 5 -o '$TEMP_DIR/preview_output.csv'"

# nail headers
run_test "headers - basic" "nail headers '$TEST_DATA'"
run_test "headers - with filter" "nail headers '$TEST_DATA' --filter '^(id|name).*'"
run_test "headers - JSON output" "nail headers '$TEST_DATA' -f json -o '$TEMP_DIR/headers.json'"

# nail schema
run_test "schema - basic" "nail schema '$TEST_DATA'"
run_test "schema - verbose" "nail schema '$TEST_DATA' --verbose"
run_test "schema - JSON output" "nail schema '$TEST_DATA' -f json -o '$TEMP_DIR/schema.json'"

# nail metadata (parquet specific)
run_test "metadata - basic" "nail metadata '$TEMP_DIR/test_data.parquet'"
run_test "metadata - all" "nail metadata '$TEMP_DIR/test_data.parquet' --all"
run_test "metadata - detailed" "nail metadata '$TEMP_DIR/test_data.parquet' --schema --row-groups --detailed"

# nail size
run_test "size - basic" "nail size '$TEST_DATA'"
run_test "size - with columns" "nail size '$TEST_DATA' --columns"
run_test "size - with rows" "nail size '$TEST_DATA' --rows"
run_test "size - comprehensive" "nail size '$TEST_DATA' --columns --rows"

# nail search
run_test "search - basic value" "nail search '$TEST_DATA' --value 'Alice'"
run_test "search - specific columns" "nail search '$TEST_DATA' --value 'Electronics' -c 'category'"
run_test "search - case insensitive" "nail search '$TEST_DATA' --value 'ACTIVE' --ignore-case"
run_test "search - exact match" "nail search '$TEST_DATA' --value 'active' --exact"
run_test "search - rows only" "nail search '$TEST_DATA' --value 'Books' --rows"

# ==========================================
# DATA QUALITY TOOLS
# ==========================================

echo -e "\n${BLUE}=== DATA QUALITY TOOLS ===${NC}"

# nail outliers
run_test "outliers - IQR method" "nail outliers '$TEST_DATA' -c 'price' --method iqr"
run_test "outliers - Z-score method" "nail outliers '$TEST_DATA' -c 'age' --method z-score"
run_test "outliers - show values" "nail outliers '$TEST_DATA' -c 'price' --method iqr --show-values"

# ==========================================
# STATISTICS & ANALYSIS
# ==========================================

echo -e "\n${BLUE}=== STATISTICS & ANALYSIS ===${NC}"

# nail stats
run_test "stats - basic" "nail stats '$TEST_DATA'"
run_test "stats - specific columns" "nail stats '$TEST_DATA' -c 'price,age,score'"
run_test "stats - exhaustive" "nail stats '$TEST_DATA' --stats-type exhaustive"
run_test "stats - output to file" "nail stats '$TEST_DATA' -o '$TEMP_DIR/stats.json'"

# nail correlations
run_test "correlations - basic" "nail correlations '$TEST_DATA'"
run_test "correlations - specific columns" "nail correlations '$TEST_DATA' -c 'price,age,score'"
run_test "correlations - kendall" "nail correlations '$TEST_DATA' --correlation-type kendall"
run_test "correlations - matrix format" "nail correlations '$TEST_DATA' --correlation-matrix"

# nail frequency
run_test "frequency - single column" "nail frequency '$TEST_DATA' -c 'category'"
run_test "frequency - multiple columns" "nail frequency '$TEST_DATA' -c 'category,status'"
run_test "frequency - output to file" "nail frequency '$TEST_DATA' -c 'region' -o '$TEMP_DIR/frequency.csv'"

# ==========================================
# DATA MANIPULATION
# ==========================================

echo -e "\n${BLUE}=== DATA MANIPULATION ===${NC}"

# nail select
run_test "select - columns" "nail select '$TEST_DATA' -c 'id,name,price'"
run_test "select - rows" "nail select '$TEST_DATA' -r '1,3,5-10'"
run_test "select - columns and rows" "nail select '$TEST_DATA' -c 'id,name' -r '1-5' -o '$TEMP_DIR/selected.csv'"

# nail drop
run_test "drop - columns" "nail drop '$TEST_DATA' -c 'description' -o '$TEMP_DIR/dropped_cols.csv'"
run_test "drop - rows" "nail drop '$TEST_DATA' -r '1,3,5' -o '$TEMP_DIR/dropped_rows.csv'"

# nail filter
run_test "filter - column conditions" "nail filter '$TEST_DATA' -c 'price>100,age<40' -o '$TEMP_DIR/filtered.csv'"
run_test "filter - numeric only" "nail filter '$TEST_DATA' --rows numeric-only -o '$TEMP_DIR/numeric_only.csv'"
run_test "filter - no NaN" "nail filter '$TEST_DATA' --rows no-nan -o '$TEMP_DIR/no_nan.csv'"

# nail fill
run_test "fill - with value" "nail fill '$TEST_DATA' --method value --value 0 -o '$TEMP_DIR/filled.csv'"
run_test "fill - with mean" "nail fill '$TEST_DATA' -c 'price,age,score' --method mean -o '$TEMP_DIR/filled_mean.csv'"

# nail rename
run_test "rename - single column" "nail rename '$TEST_DATA' --column 'id=identifier' -o '$TEMP_DIR/renamed.csv'"
run_test "rename - multiple columns" "nail rename '$TEST_DATA' --column 'id=identifier,name=full_name' -o '$TEMP_DIR/renamed_multi.csv'"

# nail create
run_test "create - single column" "nail create '$TEST_DATA' --column 'total=price*quantity' -o '$TEMP_DIR/created.csv'"
run_test "create - multiple columns" "nail create '$TEST_DATA' --column 'total=price*quantity,profit=revenue-cost' -o '$TEMP_DIR/created_multi.csv'"

# nail dedup
run_test "dedup - row-wise" "nail dedup '$TEST_DATA' --row-wise -o '$TEMP_DIR/dedup_rows.csv'"
run_test "dedup - by columns" "nail dedup '$TEST_DATA' --row-wise -c 'category,region' -o '$TEMP_DIR/dedup_by_cols.csv'"

# ==========================================
# DATA SAMPLING & TRANSFORMATION
# ==========================================

echo -e "\n${BLUE}=== DATA SAMPLING & TRANSFORMATION ===${NC}"

# nail sample
run_test "sample - random" "nail sample '$TEST_DATA' -n 10 -o '$TEMP_DIR/sample_random.csv'"
run_test "sample - with seed" "nail sample '$TEST_DATA' -n 10 --random 42 -o '$TEMP_DIR/sample_seeded.csv'"
run_test "sample - first" "nail sample '$TEST_DATA' -n 10 --method first -o '$TEMP_DIR/sample_first.csv'"
run_test "sample - last" "nail sample '$TEST_DATA' -n 10 --method last -o '$TEMP_DIR/sample_last.csv'"
run_test "sample - stratified" "nail sample '$TEST_DATA' -n 15 --method stratified --stratify-by category -o '$TEMP_DIR/sample_stratified.csv' 2>/dev/null"

# nail shuffle
run_test "shuffle - basic" "nail shuffle '$TEST_DATA' -o '$TEMP_DIR/shuffled.csv'"
run_test "shuffle - with seed" "nail shuffle '$TEST_DATA' --random 42 -o '$TEMP_DIR/shuffled_seeded.csv'"

# nail id
run_test "id - create" "nail id '$TEST_DATA' --create --id-col-name 'record_id' -o '$TEMP_DIR/with_id.csv'"
run_test "id - with prefix" "nail id '$TEST_DATA' --create --id-col-name 'record_id' --prefix 'REC-' -o '$TEMP_DIR/with_prefixed_id.csv'"

# ==========================================
# DATA COMBINATION
# ==========================================

echo -e "\n${BLUE}=== DATA COMBINATION ===${NC}"

# Create second dataset for merge/append tests
nail sample "$TEST_DATA" -n 10 -o "$TEMP_DIR/right_table.csv"
nail rename "$TEMP_DIR/right_table.csv" --column 'price=right_price,quantity=right_quantity' -o "$TEMP_DIR/right_table_renamed.csv"

# nail merge
run_test "merge - inner join" "nail merge '$TEMP_DIR/small_sample.csv' --right '$TEMP_DIR/right_table_renamed.csv' --key id -o '$TEMP_DIR/merged.csv'"
run_test "merge - left join" "nail merge '$TEMP_DIR/small_sample.csv' --right '$TEMP_DIR/right_table_renamed.csv' --left-join --key id -o '$TEMP_DIR/merged_left.csv'"

# nail append
run_test "append - basic" "nail append '$TEMP_DIR/small_sample.csv' --files '$TEMP_DIR/right_table.csv' --ignore-schema -o '$TEMP_DIR/appended.csv'"

# nail split
run_test "split - basic" "nail split '$TEST_DATA' --ratio '0.7,0.3' --names 'train,test' --output-dir '$TEMP_DIR/splits/'"
run_test "split - with seed" "nail split '$TEST_DATA' --ratio '0.6,0.4' --random 42 --output-dir '$TEMP_DIR/splits_seeded/'"

# ==========================================
# FILE OPTIMIZATION
# ==========================================

echo -e "\n${BLUE}=== FILE OPTIMIZATION ===${NC}"

# nail optimize
run_test "optimize - basic" "nail optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized.parquet'"
run_test "optimize - with compression" "nail optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized_zstd.parquet' --compression zstd"
run_test "optimize - with sorting" "nail optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized_sorted.parquet' --sort-by 'timestamp,id'"

# ==========================================
# DATA ANALYSIS & TRANSFORMATION
# ==========================================

echo -e "\n${BLUE}=== DATA ANALYSIS & TRANSFORMATION ===${NC}"

# nail binning
run_test "binning - equal width" "nail binning '$TEST_DATA' -c 'age' -b 5 -o '$TEMP_DIR/binned_age.csv'"
run_test "binning - custom bins" "nail binning '$TEST_DATA' -c 'price' -b '0,50,100,200,1000' --method custom -o '$TEMP_DIR/binned_price.csv'"
run_test "binning - with labels" "nail binning '$TEST_DATA' -c 'score' -b 3 --labels 'Low,Medium,High' -o '$TEMP_DIR/binned_score.csv'"

# nail pivot
run_test "pivot - basic" "nail pivot '$TEST_DATA' -i 'category' -c 'status' -l 'price' -o '$TEMP_DIR/pivot.csv'"
run_test "pivot - with mean" "nail pivot '$TEST_DATA' -i 'region' -c 'category' -l 'revenue' --agg mean -o '$TEMP_DIR/pivot_mean.csv'"

# ==========================================
# FORMAT CONVERSION & UTILITY
# ==========================================

echo -e "\n${BLUE}=== FORMAT CONVERSION & UTILITY ===${NC}"

# nail convert
run_test "convert - CSV to Parquet" "nail convert '$TEST_DATA' -o '$TEMP_DIR/converted.parquet'"
run_test "convert - CSV to JSON" "nail convert '$TEST_DATA' -o '$TEMP_DIR/converted.json'"
run_test "convert - Parquet to CSV" "nail convert '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/converted_back.csv'"

# nail count
run_test "count - basic" "nail count '$TEST_DATA'"
run_test "count - verbose" "nail count '$TEST_DATA' --verbose"

# nail update
run_test "update - check" "nail update"

# ==========================================
# GLOBAL OPTIONS TESTS
# ==========================================

echo -e "\n${BLUE}=== GLOBAL OPTIONS TESTS ===${NC}"

# Test global options with various commands
run_test "global - verbose flag" "nail head '$TEST_DATA' -n 2 --verbose"
run_test "global - jobs flag" "nail stats '$TEST_DATA' -j 2"
run_test "global - format flag" "nail head '$TEST_DATA' -n 3 -f json"
run_test "global - output flag" "nail schema '$TEST_DATA' -o '$TEMP_DIR/global_output.txt'"

# ==========================================
# RESULTS SUMMARY
# ==========================================

echo -e "\n=========================================="
echo "              TEST RESULTS"
echo "=========================================="
echo "Total tests run: $TOTAL_TESTS"
echo -e "${GREEN}Tests passed: $PASSED_TESTS${NC}"
echo -e "${RED}Tests failed: $FAILED_TESTS${NC}"

# ==========================================
# CLEANUP
# ==========================================

echo -e "\n${BLUE}=== CLEANUP ===${NC}"
log "Cleaning up temporary files..."

# Remove temporary files but keep the results directory
if [ -d "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
    success "Temporary files cleaned up"
else
    warning "No temporary files to clean up"
fi

# Optional: Clean up results directory (uncomment if desired)
# if [ -d "$RESULTS_DIR" ]; then
#     rm -rf "$RESULTS_DIR"
#     success "Results directory cleaned up"
# fi

log "Cleanup completed"

# ==========================================
# FINAL SUMMARY
# ==========================================

echo -e "\n=========================================="
echo "              FINAL SUMMARY"
echo "=========================================="

if [ $FAILED_TESTS -eq 0 ]; then
    success "All tests passed! ✨"
    success "Temporary files have been cleaned up"
    exit 0
else
    error "Some tests failed. Check the output above for details."
    warning "Temporary files have been cleaned up"
    exit 1
fi