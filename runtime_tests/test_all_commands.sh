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

# Default nail path
NAIL_PATH="nail"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --bin)
            NAIL_PATH="$2"
            shift # past argument
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
    esac
    shift # past argument or value
done

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

# Function to run a test (checks exit code only)
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

# Run a command and assert its stdout CONTAINS a substring. Unlike run_test this
# verifies the actual output, not just the exit code — important for commands
# that can succeed while producing wrong data (pivot/diff/optimize regressions).
run_test_contains() {
    local test_name="$1"
    local command="$2"
    local expected="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log "Running test: $test_name"

    local output status
    if output="$(eval "$command" 2>/dev/null)"; then status=0; else status=$?; fi
    if [ "$status" -eq 0 ] && printf '%s' "$output" | grep -qF -- "$expected"; then
        success "$test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        error "$test_name (expected output to contain: '$expected')"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Run a command and assert it succeeds AND its stdout does NOT contain a substring.
run_test_excludes() {
    local test_name="$1"
    local command="$2"
    local unexpected="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log "Running test: $test_name"

    local output status
    if output="$(eval "$command" 2>/dev/null)"; then status=0; else status=$?; fi
    if [ "$status" -eq 0 ] && ! printf '%s' "$output" | grep -qF -- "$unexpected"; then
        success "$test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        error "$test_name (expected success and output to exclude: '$unexpected')"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Run a command that is EXPECTED to fail (non-zero exit), e.g. invalid input.
run_test_fails() {
    local test_name="$1"
    local command="$2"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log "Running test: $test_name"

    if eval "$command" > /dev/null 2>&1; then
        error "$test_name (command unexpectedly succeeded)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    else
        success "$test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    fi
}

# Function to check if nail is installed
check_nail_installation() {
    if ! command -v "$NAIL_PATH" &> /dev/null; then
        error "nail command not found at '$NAIL_PATH'. Please install nail globally or provide the correct path with --bin."
        echo "Run: cargo install --path . (from the nail project directory) or provide path with --bin"
        exit 1
    fi
    success "nail command found: $($NAIL_PATH --version | head -n 1)"
}

# Function to convert test data to parquet for parquet-specific tests
setup_test_files() {
    log "Setting up test files..."
    
    # Convert CSV to Parquet for parquet-specific tests
    "$NAIL_PATH" convert "$TEST_DATA" -o "$TEMP_DIR/test_data.parquet"
    
    # Create additional test files
    "$NAIL_PATH" head "$TEST_DATA" -n 10 -o "$TEMP_DIR/small_sample.csv"
    "$NAIL_PATH" convert "$TEMP_DIR/small_sample.csv" -o "$TEMP_DIR/small_sample.parquet"
    
    # Create JSON version
    "$NAIL_PATH" convert "$TEST_DATA" -o "$TEMP_DIR/test_data.json"
    
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
run_test "head - basic" "\"$NAIL_PATH\" head '$TEST_DATA' -n 5"
run_test "head - with output" "\"$NAIL_PATH\" head '$TEST_DATA' -n 3 -o '$TEMP_DIR/head_output.csv'"
run_test "head - verbose" "\"$NAIL_PATH\" head '$TEST_DATA' -n 2 --verbose"
run_test "head - JSON output" "\"$NAIL_PATH\" head '$TEST_DATA' -n 5 -f json -o '$TEMP_DIR/head_output.json'"

# nail tail
run_test "tail - basic" "\"$NAIL_PATH\" tail '$TEST_DATA' -n 5"
run_test "tail - with output" "\"$NAIL_PATH\" tail '$TEST_DATA' -n 3 -o '$TEMP_DIR/tail_output.csv'"
run_test "tail - verbose" "\"$NAIL_PATH\" tail '$TEST_DATA' -n 2 --verbose"

# nail preview
run_test "preview - basic" "\"$NAIL_PATH\" preview '$TEST_DATA' -n 5"
run_test "preview - with seed" "\"$NAIL_PATH\" preview '$TEST_DATA' -n 3 --random 42"
run_test "preview - output to file" "\"$NAIL_PATH\" preview '$TEST_DATA' -n 5 -o '$TEMP_DIR/preview_output.csv'"

# nail headers
run_test "headers - basic" "\"$NAIL_PATH\" headers '$TEST_DATA'"
run_test "headers - with filter" "\"$NAIL_PATH\" headers '$TEST_DATA' --filter '^(id|name).*'"
run_test "headers - JSON output" "\"$NAIL_PATH\" headers '$TEST_DATA' -f json -o '$TEMP_DIR/headers.json'"

# nail schema
run_test "schema - basic" "\"$NAIL_PATH\" schema '$TEST_DATA'"
run_test "schema - verbose" "\"$NAIL_PATH\" schema '$TEST_DATA' --verbose"
run_test "schema - JSON output" "\"$NAIL_PATH\" schema '$TEST_DATA' -f json -o '$TEMP_DIR/schema.json'"

# nail metadata (parquet specific)
run_test "metadata - basic" "\"$NAIL_PATH\" metadata '$TEMP_DIR/test_data.parquet'"
run_test "metadata - all" "\"$NAIL_PATH\" metadata '$TEMP_DIR/test_data.parquet' --all"
run_test "metadata - detailed" "\"$NAIL_PATH\" metadata '$TEMP_DIR/test_data.parquet' --schema --row-groups --detailed"

# nail size
run_test "size - basic" "\"$NAIL_PATH\" size '$TEST_DATA'"
run_test "size - with columns" "\"$NAIL_PATH\" size '$TEST_DATA' --columns"
run_test "size - with rows" "\"$NAIL_PATH\" size '$TEST_DATA' --rows"
run_test "size - comprehensive" "\"$NAIL_PATH\" size '$TEST_DATA' --columns --rows"

# nail search
run_test "search - basic value" "\"$NAIL_PATH\" search '$TEST_DATA' --value 'Alice'"
run_test "search - specific columns" "\"$NAIL_PATH\" search '$TEST_DATA' --value 'Electronics' -c 'category'"
run_test "search - case insensitive" "\"$NAIL_PATH\" search '$TEST_DATA' --value 'ACTIVE' --ignore-case"
run_test "search - exact match" "\"$NAIL_PATH\" search '$TEST_DATA' --value 'active' --exact"
run_test "search - rows only" "\"$NAIL_PATH\" search '$TEST_DATA' --value 'Books' --rows"

# ==========================================
# DATA QUALITY TOOLS
# ==========================================

echo -e "\n${BLUE}=== DATA QUALITY TOOLS ===${NC}"

# nail outliers
run_test "outliers - IQR method" "\"$NAIL_PATH\" outliers '$TEST_DATA' -c 'price' --method iqr"
run_test "outliers - Z-score method" "\"$NAIL_PATH\" outliers '$TEST_DATA' -c 'age' --method z-score"
run_test "outliers - show values" "\"$NAIL_PATH\" outliers '$TEST_DATA' -c 'price' --method iqr --show-values"

# ==========================================
# STATISTICS & ANALYSIS
# ==========================================

echo -e "\n${BLUE}=== STATISTICS & ANALYSIS ===${NC}"

# nail stats
run_test "stats - basic" "\"$NAIL_PATH\" stats '$TEST_DATA'"
run_test "stats - specific columns" "\"$NAIL_PATH\" stats '$TEST_DATA' -c 'price,age,score'"
run_test "stats - exhaustive" "\"$NAIL_PATH\" stats '$TEST_DATA' --stats-type exhaustive"
run_test "stats - output to file" "\"$NAIL_PATH\" stats '$TEST_DATA' -o '$TEMP_DIR/stats.json'"

# nail correlations
run_test "correlations - basic" "\"$NAIL_PATH\" correlations '$TEST_DATA'"
run_test "correlations - specific columns" "\"$NAIL_PATH\" correlations '$TEST_DATA' -c 'price,age,score'"
run_test "correlations - kendall" "\"$NAIL_PATH\" correlations '$TEST_DATA' --type kendall"
run_test "correlations - matrix format" "\"$NAIL_PATH\" correlations '$TEST_DATA' --matrix"

# nail frequency
run_test "frequency - single column" "\"$NAIL_PATH\" frequency '$TEST_DATA' -c 'category'"
run_test "frequency - multiple columns" "\"$NAIL_PATH\" frequency '$TEST_DATA' -c 'category,status'"
run_test "frequency - output to file" "\"$NAIL_PATH\" frequency '$TEST_DATA' -c 'region' -o '$TEMP_DIR/frequency.csv'"

# ==========================================
# DATA MANIPULATION
# ==========================================

echo -e "\n${BLUE}=== DATA MANIPULATION ===${NC}"

# nail select
run_test "select - columns" "\"$NAIL_PATH\" select '$TEST_DATA' -c 'id,name,price'"
run_test "select - rows" "\"$NAIL_PATH\" select '$TEST_DATA' -r '1,3,5-10'"
run_test "select - columns and rows" "\"$NAIL_PATH\" select '$TEST_DATA' -c 'id,name' -r '1-5' -o '$TEMP_DIR/selected.csv'"

# nail drop
run_test "drop - columns" "\"$NAIL_PATH\" drop '$TEST_DATA' -c 'description' -o '$TEMP_DIR/dropped_cols.csv'"
run_test "drop - rows" "\"$NAIL_PATH\" drop '$TEST_DATA' -r '1,3,5' -o '$TEMP_DIR/dropped_rows.csv'"

# nail filter
run_test "filter - column conditions" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'price>100,age<40' -o '$TEMP_DIR/filtered.csv'"
run_test "filter - numeric only" "\"$NAIL_PATH\" filter '$TEST_DATA' --rows numeric-only -o '$TEMP_DIR/numeric_only.csv'"
run_test "filter - no NaN" "\"$NAIL_PATH\" filter '$TEST_DATA' --rows no-nan -o '$TEMP_DIR/no_nan.csv'"

# nail fill
run_test "fill - with value" "\"$NAIL_PATH\" fill '$TEST_DATA' --method value --value 0 -o '$TEMP_DIR/filled.csv'"
run_test "fill - with mean" "\"$NAIL_PATH\" fill '$TEST_DATA' -c 'price,age,score' --method mean -o '$TEMP_DIR/filled_mean.csv'"

# nail rename
run_test "rename - single column" "\"$NAIL_PATH\" rename '$TEST_DATA' --column 'id=identifier' -o '$TEMP_DIR/renamed.csv'"
run_test "rename - multiple columns" "\"$NAIL_PATH\" rename '$TEST_DATA' --column 'id=identifier,name=full_name' -o '$TEMP_DIR/renamed_multi.csv'"

# nail create
run_test "create - single column" "\"$NAIL_PATH\" create '$TEST_DATA' --column 'total=price*quantity' -o '$TEMP_DIR/created.csv'"
run_test "create - multiple columns" "\"$NAIL_PATH\" create '$TEST_DATA' --column 'total=price*quantity,profit=revenue-cost' -o '$TEMP_DIR/created_multi.csv'"

# nail dedup
run_test "dedup - row-wise" "\"$NAIL_PATH\" dedup '$TEST_DATA' --row-wise -o '$TEMP_DIR/dedup_rows.csv'"
run_test "dedup - by columns" "\"$NAIL_PATH\" dedup '$TEST_DATA' --row-wise -c 'category,region' -o '$TEMP_DIR/dedup_by_cols.csv'"

# ==========================================
# DATA SAMPLING & TRANSFORMATION
# ==========================================

echo -e "\n${BLUE}=== DATA SAMPLING & TRANSFORMATION ===${NC}"

# nail sample
run_test "sample - random" "\"$NAIL_PATH\" sample '$TEST_DATA' -n 10 -o '$TEMP_DIR/sample_random.csv'"
run_test "sample - with seed" "\"$NAIL_PATH\" sample '$TEST_DATA' -n 10 --random 42 -o '$TEMP_DIR/sample_seeded.csv'"
run_test "sample - first" "\"$NAIL_PATH\" sample '$TEST_DATA' -n 10 --method first -o '$TEMP_DIR/sample_first.csv'"
run_test "sample - last" "\"$NAIL_PATH\" sample '$TEST_DATA' -n 10 --method last -o '$TEMP_DIR/sample_last.csv'"
run_test "sample - stratified" "\"$NAIL_PATH\" sample '$TEST_DATA' -n 15 --method stratified --stratify-by category -o '$TEMP_DIR/sample_stratified.csv' 2>/dev/null"

# nail shuffle
run_test "shuffle - basic" "\"$NAIL_PATH\" shuffle '$TEST_DATA' -o '$TEMP_DIR/shuffled.csv'"
run_test "shuffle - with seed" "\"$NAIL_PATH\" shuffle '$TEST_DATA' --random 42 -o '$TEMP_DIR/shuffled_seeded.csv'"

# nail sort
run_test "sort - basic all columns" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'all' -o '$TEMP_DIR/sorted_all.csv'"
run_test "sort - specific columns" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'price,age' -o '$TEMP_DIR/sorted_cols.csv'"
run_test "sort - descending" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'price' -d true -o '$TEMP_DIR/sorted_desc.csv'"
run_test "sort - numeric strategy" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'price,age' -s 'numeric,numeric' -o '$TEMP_DIR/sorted_numeric.csv'"
run_test "sort - alphabetic strategy" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'name,category' -s 'alphabetic' --case-insensitive -o '$TEMP_DIR/sorted_alpha.csv'"
run_test "sort - nulls first" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'score' --nulls 'first' -o '$TEMP_DIR/sorted_nulls_first.csv'"
run_test "sort - nulls skip" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'score,price' --nulls 'skip' -o '$TEMP_DIR/sorted_nulls_skip.csv'"
run_test "sort - mixed strategies" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'category,price,age' -s 'alphabetic,numeric,numeric' -d 'false,true,false' -o '$TEMP_DIR/sorted_mixed.csv'"
run_test "sort - date strategy" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'timestamp' -s 'date' -o '$TEMP_DIR/sorted_date.csv'"
run_test "sort - verbose" "\"$NAIL_PATH\" sort '$TEST_DATA' -c 'id' --verbose"

# nail id
run_test "id - create" "\"$NAIL_PATH\" id '$TEST_DATA' --create --id-col-name 'record_id' -o '$TEMP_DIR/with_id.csv'"
run_test "id - with prefix" "\"$NAIL_PATH\" id '$TEST_DATA' --create --id-col-name 'record_id' --prefix 'REC-' -o '$TEMP_DIR/with_prefixed_id.csv'"

# ==========================================
# DATA COMBINATION
# ==========================================

echo -e "\n${BLUE}=== DATA COMBINATION ===${NC}"

# Create second dataset for merge/append tests
"$NAIL_PATH" sample "$TEST_DATA" -n 10 -o "$TEMP_DIR/right_table.csv"
"$NAIL_PATH" rename "$TEMP_DIR/right_table.csv" --column 'price=right_price,quantity=right_quantity' -o "$TEMP_DIR/right_table_renamed.csv"

# nail merge
run_test "merge - inner join" "\"$NAIL_PATH\" merge '$TEMP_DIR/small_sample.csv' --right '$TEMP_DIR/right_table_renamed.csv' --key id -o '$TEMP_DIR/merged.csv'"
run_test "merge - left join" "\"$NAIL_PATH\" merge '$TEMP_DIR/small_sample.csv' --right '$TEMP_DIR/right_table_renamed.csv' --left-join --key id -o '$TEMP_DIR/merged_left.csv'"

# nail append
run_test "append - basic" "\"$NAIL_PATH\" append '$TEMP_DIR/small_sample.csv' --files '$TEMP_DIR/right_table.csv' --ignore-schema -o '$TEMP_DIR/appended.csv'"

# nail split
run_test "split - basic" "\"$NAIL_PATH\" split '$TEST_DATA' --ratio '0.7,0.3' --names 'train,test' --output-dir '$TEMP_DIR/splits/'"
run_test "split - with seed" "\"$NAIL_PATH\" split '$TEST_DATA' --ratio '0.6,0.4' --random 42 --output-dir '$TEMP_DIR/splits_seeded/'"

# ==========================================
# FILE OPTIMIZATION
# ==========================================

echo -e "\n${BLUE}=== FILE OPTIMIZATION ===${NC}"

# nail optimize
run_test "optimize - basic" "\"$NAIL_PATH\" optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized.parquet'"
run_test "optimize - with compression" "\"$NAIL_PATH\" optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized_zstd.parquet' --compression zstd"
run_test "optimize - with sorting" "\"$NAIL_PATH\" optimize '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/optimized_sorted.parquet' --sort-by 'timestamp,id'"

# ==========================================
# DATA ANALYSIS & TRANSFORMATION
# ==========================================

echo -e "\n${BLUE}=== DATA ANALYSIS & TRANSFORMATION ===${NC}"

# nail binning
run_test "binning - equal width" "\"$NAIL_PATH\" binning '$TEST_DATA' -c 'age' -b 5 -o '$TEMP_DIR/binned_age.csv'"
run_test "binning - custom bins" "\"$NAIL_PATH\" binning '$TEST_DATA' -c 'price' -b '0,50,100,200,1000' --method custom -o '$TEMP_DIR/binned_price.csv'"
run_test "binning - with labels" "\"$NAIL_PATH\" binning '$TEST_DATA' -c 'score' -b 3 --labels 'Low,Medium,High' -o '$TEMP_DIR/binned_score.csv'"

# nail pivot
run_test "pivot - basic" "\"$NAIL_PATH\" pivot '$TEST_DATA' -i 'category' -c 'status' -l 'price' -o '$TEMP_DIR/pivot.csv'"
run_test "pivot - with mean" "\"$NAIL_PATH\" pivot '$TEST_DATA' -i 'region' -c 'category' -l 'revenue' --agg mean -o '$TEMP_DIR/pivot_mean.csv'"

# ==========================================
# FORMAT CONVERSION & UTILITY
# ==========================================

echo -e "\n${BLUE}=== FORMAT CONVERSION & UTILITY ===${NC}"

# nail convert
run_test "convert - CSV to Parquet" "\"$NAIL_PATH\" convert '$TEST_DATA' -o '$TEMP_DIR/converted.parquet'"
run_test "convert - CSV to JSON" "\"$NAIL_PATH\" convert '$TEST_DATA' -o '$TEMP_DIR/converted.json'"
run_test "convert - Parquet to CSV" "\"$NAIL_PATH\" convert '$TEMP_DIR/test_data.parquet' -o '$TEMP_DIR/converted_back.csv'"

# nail count
run_test "count - basic" "\"$NAIL_PATH\" count '$TEST_DATA'"
run_test "count - verbose" "\"$NAIL_PATH\" count '$TEST_DATA' --verbose"

# nail update
run_test "update - check" "\"$NAIL_PATH\" update"

# ==========================================
# GLOBAL OPTIONS TESTS
# ==========================================

echo -e "\n${BLUE}=== GLOBAL OPTIONS TESTS ===${NC}"

# Test global options with various commands
run_test "global - verbose flag" "\"$NAIL_PATH\" head '$TEST_DATA' -n 2 --verbose"
run_test "global - jobs flag" "\"$NAIL_PATH\" stats '$TEST_DATA' -j 2"
run_test "global - format flag" "\"$NAIL_PATH\" head '$TEST_DATA' -n 3 -f json"
run_test "global - output flag" "\"$NAIL_PATH\" schema '$TEST_DATA' -o '$TEMP_DIR/global_output.txt'"

# ==========================================
# UNIFIED CONDITION ENGINE (filter / drop / create)
# ==========================================
# These verify the shared, newbie-friendly condition syntax: math-style chained
# ranges, SQL operators (IN/LIKE/BETWEEN/IS NULL), case-insensitive column names,
# unquoted string values, '==' alias, and ',' = AND / '|' = OR.
# Reference rows in test_data.csv: Alice Johnson (age 25, price 129.99, active),
# Bob Smith (age 34, price 45.50, Books, inactive), Carol Williams (age 28, active).

echo -e "\n${BLUE}=== UNIFIED CONDITION ENGINE (filter / drop / create) ===${NC}"

# filter: math-style chained range — keeps Alice (25), drops Bob (34)
run_test_contains "filter - chained range keeps in-range" "\"$NAIL_PATH\" filter '$TEST_DATA' -c '20 < age < 30' -o - -f csv" "Alice Johnson"
run_test_excludes "filter - chained range drops out-of-range" "\"$NAIL_PATH\" filter '$TEST_DATA' -c '20 < age < 30' -o - -f csv" "Bob Smith"

# filter: case-insensitive column (STATUS) + unquoted value (active)
run_test_contains "filter - case-insensitive col + unquoted value" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'STATUS=active' -o - -f csv" "Alice Johnson"
run_test_excludes "filter - unquoted value excludes non-matches" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'STATUS=active' -o - -f csv" "inactive"

# filter: SQL IN list
run_test_contains "filter - SQL IN (Electronics)" "\"$NAIL_PATH\" filter '$TEST_DATA' -c \"category IN ('Electronics','Books')\" -o - -f csv" "Electronics"
run_test_contains "filter - SQL IN (Books)" "\"$NAIL_PATH\" filter '$TEST_DATA' -c \"category IN ('Electronics','Books')\" -o - -f csv" "Books"

# filter: SQL LIKE
run_test_contains "filter - SQL LIKE prefix" "\"$NAIL_PATH\" filter '$TEST_DATA' -c \"name LIKE 'Alice%'\" -o - -f csv" "Alice Johnson"
run_test_excludes "filter - SQL LIKE excludes non-matches" "\"$NAIL_PATH\" filter '$TEST_DATA' -c \"name LIKE 'Alice%'\" -o - -f csv" "Bob Smith"

# filter: SQL BETWEEN — keeps Alice (129.99), drops Bob (45.50)
run_test_contains "filter - SQL BETWEEN keeps in-range" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'price BETWEEN 100 AND 300' -o - -f csv" "Alice Johnson"
run_test_excludes "filter - SQL BETWEEN drops out-of-range" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'price BETWEEN 100 AND 300' -o - -f csv" "Bob Smith"

# filter: '==' alias
run_test_contains "filter - '==' alias" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'age == 25' -o - -f csv" "Alice Johnson"

# filter: precedence — (age>100 AND status=active) OR category=Books => only Books
run_test_contains "filter - AND/OR precedence via ,|" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'age>100,status=active|category=Books' -o - -f csv" "Bob Smith"

# filter: a typo'd column must ERROR, not silently pass (LHS stays an identifier)
run_test_fails "filter - typo column errors (no silent pass)" "\"$NAIL_PATH\" filter '$TEST_DATA' -c 'aage > 20' -o - -f csv"

# drop: condition form is the mirror of filter — drops active rows, keeps inactive
run_test_excludes "drop - condition drops matching rows" "\"$NAIL_PATH\" drop '$TEST_DATA' -r 'status=active' -o - -f csv" "Alice Johnson"
run_test_contains "drop - condition keeps non-matching rows" "\"$NAIL_PATH\" drop '$TEST_DATA' -r 'status=active' -o - -f csv" "Bob Smith"

# drop: chained range condition drops Alice (25) & Carol (28), keeps Bob (34)
run_test_excludes "drop - chained range drops in-range" "\"$NAIL_PATH\" drop '$TEST_DATA' -r '20 < age < 30' -o - -f csv" "Alice Johnson"
run_test_contains "drop - chained range keeps out-of-range" "\"$NAIL_PATH\" drop '$TEST_DATA' -r '20 < age < 30' -o - -f csv" "Bob Smith"

# drop: plain row indices must still be treated as indices, not a condition
run_test "drop - row indices still work" "\"$NAIL_PATH\" drop '$TEST_DATA' -r '1,3,5' -o '$TEMP_DIR/drop_idx.csv'"

# create: row filter (-r) now uses the unified engine; column expr still works
run_test_contains "create - unified row filter + new column" "\"$NAIL_PATH\" create '$TEST_DATA' -r '20 < age < 30' -c 'doubled=price*2' -o - -f csv" "doubled"

# ==========================================
# REGRESSION CHECKS (pivot / diff / optimize correctness)
# ==========================================
# These guard the three bugs that previously produced WRONG output while exiting 0.

echo -e "\n${BLUE}=== REGRESSION CHECKS (pivot / diff / optimize) ===${NC}"

# pivot: distinct status values must be spread into their own columns (the header
# should carry 'inactive' as a column; the old broken version kept a 'status' column).
run_test_contains "pivot - spreads pivot values into columns" "\"$NAIL_PATH\" pivot '$TEST_DATA' -i category -c status -l price --agg sum -o - -f csv | head -1" "inactive"
run_test_excludes "pivot - original pivot column is gone" "\"$NAIL_PATH\" pivot '$TEST_DATA' -i category -c status -l price --agg sum -o - -f csv | head -1" "status"

# diff: build two tiny datasets with one unchanged, one modified, one added, one removed row.
printf 'id,val\n1,a\n2,b\n3,c\n' > "$TEMP_DIR/diff_left.csv"
printf 'id,val\n1,a\n2,B\n4,d\n' > "$TEMP_DIR/diff_right.csv"
run_test_contains "diff - identical row reported UNCHANGED" "\"$NAIL_PATH\" diff '$TEMP_DIR/diff_left.csv' -c '$TEMP_DIR/diff_right.csv' -k id -o - -f csv" "UNCHANGED"
run_test_contains "diff - changed row reported MODIFIED" "\"$NAIL_PATH\" diff '$TEMP_DIR/diff_left.csv' -c '$TEMP_DIR/diff_right.csv' -k id -o - -f csv" "MODIFIED"
run_test_contains "diff - added row reported ADDED" "\"$NAIL_PATH\" diff '$TEMP_DIR/diff_left.csv' -c '$TEMP_DIR/diff_right.csv' -k id -o - -f csv" "ADDED"
run_test_contains "diff - removed row reported REMOVED" "\"$NAIL_PATH\" diff '$TEMP_DIR/diff_left.csv' -c '$TEMP_DIR/diff_right.csv' -k id -o - -f csv" "REMOVED"
run_test_excludes "diff - --changes-only drops UNCHANGED rows" "\"$NAIL_PATH\" diff '$TEMP_DIR/diff_left.csv' -c '$TEMP_DIR/diff_right.csv' -k id --changes-only -o - -f csv" "UNCHANGED"

# optimize: --compression zstd must actually be written (verified via metadata)
run_test_contains "optimize - zstd compression actually applied" "\"$NAIL_PATH\" optimize '$TEMP_DIR/test_data.parquet' --compression zstd -o '$TEMP_DIR/opt_zstd.parquet' && \"$NAIL_PATH\" metadata '$TEMP_DIR/opt_zstd.parquet' --show-compression" "zstd"

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