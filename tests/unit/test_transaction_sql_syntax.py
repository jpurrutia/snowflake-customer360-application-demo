"""
Unit tests for transaction generation SQL script syntax validation.

Tests validate:
- SQL file exists and is readable
- SQL syntax is valid (basic parsing)
- Required sections are present
- No obvious SQL syntax errors
"""

import pytest
import os
import re
from pathlib import Path


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def sql_file_path() -> Path:
    """
    Get path to transaction generation SQL file.
    """
    # Assume tests run from project root
    project_root = Path(__file__).parent.parent.parent
    sql_file = project_root / "snowflake" / "data_generation" / "generate_transactions.sql"

    assert sql_file.exists(), f"SQL file not found: {sql_file}"

    return sql_file


@pytest.fixture(scope="module")
def sql_content(sql_file_path: Path) -> str:
    """
    Read SQL file content.
    """
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    assert len(content) > 0, "SQL file is empty"

    return content


# ============================================================================
# Test 1: SQL File Parses
# ============================================================================

def test_sql_file_parses(sql_content: str):
    """
    Verify SQL file has valid basic syntax.

    Checks:
    - Balanced parentheses
    - Balanced quotes
    - No unterminated strings
    - No obvious syntax errors
    """
    # Test 1: Balanced parentheses
    open_parens = sql_content.count('(')
    close_parens = sql_content.count(')')
    assert open_parens == close_parens, \
        f"Unbalanced parentheses: {open_parens} open, {close_parens} close"

    # Test 2: Balanced single quotes (simplified - ignore escaped quotes)
    # This is a basic check - actual SQL parsing is more complex
    # Count single quotes not in comments
    lines_without_comments = [
        line.split('--')[0]  # Remove line comments
        for line in sql_content.split('\n')
    ]
    content_without_comments = '\n'.join(lines_without_comments)

    # Remove block comments /* ... */
    content_clean = re.sub(r'/\*.*?\*/', '', content_without_comments, flags=re.DOTALL)

    # Count single quotes
    single_quote_count = content_clean.count("'")
    assert single_quote_count % 2 == 0, \
        f"Unbalanced single quotes: {single_quote_count} (should be even)"

    # Test 3: No obvious syntax errors
    error_patterns = [
        r'\bFROM\s+FROM\b',  # Duplicate FROM
        r'\bSELECT\s+FROM\b',  # SELECT without columns
        r'\bWHERE\s+FROM\b',  # WHERE before FROM
        r'\bGROUP BY\s+FROM\b',  # GROUP BY before FROM
    ]

    for pattern in error_patterns:
        matches = re.findall(pattern, content_clean, re.IGNORECASE)
        assert len(matches) == 0, \
            f"Found syntax error pattern '{pattern}': {matches}"

    print("✓ SQL syntax validation passed")


# ============================================================================
# Test 2: Required Sections Present
# ============================================================================

def test_required_sections_present(sql_content: str):
    """
    Verify all required sections are present in SQL file.

    Required sections:
    - Part A: Date spine creation
    - Part B: Customer monthly volume
    - Part C: Transaction expansion
    - Part D: Transaction details
    - Part E: Export to S3
    """
    required_sections = [
        ("Part A", r"Part A.*Date Spine"),
        ("Part B", r"Part B.*Monthly.*Volume"),
        ("Part C", r"Part C.*Expand.*Transaction"),
        ("Part D", r"Part D.*Transaction Details"),
        ("Part E", r"Part E.*Export.*S3"),
    ]

    for section_name, pattern in required_sections:
        matches = re.findall(pattern, sql_content, re.IGNORECASE | re.DOTALL)
        assert len(matches) > 0, \
            f"Missing required section: {section_name}"

    print("✓ All required sections present")


# ============================================================================
# Test 3: Temp Tables Created
# ============================================================================

def test_temp_tables_created(sql_content: str):
    """
    Verify all expected temp tables are created.

    Expected temp tables:
    - date_spine
    - customer_monthly_volume
    - transactions_expanded
    - transactions_with_details
    """
    expected_tables = [
        "date_spine",
        "customer_monthly_volume",
        "transactions_expanded",
        "transactions_with_details",
    ]

    for table_name in expected_tables:
        pattern = rf"CREATE\s+(OR\s+REPLACE\s+)?TEMP\s+TABLE\s+{table_name}"
        matches = re.findall(pattern, sql_content, re.IGNORECASE)
        assert len(matches) > 0, \
            f"Missing temp table creation: {table_name}"

    print(f"✓ All {len(expected_tables)} temp tables created")


# ============================================================================
# Test 4: GENERATOR Function Used
# ============================================================================

def test_generator_function_used(sql_content: str):
    """
    Verify GENERATOR() function is used for data generation.

    Expected:
    - At least 2 uses of GENERATOR (date spine + transaction expansion)
    """
    pattern = r"TABLE\s*\(\s*GENERATOR\s*\(\s*ROWCOUNT\s*=>"
    matches = re.findall(pattern, sql_content, re.IGNORECASE)

    MIN_GENERATOR_USES = 2

    assert len(matches) >= MIN_GENERATOR_USES, \
        f"GENERATOR used {len(matches)} times (expected at least {MIN_GENERATOR_USES})"

    print(f"✓ GENERATOR function used {len(matches)} times")


# ============================================================================
# Test 5: Segment Logic Present
# ============================================================================

def test_segment_logic_present(sql_content: str):
    """
    Verify segment-specific logic is implemented.

    Expected segments:
    - High-Value Travelers
    - Stable Mid-Spenders
    - Budget-Conscious
    - Declining
    - New & Growing
    """
    expected_segments = [
        "High-Value Travelers",
        "Stable Mid-Spenders",
        "Budget-Conscious",
        "Declining",
        "New & Growing",
    ]

    for segment in expected_segments:
        # Check if segment name appears in SQL
        assert segment in sql_content, \
            f"Missing segment logic: {segment}"

    print(f"✓ All {len(expected_segments)} segments referenced")


# ============================================================================
# Test 6: Decline Patterns Implemented
# ============================================================================

def test_decline_patterns_implemented(sql_content: str):
    """
    Verify decline pattern logic is implemented.

    Expected:
    - decline_type column used
    - 'gradual' decline pattern
    - 'sudden' decline pattern
    """
    # Check for decline_type column
    assert "decline_type" in sql_content.lower(), \
        "Missing decline_type column reference"

    # Check for gradual decline logic
    gradual_pattern = r"WHEN\s+['\"]gradual['\"]"
    assert re.search(gradual_pattern, sql_content, re.IGNORECASE), \
        "Missing 'gradual' decline pattern"

    # Check for sudden decline logic
    sudden_pattern = r"WHEN\s+['\"]sudden['\"]"
    assert re.search(sudden_pattern, sql_content, re.IGNORECASE), \
        "Missing 'sudden' decline pattern"

    print("✓ Decline patterns (gradual and sudden) implemented")


# ============================================================================
# Test 7: COPY INTO S3 Present
# ============================================================================

def test_copy_into_s3_present(sql_content: str):
    """
    Verify COPY INTO command for S3 export is present.

    Expected:
    - COPY INTO with stage reference
    - FILE_FORMAT specified
    - COMPRESSION = 'GZIP'
    - MAX_FILE_SIZE specified
    """
    # Check COPY INTO
    assert re.search(r"COPY\s+INTO\s+@", sql_content, re.IGNORECASE), \
        "Missing COPY INTO command"

    # Check FILE_FORMAT
    assert re.search(r"FILE_FORMAT\s*=", sql_content, re.IGNORECASE), \
        "Missing FILE_FORMAT specification"

    # Check GZIP compression
    assert re.search(r"COMPRESSION\s*=\s*['\"]GZIP['\"]", sql_content, re.IGNORECASE), \
        "Missing GZIP compression"

    # Check MAX_FILE_SIZE
    assert re.search(r"MAX_FILE_SIZE", sql_content, re.IGNORECASE), \
        "Missing MAX_FILE_SIZE specification"

    print("✓ COPY INTO S3 export configured correctly")


# ============================================================================
# Test 8: Transaction ID Generation
# ============================================================================

def test_transaction_id_generation(sql_content: str):
    """
    Verify transaction ID is generated with proper format.

    Expected:
    - 'TXN' prefix
    - LPAD for zero-padding
    - ROW_NUMBER() for uniqueness
    """
    # Check for TXN prefix
    txn_prefix_pattern = r"['\"]TXN['\"]"
    assert re.search(txn_prefix_pattern, sql_content), \
        "Missing 'TXN' prefix for transaction IDs"

    # Check for LPAD
    assert re.search(r"LPAD\s*\(", sql_content, re.IGNORECASE), \
        "Missing LPAD for transaction ID padding"

    # Check for ROW_NUMBER()
    assert re.search(r"ROW_NUMBER\s*\(\s*\)", sql_content, re.IGNORECASE), \
        "Missing ROW_NUMBER() for transaction ID uniqueness"

    print("✓ Transaction ID generation implemented correctly")


# ============================================================================
# Test 9: Summary Statistics Included
# ============================================================================

def test_summary_statistics_included(sql_content: str):
    """
    Verify summary statistics queries are included.

    Expected:
    - Overall statistics (count, amount)
    - Segment breakdown
    - Status breakdown
    - Channel breakdown
    """
    summary_sections = [
        ("Overall stats", r"Total Transactions"),
        ("Segment breakdown", r"Segment Breakdown"),
        ("Status breakdown", r"Status Breakdown"),
        ("Channel breakdown", r"Channel Breakdown"),
    ]

    for section_name, pattern in summary_sections:
        matches = re.findall(pattern, sql_content, re.IGNORECASE)
        assert len(matches) > 0, \
            f"Missing summary section: {section_name}"

    print("✓ All summary statistics sections included")


# ============================================================================
# Test 10: Metadata Columns Used
# ============================================================================

def test_metadata_columns_used(sql_content: str):
    """
    Verify important metadata is captured.

    Expected:
    - transaction_id
    - customer_id
    - transaction_date
    - transaction_amount
    - merchant_name
    - merchant_category
    - channel
    - status
    """
    expected_columns = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "transaction_amount",
        "merchant_name",
        "merchant_category",
        "channel",
        "status",
    ]

    for column in expected_columns:
        # Case-insensitive search
        pattern = rf"\b{column}\b"
        assert re.search(pattern, sql_content, re.IGNORECASE), \
            f"Missing expected column: {column}"

    print(f"✓ All {len(expected_columns)} expected columns present")


# ============================================================================
# Test 11: No Hardcoded Dates
# ============================================================================

def test_no_hardcoded_dates(sql_content: str):
    """
    Verify no hardcoded dates are used (should use CURRENT_DATE).

    This ensures the script works regardless of when it's run.
    """
    # Check for CURRENT_DATE usage
    assert re.search(r"CURRENT_DATE\s*\(\s*\)", sql_content, re.IGNORECASE), \
        "Missing CURRENT_DATE() - may have hardcoded dates"

    # Check for suspicious hardcoded dates (YYYY-MM-DD format)
    hardcoded_date_pattern = r"['\"]20\d{2}-\d{2}-\d{2}['\"]"
    matches = re.findall(hardcoded_date_pattern, sql_content)

    # Allow dates in comments
    lines_with_dates = []
    for line in sql_content.split('\n'):
        if re.search(hardcoded_date_pattern, line) and not line.strip().startswith('--'):
            lines_with_dates.append(line.strip())

    assert len(lines_with_dates) == 0, \
        f"Found potential hardcoded dates: {lines_with_dates}"

    print("✓ No hardcoded dates (using CURRENT_DATE)")


# ============================================================================
# Test 12: File Size Appropriate
# ============================================================================

def test_file_size_appropriate(sql_file_path: Path):
    """
    Verify SQL file is not too small (incomplete) or too large (bloated).

    Expected range: 10 KB - 100 KB
    """
    file_size = sql_file_path.stat().st_size
    file_size_kb = file_size / 1024

    MIN_SIZE_KB = 10
    MAX_SIZE_KB = 100

    assert MIN_SIZE_KB <= file_size_kb <= MAX_SIZE_KB, \
        f"SQL file size {file_size_kb:.1f} KB outside expected range [{MIN_SIZE_KB}, {MAX_SIZE_KB}]"

    print(f"✓ SQL file size: {file_size_kb:.1f} KB (appropriate)")


# ============================================================================
# Test Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
