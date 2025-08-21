# CONTEXT HANDOVER - Snowflake TSV Loader Project
*Last Updated: 2025-08-21*

## Project Overview
High-performance ETL pipeline for loading large TSV files (up to 50GB) into Snowflake with comprehensive data quality validation, progress tracking, and parallel processing capabilities.

## Recent Session Accomplishments (2025-08-21)

### 1. ✅ Fixed Critical IndentationError
- **Issue**: Line 1842 had orphaned code causing all validation tests to fail
- **Solution**: Removed 9 lines of incorrectly indented duplicate code
- **Result**: All validation tests now run successfully

### 2. ✅ Enhanced Validation System
#### Row Count Anomaly Detection
- Added statistical analysis of daily row counts
- Detects partial data loads (e.g., 1 row vs expected 48,000)
- Classification levels:
  - **SEVERELY_LOW**: < 10% of average (critical data loss)
  - **LOW**: 10-50% of average
  - **OUTLIER_LOW**: 50-90% of average  
  - **NORMAL**: 90-110% of average (±10% variance)
  - **OUTLIER_HIGH**: > 110% of average

#### Clear Failure Explanations
- Shows WHY validation failed with specific reasons
- Distinguishes "Date Range Requested" vs "Date Range Found"
- Lists specific dates with anomalies and their severity
- Example: "3 date(s) with critically low row counts (<10% of average)"

### 3. ✅ Validation Progress Bars
- Added progress bars for both `--validate-only` and `--validate-in-snowflake`
- Progress bars visible even in `--quiet` mode (via stderr)
- Shows anomaly count in status: "✗ (3 anomalies)"

### 4. ✅ Always-Visible Validation Results
- Validation results ALWAYS display, even in `--quiet` mode
- Critical data quality information never hidden
- Full anomaly details included in output

### 5. ✅ Comprehensive Batch Summary
- Added COMPREHENSIVE VALIDATION RESULTS at end of batch runs
- Shows aggregated statistics across all months:
  ```
  OVERALL STATISTICS:
    Total Tables Validated: 12
    ✓ Valid Tables:        8
    ✗ Invalid Tables:      4
    ⚠ Total Anomalous Dates: 25
  ```
- Lists all failed validations with specific reasons
- Includes detailed results by month

### 6. ✅ Progress Bar Static Issue Fixed
- **Previous Issue**: Multiple dead progress bars accumulated in parallel mode
- **Solution**: Implemented bar reuse pattern
  - Bars created once, reset for each file
  - Used `leave=True` with `reset()` method
  - Added `clear_file_bars()` method
- **Result**: Clean progress display without accumulation

## Current Architecture

### Key Components
1. **tsv_loader.py** - Main ETL script
   - `ProgressTracker` class with bar reuse pattern
   - `SnowflakeDataValidator` with anomaly detection
   - Comprehensive validation result structure

2. **run_loader.sh** - Bash wrapper
   - Parallel processing support
   - Batch validation summary
   - JSON result aggregation

3. **Validation System**
   - File-based QC (traditional)
   - Snowflake-based validation (fast, for large files)
   - Anomaly detection with statistical analysis

## Validation Features Summary

### What Gets Validated
- **Date Completeness**: All expected dates present
- **Gap Detection**: Missing date ranges
- **Row Count Anomalies**: 
  - Uses 10% threshold for outliers
  - Groups by severity levels
  - Shows specific dates with issues
- **Statistical Analysis**: Mean, median, quartiles, std deviation

### Display Format
```
❌ VALIDATION FAILED BECAUSE:
  • 3 date(s) with critically low row counts (<10% of average)

⚠️ SPECIFIC DATES WITH ANOMALIES:
  CRITICALLY LOW (<10% of average):
    • 2024-01-05 → 1 rows (expected ~46,500, got 0.0% of avg)
  OUTLIERS (10-50% below average):
    • 2024-01-28 → 42,000 rows (expected ~46,500, got 87.5% of avg)
```

## Known Issues & Limitations
1. **Memory Usage**: File-based QC can be memory-intensive for 50GB+ files
   - Workaround: Use `--validate-in-snowflake` for large files
2. **Progress Bar Positioning**: In parallel mode, bars can sometimes overlap if terminal is resized
3. **Date Format**: Only supports YYYYMMDD format in date columns

## Next Session Priorities

### High Priority
1. **Performance Optimization**
   - Investigate streaming validation for file-based QC
   - Optimize memory usage for large file processing
   - Consider chunked processing for anomaly detection

2. **Error Recovery**
   - Add retry mechanism for failed Snowflake operations
   - Implement checkpoint/resume for interrupted batch runs
   - Better error messages for common issues

3. **Enhanced Reporting**
   - Export validation results to CSV/Excel
   - Add email notifications for validation failures
   - Create HTML reports with charts

### Medium Priority
1. **Configuration Management**
   - Support for multiple config files
   - Environment-specific configs
   - Config validation and schema checking

2. **Testing Infrastructure**
   - Add integration tests with mock Snowflake
   - Performance benchmarking suite
   - Automated regression testing

### Low Priority
1. **UI Improvements**
   - Web dashboard for monitoring
   - Real-time progress updates via websocket
   - Historical trend analysis

## Technical Debt
1. **Code Organization**
   - Consider splitting tsv_loader.py into modules
   - Separate validation logic into its own module
   - Create utility module for common functions

2. **Documentation**
   - Add API documentation
   - Create troubleshooting guide
   - Document Snowflake table requirements

## Environment & Dependencies
- Python 3.7+
- snowflake-connector-python
- pandas, numpy
- tqdm (optional but recommended)
- Test environment: test_venv/

## Configuration Files
- **config/**: Snowflake connection configs
- **logs/**: Execution and debug logs
- **data/**: TSV file directories (MMYYYY format)

## Test Files (Keep)
- `test_simple_progress.py` - Verifies bar reuse
- `test_progress_bar_fix.py` - Comprehensive parallel test
- `test_anomaly_detection.py` - Anomaly detection test
- `test_validation_progress.py` - Validation progress test
- `test_validation_reasons.py` - Failure explanation test
- `test_anomaly_display.py` - Anomaly display test
- `test_validation_summary.py` - Summary display test

## Quick Commands
```bash
# Validate with progress bars and anomaly detection
./run_loader.sh --validate-only --month 2024-01 --quiet

# Batch validation with comprehensive summary
./run_loader.sh --validate-only --batch --parallel 4

# Process with Snowflake validation (faster for large files)
./run_loader.sh --month 2024-01 --validate-in-snowflake

# Check system capabilities
python tsv_loader.py --check-system
```

## Success Metrics
- Validation now detects partial data loads (not just missing dates)
- 10% threshold prevents normal variance from being flagged
- Comprehensive batch summary provides clear overview
- Progress bars work cleanly in parallel mode
- Critical validation data always visible

## Contact & Support
- GitHub Issues: https://github.com/duggasco/snowflake_tsv
- Documentation: See README.md for detailed usage

---
*This handover document ensures continuity for the next development session*