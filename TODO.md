# TODO.md - Snowflake TSV Loader
*Last Updated: 2025-08-21*

## ✅ Completed (This Session - 2025-08-21)
- [x] Fix critical IndentationError in validation code
- [x] Add row count anomaly detection to validation
- [x] Implement 10% threshold for outliers (normal variance)
- [x] Add clear validation failure explanations
- [x] Implement validation progress bars
- [x] Ensure validation results always visible (even in quiet mode)
- [x] Add comprehensive batch summary at end of runs
- [x] Fix static progress bar accumulation issue
- [x] Create comprehensive CONTEXT_HANDOVER.md

## 🔥 High Priority (Next Session)

### Performance Optimization
- [ ] Investigate streaming validation for file-based QC
- [ ] Optimize memory usage for 50GB+ file processing
- [ ] Implement chunked processing for anomaly detection
- [ ] Profile and optimize slow validation queries
- [ ] Add connection pooling for Snowflake operations

### Error Recovery & Resilience
- [ ] Add retry mechanism for failed Snowflake operations
- [ ] Implement checkpoint/resume for interrupted batch runs
- [ ] Better error messages for common issues
- [ ] Add timeout handling for long-running operations
- [ ] Implement graceful degradation when tqdm unavailable

### Enhanced Reporting
- [ ] Export validation results to CSV/Excel format
- [ ] Add email notifications for validation failures
- [ ] Create HTML reports with charts and graphs
- [ ] Add summary statistics to log files
- [ ] Implement validation history tracking

## 📊 Medium Priority

### Configuration Management
- [ ] Support for multiple config files
- [ ] Environment-specific configs (dev/staging/prod)
- [ ] Config validation and schema checking
- [ ] Encrypted password storage
- [ ] Config inheritance and overrides

### Testing Infrastructure
- [ ] Add integration tests with mock Snowflake
- [ ] Create performance benchmarking suite
- [ ] Automated regression testing
- [ ] Test coverage reporting
- [ ] CI/CD pipeline setup

### Data Quality Enhancements
- [ ] Add data profiling capabilities
- [ ] Implement custom validation rules
- [ ] Support for business day validation
- [ ] Add data lineage tracking
- [ ] Implement data quality scoring

## 💡 Low Priority / Future Ideas

### UI/UX Improvements
- [ ] Web dashboard for monitoring
- [ ] Real-time progress updates via websocket
- [ ] Historical trend analysis
- [ ] Interactive validation reports
- [ ] Mobile-friendly status page

### Advanced Features
- [ ] Support for other file formats (CSV, Parquet)
- [ ] Incremental loading capabilities
- [ ] Data transformation pipeline
- [ ] Schema evolution handling
- [ ] Multi-region Snowflake support

### Documentation
- [ ] API documentation with Sphinx
- [ ] Video tutorials
- [ ] Troubleshooting guide
- [ ] Performance tuning guide
- [ ] Architecture diagrams

## 🐛 Known Bugs
- [ ] Progress bars can overlap if terminal is resized during execution
- [ ] Memory usage high for file-based QC on 50GB+ files
- [ ] Date format limited to YYYYMMDD only
- [ ] Validation results not aggregated for non-batch runs
- [ ] No cleanup of old log files

## 🔧 Technical Debt
- [ ] Split tsv_loader.py into modules (validation, loading, progress)
- [ ] Create abstract base classes for validators
- [ ] Implement proper logging hierarchy
- [ ] Add type hints throughout codebase
- [ ] Refactor duplicate code in run_loader.sh

## 📝 Notes for Next Session
1. Start with performance optimization - users reporting memory issues with large files
2. Consider using Dask or Ray for distributed processing
3. Investigate Snowflake's COPY INTO with VALIDATION_MODE for better error handling
4. Look into using Snowflake's GET_QUERY_OPERATOR_STATS for performance monitoring
5. Review possibility of using Snowflake Tasks for scheduling

## 💬 User Feedback to Address
- "Need ability to resume failed batch runs"
- "Want email alerts when validation fails"
- "CSV export would be helpful for reporting"
- "Memory usage too high for our 50GB files"
- "Need better error messages when Snowflake connection fails"

---
*Use this TODO list to maintain project momentum across sessions*