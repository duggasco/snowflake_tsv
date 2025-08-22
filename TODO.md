# TODO.md - Snowflake ETL Pipeline Manager
*Last Updated: 2025-01-22*
*Current Version: 2.8.1*

## ✅ Completed (2025-08-21)
- [x] Fix critical IndentationError in validation code
- [x] Add row count anomaly detection to validation
- [x] Implement 10% threshold for outliers (normal variance)
- [x] Add clear validation failure explanations
- [x] Implement validation progress bars
- [x] Ensure validation results always visible (even in quiet mode)
- [x] Add comprehensive batch summary at end of runs
- [x] Fix static progress bar accumulation issue
- [x] **Implement drop_month.py for safe month-based data deletion** ✅
- [x] **Add SnowflakeDeleter class with transaction management** ✅
- [x] **Create comprehensive safety features (dry-run, preview, confirmation)** ✅
- [x] **Add audit logging for all deletion operations** ✅
- [x] **Create drop_month.sh bash wrapper for batch operations** ✅
- [x] **Document recovery procedures using Snowflake Time Travel** ✅
- [x] **Fix "ping pong" issue with long-running COPY operations** ✅
- [x] **Add async execution for files >100MB** ✅
- [x] **Implement keepalive mechanism to prevent query cancellation** ✅
- [x] **Fix slow COPY (ON_ERROR='CONTINUE' → 'ABORT_STATEMENT')** ✅
- [x] **Add warehouse size detection and warnings** ✅
- [x] **Implement automatic stage cleanup** ✅
- [x] **Create performance diagnostic tool (check_stage_and_performance.py)** ✅

## ✅ Completed (2025-01-22 Session) - Unified Wrapper v2.8.1
- [x] **Smart table selection from config files** ✅
- [x] **Auto-detect tables from config and show selection menu** ✅
- [x] **Implement context-aware prompts with table names** ✅
- [x] **Fix black screen issues - all operations use job management** ✅
- [x] **Enhanced job results display - show actual output** ✅
- [x] **Dynamic UI sizing for full content visibility** ✅
- [x] **Create check_duplicates_interactive.py for progress** ✅
- [x] **Fix character encoding in job status display** ✅
- [x] **Implement scrollable view for long content** ✅
- [x] **Test job management system comprehensively** ✅
- [x] **Implement persistent log viewer using 'less' pager** ✅
- [x] **Fix job log viewing disappearing immediately after display** ✅
- [x] **Fix critical unbound variable error (GRAY) causing script crash** ✅
- [x] **Implement robust log viewer with comprehensive error handling** ✅
- [x] **Add fallback pagers (less > more > cat) for compatibility** ✅

## ✅ Completed (Previous Session) - Unified Wrapper v2.4.0
- [x] Implement all placeholder recovery functions in unified wrapper
- [x] Parameterize duplicate checking to accept custom columns and dates
- [x] Add stage cleaning functionality with interactive mode
- [x] Implement VARCHAR error recovery automation
- [x] Add clean file generation for problematic TSVs
- [x] Update wrapper to version 2.1.0 with full recovery capabilities
- [x] **Add smart table selection from config files** ✅
- [x] **Auto-detect tables from config and show selection menu** ✅
- [x] **Implement context-aware prompts with table names** ✅

## 🔥 High Priority (Next Session - Focus on Performance & Scale)

### Performance Optimization
- [ ] Investigate streaming validation for file-based QC
- [ ] Optimize memory usage for 50GB+ file processing
- [ ] Implement chunked processing for anomaly detection
- [ ] Profile and optimize slow validation queries
- [ ] Add connection pooling for Snowflake operations

### Error Recovery & Resilience
- [ ] Add retry mechanism for failed Snowflake operations
- [ ] Implement checkpoint/resume for interrupted batch runs
- [ ] Better error messages for common issues (partially done)
- [ ] Add timeout handling for long-running operations (partially done with async)
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
- [ ] Troubleshooting guide (partially done in README)
- [ ] Performance tuning guide (partially done in README)
- [ ] Architecture diagrams

## 🐛 Known Bugs (Some Fixed)
- [ ] Progress bars can overlap if terminal is resized during execution
- [ ] Memory usage high for file-based QC on 50GB+ files
- [ ] Date format limited to YYYYMMDD only
- [ ] Validation results not aggregated for non-batch runs
- [ ] No cleanup of old log files
- [x] ~~COPY operations stuck in ping-pong for large files~~ ✅ FIXED
- [x] ~~ON_ERROR='CONTINUE' causing extremely slow loads~~ ✅ FIXED

## 🔧 Technical Debt
- [ ] Split tsv_loader.py into modules (validation, loading, progress)
- [ ] Create abstract base classes for validators
- [ ] Implement proper logging hierarchy
- [ ] Add type hints throughout codebase
- [ ] Refactor duplicate code in run_loader.sh

## 📝 Notes for Next Session
1. Monitor async COPY performance with new optimizations
2. Consider using Dask or Ray for distributed processing
3. Review async execution effectiveness for very large files (50GB+)
4. Look into using Snowflake's GET_QUERY_OPERATOR_STATS for performance monitoring
5. Review possibility of using Snowflake Tasks for scheduling

## 💬 User Feedback Addressed
- ✅ "COPY operations taking hours" - Fixed with async and ABORT_STATEMENT
- ✅ "Need ability to delete monthly data" - Implemented drop_month.py
- ✅ "Better visibility into long-running operations" - Added async with progress
- "Need ability to resume failed batch runs" - Still pending
- "Want email alerts when validation fails" - Still pending
- "CSV export would be helpful for reporting" - Still pending
- "Memory usage too high for our 50GB files" - Partially addressed

---
*Use this TODO list to maintain project momentum across sessions*