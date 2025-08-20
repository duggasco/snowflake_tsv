# TODO.md

## Completed Tasks ✓
- [x] Analyze factLendingBenchmark TSV file structure using tsv_sampler.sh
- [x] Identify 41 columns with 38 containing data (3 completely null)
- [x] Map generic column names to business column names
- [x] Create factLendingBenchmark_config.json with proper column mappings
- [x] Add Snowflake credentials from generated_config.json
- [x] Configure RECORDDATEID as the date column (YYYYMMDD format)
- [x] Add Gemini MCP tool for collaborative planning
- [x] Fix critical os module import scope error in SnowflakeLoader (BUG-001)
- [x] Update parallel processing to handle multiple months simultaneously
- [x] Add automatic worker distribution in run_loader.sh
- [x] Add --quiet mode to suppress console logging while keeping progress bars
- [x] Fix progress bars not showing in quiet mode
- [x] Implement SnowflakeDataValidator for in-database validation
- [x] Add --validate-in-snowflake flag to skip memory-intensive file QC
- [x] Add --validate-only flag for checking existing Snowflake data
- [x] Create comprehensive test suite for Snowflake validator
- [x] Test with mock billion+ row table scenarios
- [x] Fix global logger declaration issue in main function
- [x] Add validation flags to run_loader.sh bash script
- [x] Create comprehensive README.md documentation
- [x] Update CLAUDE.md with new command examples
- [x] Update CHANGELOG.md with all recent changes

## Completed in Latest Session (2025-08-20 Part 2) ✅
- [x] Add Upload Progress Bar for Azure/Snowflake stage
  - [x] Implement start_file_upload() method
  - [x] Track PUT command progress
  - [x] Show MB/s upload speed after completion
  - [x] Handle parallel uploads
- [x] Add COPY Progress Bar for Snowflake operations
  - [x] Implement start_copy_operation() method
  - [x] Track row insertion progress (estimated)
  - [x] Show rows/second rate in logs
  - [x] Handle large table operations
- [x] Update position calculations for 5 progress bars
  - [x] Adjust lines_per_job in bash script (5 with QC, 4 without)
  - [x] Handle different skip modes
  - [x] Create test script for validation

## Next Session Priority 🎯 - Fix Static Progress Bars Issue

### Critical Bug Fix
- [ ] Fix multiple static progress bars in parallel mode
  - [ ] Implement progress bar reuse in ProgressTracker
  - [ ] Update start_file_compression() to reset instead of recreate
  - [ ] Update start_file_upload() to reset instead of recreate  
  - [ ] Update start_copy_operation() to reset instead of recreate
  - [ ] Test with --parallel 3 --quiet to verify fix

### Implementation Tasks
- [ ] Modify ProgressTracker class in tsv_loader.py
  - [ ] Add bar existence check before creation
  - [ ] Implement reset() and set_description() for reuse
  - [ ] Ensure thread-safe bar updates
  - [ ] Test bar lifecycle management

### Testing Requirements
- [ ] Test parallel configurations
  - [ ] Test with 1, 2, 3, 5 parallel jobs
  - [ ] Test with --quiet flag
  - [ ] Test without --quiet flag
  - [ ] Verify no static bars remain
- [ ] Test edge cases
  - [ ] Process interruption (Ctrl+C)
  - [ ] Mixed file sizes
  - [ ] Failed file processing
  - [ ] Sequential vs parallel mode switching

### Documentation Updates
- [ ] Update CHANGELOG.md with bug fix
- [ ] Update README.md with parallel processing notes
- [ ] Add troubleshooting section for progress bars

## In Progress 🔄
- [ ] Monitor production runs with new validation features
- [ ] Gather performance metrics from real-world usage
- [ ] Test full pipeline with production data

## Completed Today (2025-08-20) ✓
- [x] Implement parallel progress bar improvements - DONE
  - [x] Add stacked progress bars for parallel processing
  - [x] Each job gets non-overlapping progress bars
  - [x] Label progress bars with month identifier
  - [x] Use TSV_JOB_POSITION environment variable for positioning
- [x] Add context-aware progress display - DONE
  - [x] Show QC Rows bar only when doing file-based QC
  - [x] Hide QC Rows bar when using --skip-qc or --validate-in-snowflake
  - [x] Automatically adjust spacing (3 lines with QC, 2 without)
  - [x] Update ProgressTracker class with show_qc_progress parameter
- [x] Update bash script for parallel progress tracking - DONE
  - [x] Set TSV_JOB_POSITION for each parallel job
  - [x] Calculate initial spacing based on QC mode
  - [x] Track job positions for proper stacking
- [x] Test parallel progress bar display - DONE
  - [x] Created test scripts for with/without QC modes
  - [x] Verified proper stacking and no overlap
  - [x] Confirmed month labels appear correctly

## Previously Completed Today (2025-08-20) ✓
- [x] Create config generator tool (generate_config.sh) - DONE
  - [x] Basic TSV analysis and pattern detection
  - [x] Snowflake table inspection for column headers
  - [x] Pattern detection from filenames ({month} vs {date_range})
  - [x] Interactive mode for Snowflake credentials
  - [x] Support for batch processing of multiple TSV files
  - [x] Validation and error handling
  - [x] Dry-run mode for testing
  - [x] Handle headerless TSV files using column headers option
- [x] Add --direct-file flag to run_loader.sh - DONE
  - [x] Process specific TSV files directly
  - [x] Auto-extract directory for base-path
  - [x] Detect month from filename patterns
  - [x] Support comma-separated file lists
- [x] Update all documentation with new features - DONE
  - [x] README.md updated with config generator and direct file examples
  - [x] CLAUDE.md updated with new commands
  - [x] CHANGELOG.md updated with all recent changes
  - [x] CONTEXT_HANDOVER.md prepared for next session

## Pending Tasks 📋
- [ ] Run tsv_loader.py with the new configuration
- [ ] Monitor data quality checks for September 2022 date range
- [ ] Verify compression process for 21GB file
- [ ] Track upload progress to Snowflake internal stage
- [ ] Validate COPY command execution to FACTLENDINGBENCHMARK table
- [ ] Check row counts match (60,673,993 expected)
- [ ] Review logs for any data quality issues

## Future Considerations 🔮
- [ ] Optimize parallel processing based on actual performance
- [ ] Consider handling for the 3 empty columns (FEEDBUCKET, INVESTMENTSTYLE, DATASOURCETYPE)
- [ ] Set up monitoring for regular monthly loads
- [ ] Document any data quality patterns found