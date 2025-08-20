# CONTEXT HANDOVER - Snowflake TSV Loader Project

## Session Date: 2025-08-20

## Current State Summary

The Snowflake TSV Loader is a production-ready ETL pipeline for loading large TSV files (up to 50GB) into Snowflake. The system now has fully functional parallel processing with clean progress bar display, especially optimized for `--quiet` mode.

## Recent Accomplishments

### 1. Progress Bar System Overhaul
- **Parallel Progress Bars**: Each parallel job gets its own set of non-overlapping progress bars
- **Context-Aware Display**: Shows QC Rows bar only when performing file-based quality checks
- **Per-File Compression**: Each file shows its own compression progress with filename
- **Full Width Bars**: Fixed compression bar width to match other progress bars
- **Position Management**: Uses TSV_JOB_POSITION environment variable for proper stacking

### 2. Quiet Mode Implementation
- **Complete Suppression**: ALL bash script outputs wrapped with quiet mode checks
- **Progress-Only Display**: In --quiet mode, ONLY progress bars are shown (via stderr)
- **Log Preservation**: All logs still written to files for debugging
- **Parallel Optimization**: Perfect for parallel processing without terminal clutter

### 3. Technical Improvements
- Fixed tqdm positioning issues with parallel execution
- Removed manual spacing that caused display artifacts
- Implemented proper cleanup with leave=False for completed bars
- Added file-specific tracking to ProgressTracker class

## Current Architecture

### Progress Bar Hierarchy (per job)
1. **Files Progress** - Overall file processing (always shown)
2. **QC Rows Progress** - Quality check progress (only with file-based QC)
3. **Compression Progress** - Per-file compression with filename
4. **[PENDING] Upload Progress** - Azure blob/Snowflake stage upload
5. **[PENDING] COPY Progress** - Snowflake COPY operation

### Key Files Modified
- `tsv_loader.py` - ProgressTracker class with position management
- `run_loader.sh` - Quiet mode checks throughout, TSV_JOB_POSITION setting

## Next Session Tasks

### High Priority - Additional Progress Bars

1. **Azure Blob/Snowflake Stage Upload Progress**
   - Track PUT command progress
   - Show MB/s upload speed
   - Display estimated time remaining
   - Handle parallel uploads properly

2. **Snowflake COPY Operation Progress**
   - Track rows being inserted
   - Show rows/second processing rate
   - Display completion percentage
   - Handle large file COPY operations

### Implementation Plan for New Progress Bars

#### 1. Upload Progress Bar
```python
def start_file_upload(self, filename: str, file_size_mb: float):
    """Start upload progress for a specific file"""
    # Position at compress_position + 1
    self.upload_pbar = tqdm(total=file_size_mb,
                           desc="{}Uploading {}".format(self.desc_prefix, os.path.basename(filename)),
                           unit="MB",
                           unit_scale=True,
                           position=self.upload_position,
                           leave=False,
                           file=sys.stderr)
```

#### 2. COPY Progress Bar
```python
def start_copy_operation(self, table_name: str, row_count: int):
    """Start COPY progress for Snowflake operation"""
    # Position at upload_position + 1
    self.copy_pbar = tqdm(total=row_count,
                         desc="{}Loading into {}".format(self.desc_prefix, table_name),
                         unit="rows",
                         unit_scale=True,
                         position=self.copy_position,
                         leave=False,
                         file=sys.stderr)
```

### Technical Challenges to Address

1. **Upload Progress Tracking**
   - Snowflake PUT command doesn't provide real-time progress
   - May need to monitor file transfer in chunks
   - Could use callback mechanism if available

2. **COPY Progress Tracking**
   - Snowflake COPY is atomic - completes or fails
   - May need to use VALIDATION_MODE first to estimate
   - Could query information_schema for progress

3. **Position Management**
   - With 5 progress bars, need to adjust position calculations
   - Update lines_per_job in bash script (currently 2-3, will be 4-5)
   - Handle different modes (skip QC, skip upload, etc.)

## Environment Variables

### Key Variables for Progress Management
- `TSV_JOB_POSITION` - Set by run_loader.sh for each parallel job (0, 1, 2...)
- `QUIET_MODE` - Suppresses all output except progress bars
- `SKIP_QC` - Affects number of progress bars shown

## Testing Requirements

### Test Scenarios for New Progress Bars
1. Single file upload with progress tracking
2. Parallel uploads with proper positioning
3. COPY operation progress for various file sizes
4. Quiet mode with all 5 progress bars
5. Skip modes (--skip-qc, --validate-only) with correct bar count

## Dependencies

### Current
- `tqdm` - Progress bar library
- `snowflake-connector-python` - Snowflake connectivity
- `pandas`, `numpy` - Data processing

### May Need
- Progress callback hooks for Snowflake PUT/COPY
- Async monitoring for upload progress
- Threading for progress updates

## Configuration Considerations

### Progress Bar Configuration
- Currently hardcoded positions based on mode
- May want to make configurable:
  - Progress bar colors
  - Update frequency
  - Minimum file size for progress display

## Known Issues

1. **tqdm Parallel Limitations**
   - Position parameter has issues with true parallel execution
   - Some visual artifacts may appear with many concurrent jobs
   - Works well with 3-4 parallel jobs, may need adjustment for more

2. **Progress Accuracy**
   - Compression progress is accurate (streaming)
   - Upload progress will depend on API capabilities
   - COPY progress may be estimated rather than real-time

## Success Metrics

### For Next Session
- [ ] Upload progress bar shows real-time transfer progress
- [ ] COPY progress bar shows row insertion progress
- [ ] All 5 bars stack properly in parallel mode
- [ ] Quiet mode remains clean with new bars
- [ ] No performance impact from progress tracking

## Notes for Next Developer

1. **Progress Bar Philosophy**: Only show what's actually happening - hide bars for skipped operations
2. **Quiet Mode is King**: Prioritize clean --quiet mode output for production use
3. **Position Calculation**: Each job needs (number_of_active_bars + 1) lines of space
4. **Test with Real Data**: Progress bars behave differently with small test files vs. large production files
5. **Error Handling**: Progress bars should close cleanly on errors to avoid terminal corruption

## Commands for Quick Testing

```bash
# Test quiet mode with parallel processing
./run_loader.sh --months 012024,022024,032024 --parallel 3 --quiet --skip-qc

# Test with all progress bars (once implemented)
./run_loader.sh --month 2024-01 --quiet

# Test progress bar positioning
TSV_JOB_POSITION=1 python tsv_loader.py --check-system
```

## File Structure Reference

```
snowflake/
├── tsv_loader.py          # Main script with ProgressTracker class
├── run_loader.sh          # Bash wrapper with quiet mode support
├── CONTEXT_HANDOVER.md    # This file - session continuity
├── TODO.md                # Task tracking
├── PLAN.md                # Implementation planning
└── CHANGELOG.md           # Version history
```

## Final State

The system is stable and production-ready with current progress bars. The next session should focus on adding upload and COPY progress bars to complete the visual feedback system. The architecture is in place to support these additions with minimal refactoring.