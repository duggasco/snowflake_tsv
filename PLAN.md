# PLAN.md

## Current Focus: Complete Progress Bar System

### Phase 1: Upload Progress Bar (Next Session Priority)
**Goal**: Add real-time progress tracking for file uploads to Snowflake stage

**Implementation Steps**:
1. Research Snowflake PUT command progress capabilities
   - Check if snowflake-connector-python supports callbacks
   - Investigate chunked upload possibilities
   - Look for async upload monitoring options

2. Implement start_file_upload() method in ProgressTracker
   - Calculate upload position (compress_position + 1)
   - Track file size and transfer rate
   - Show filename being uploaded
   - Display MB/s and ETA

3. Integration with SnowflakeLoader
   - Hook into PUT command execution
   - Update progress during transfer
   - Handle connection interruptions gracefully

4. Testing
   - Single file upload tracking
   - Parallel upload progress
   - Network interruption handling
   - Various file sizes (1MB to 50GB)

### Phase 2: COPY Progress Bar
**Goal**: Track Snowflake COPY operation progress

**Implementation Steps**:
1. Research COPY progress tracking options
   - Query information_schema during COPY
   - Use VALIDATION_MODE for estimation
   - Check for progress callbacks

2. Implement start_copy_operation() method
   - Calculate copy position (upload_position + 1)
   - Track rows processed
   - Show rows/second rate
   - Display target table name

3. Progress estimation strategies
   - Pre-validate to get row count
   - Monitor query_history during execution
   - Use stage file metadata

4. Testing
   - Various table sizes
   - Different file formats
   - Error handling during COPY
   - Parallel COPY operations

### Phase 3: Position Management Refactor
**Goal**: Support 5 progress bars with dynamic positioning

**Tasks**:
1. Update position calculations
   - Base: Files (always shown)
   - +1: QC Rows (if not skip_qc)
   - +1: Compression (always during processing)
   - +1: Upload (during PUT operation)
   - +1: COPY (during COPY operation)

2. Bash script updates
   - Dynamic lines_per_job calculation
   - Handle all skip mode combinations
   - Proper spacing for 5 bars

3. Mode-specific bar counts
   - --skip-qc: 4 bars (no QC)
   - --validate-only: 1 bar (validation only)
   - --analyze-only: 1 bar (analysis only)
   - Normal: 5 bars (all operations)

### Phase 4: Performance Optimization
**Goal**: Ensure progress tracking doesn't impact performance

**Considerations**:
1. Update frequency throttling
   - Limit updates to every 100ms
   - Batch small updates
   - Skip updates for tiny files

2. Memory management
   - Close unused progress bars immediately
   - Reuse bar objects where possible
   - Monitor memory during large operations

3. Thread safety
   - Ensure locks don't cause deadlocks
   - Minimize lock contention
   - Test with high parallelism

## Implementation Priority Order

1. **Week 1**: Upload Progress Bar
   - Research and prototype
   - Basic implementation
   - Single file testing

2. **Week 2**: COPY Progress Bar
   - Research tracking options
   - Implementation
   - Integration testing

3. **Week 3**: Position Management
   - Refactor calculations
   - Update bash script
   - Test all modes

4. **Week 4**: Polish and Optimization
   - Performance testing
   - Edge case handling
   - Documentation update

## Success Criteria

- [ ] All 5 progress bars display correctly
- [ ] No overlap in parallel mode (3+ jobs)
- [ ] Quiet mode shows only progress bars
- [ ] < 1% performance impact from tracking
- [ ] Clean error handling and recovery
- [ ] Works with files from 1KB to 50GB

## Risk Mitigation

1. **Snowflake API Limitations**
   - Fallback: Estimate progress based on file size
   - Alternative: Show spinner instead of progress

2. **Performance Impact**
   - Throttle updates for large operations
   - Make progress bars optional (--no-progress flag)

3. **Terminal Compatibility**
   - Test on various terminals
   - Fallback to simple text output
   - Handle missing tqdm gracefully

## Dependencies to Research

- Snowflake Python connector progress callbacks
- Azure blob storage transfer progress APIs
- Alternative progress bar libraries (rich, alive-progress)
- Async monitoring capabilities

## Notes

- Keep --quiet mode as the gold standard
- Progress bars should enhance, not hinder
- Consider colorization for different operations
- Document all new environment variables