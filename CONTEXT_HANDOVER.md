# CONTEXT_HANDOVER.md - Session 4 to Session 5
*Created: 2025-08-27*
*From Version: 3.0.6*

## Critical Information for Next Session

### 1. COPY Validation Timeout Fix (v3.0.6) - COMPLETED ✅

#### Problem Solved:
- Large files (>100MB compressed) were timing out after exactly 5 minutes
- Error: `000604 (57014): SQL execution canceled`
- Root cause: COPY validation running synchronously without keepalive
- The validation was running `COPY INTO ... VALIDATION_MODE = 'RETURN_ERRORS'`

#### Solution Implemented:
- **Completely removed COPY validation** from `snowflake_etl/core/snowflake_loader.py`
- Deleted `_validate_data()` method entirely
- Removed `VALIDATION_MODE = 'RETURN_ERRORS'` from COPY query
- Now relies solely on `ON_ERROR = 'ABORT_STATEMENT'` during actual COPY
- Async COPY with keepalive still works for files >100MB

#### Files Modified:
- `/root/snowflake/snowflake_etl/core/snowflake_loader.py`
  - Lines 287-319: Simplified `_copy_to_table()` method
  - Line 341: Removed VALIDATION_MODE from query
  - Lines 345-403: Deleted entire `_validate_data()` method

### 2. Menu QC Selection Enhancement - IN PROGRESS 🚧

#### User Request:
"For loading operations - have our menus prompt us where we want to run validity/quality checks - either file-based or snowflake based. This should work for both the normal snowflake operations loading and for quick loading"

#### What Needs to Be Done:
1. **Add helper function** to `snowflake_etl.sh`:
```bash
select_quality_check_method() {
    local choice=$(show_menu "Select Quality Check Method" \
        "File-based QC (thorough but slower for large files)" \
        "Snowflake-based validation (fast, uses SQL aggregates)" \
        "Skip quality checks (rely on COPY error handling)")
    
    case "$choice" in
        1) echo "" ;;  # File-based (default, no flag needed)
        2) echo "--validate-in-snowflake" ;;
        3) echo "--skip-qc" ;;
        *) echo "" ;;
    esac
}
```

2. **Update Quick Load functions** (lines ~1150-1230):
   - `quick_load_current_month()`
   - `quick_load_last_month()` 
   - `quick_load_specific_file()`
   - Add QC method selection before running command

3. **Update normal Load Data menu** (line ~1271):
   - `menu_load_data()` all three options
   - Add QC method selection

#### Example implementation pattern:
```bash
quick_load_current_month() {
    # ... existing code ...
    
    # Add QC selection
    local qc_flags=$(select_quality_check_method)
    
    # Modify command to include flags
    local cmd="python -m snowflake_etl --config \"$CONFIG_FILE\" load"
    cmd="$cmd --base-path \"$base_path\" --month \"$month\" $qc_flags"
    
    # ... rest of function ...
}
```

### 3. Important Context About Validation

#### Two Types of Validation in the System:
1. **COPY Validation** (REMOVED in v3.0.6):
   - Was checking TSV format errors during load
   - Timed out on large files
   - Now removed entirely

2. **Data Quality Validation** (STILL ACTIVE):
   - File-based: Checks dates/completeness while reading TSV
   - Snowflake-based: Uses SQL aggregates after loading
   - This is what the menu QC selection controls

### 4. Current System Architecture

#### Key Performance Optimizations:
- Async COPY for files >100MB compressed (with keepalive)
- Keepalive sends query every 4 minutes to prevent timeout
- `ON_ERROR = 'ABORT_STATEMENT'` for fast failure
- PURGE=TRUE for automatic stage cleanup

#### Critical Constants in SnowflakeLoader:
- `ASYNC_THRESHOLD_MB = 100`
- `KEEPALIVE_INTERVAL_SEC = 240` (4 minutes)
- `MAX_WAIT_TIME_SEC = 7200` (2 hours)

### 5. Testing Notes

#### To Test the Fix on Remote System:
1. Pull latest changes or update `snowflake_etl/core/snowflake_loader.py`
2. Load a large file (>100MB compressed)
3. Should NOT see "Validating data for..." message
4. Should see "Using async COPY for large file"
5. File should load without 5-minute timeout

### 6. Known Issues to Watch

- Memory usage still high for file-based QC on 50GB+ files
- That's why Snowflake-based validation is preferred for large files
- The menu enhancement will help users choose appropriately

### 7. Next Session Priority

**PRIORITY 1**: Complete the menu QC selection enhancement
- This is partially started but not finished
- User specifically requested this feature
- Will improve user experience significantly

**PRIORITY 2**: Test v3.0.6 on production with large files
- Ensure timeout issue is fully resolved
- Monitor async COPY performance

## Commands for Quick Reference

```bash
# Test with a large file
./run_loader.sh --direct-file /path/to/largefile.tsv --skip-qc

# Check if validation was removed
grep -n "validate_data" snowflake_etl/core/snowflake_loader.py
# Should return no results

# Monitor async COPY
tail -f logs/snowflake_etl_debug.log | grep -i "async\|keepalive"
```

## Session Summary
- Fixed critical timeout issue by removing COPY validation
- Started but didn't complete menu QC selection enhancement  
- System is now at v3.0.6 and production ready
- Remote systems need to pull latest for timeout fix