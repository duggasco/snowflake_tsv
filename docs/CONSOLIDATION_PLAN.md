# Consolidation Plan: Single Unified snowflake_etl.sh Script

## Objective
Consolidate all bash script functionality into a single `snowflake_etl.sh` script, eliminating the need for separate wrapper scripts while maintaining all existing functionality.

## Current Architecture (TO BE REPLACED)
```
snowflake_etl.sh (menu) 
    ├── calls → run_loader.sh (wrapper)
    ├── calls → drop_month.sh (wrapper)  
    └── calls → generate_config.sh (standalone)

Each wrapper then calls → python -m snowflake_etl (CLI)
```

## Target Architecture (SINGLE SCRIPT)
```
snowflake_etl.sh (unified script)
    └── calls → python -m snowflake_etl (CLI) directly
```

## Migration Status

### ✅ Phase 1: Migrate Core Functions from run_loader.sh [COMPLETE - 2025-09-02]

#### Functions Added: ✅
1. **convert_month_format()** - Convert between YYYY-MM and MMYYYY formats ✅
2. **find_month_directories()** - Discover available months in data directory ✅
3. **process_direct_files()** - Handle direct TSV file processing ✅
4. **process_month_direct()** - Core month processing logic ✅
5. **check_prerequisites()** - Verify Python, packages installed ✅
6. **execute_python_cli()** - Direct wrapper for Python CLI calls ✅

#### Functions Deferred to Phase 2:
- **Parallel processing logic** - Handle multiple months in parallel (Phase 2)
- **Batch mode processing** - Process all discovered months (Phase 2)

#### Key Features Preserved: ✅
- Color-coded output for different message types
- Progress tracking and timing
- Parallel job management with configurable workers
- Direct file mode (--direct-file)
- Batch processing (--batch)
- Validation mode selection (file/snowflake/skip)

### Phase 2: Migrate Functions from drop_month.sh

#### Functions to Add:
1. **Safe deletion workflow** - Multi-step confirmation
2. **Multi-month deletion** - Handle comma-separated months
3. **Dry-run support** - Preview before deletion

#### Key Features to Preserve:
- Safety warnings and confirmations
- Color-coded danger messages
- Preview mode before actual deletion
- Batch deletion for multiple months/tables

### Phase 3: Migrate Functions from generate_config.sh

#### Functions to Add:
1. **detect_pattern()** - Auto-detect file naming patterns
2. **extract_table_name()** - Extract table name from filename
3. **analyze_tsv()** - Sample TSV to detect structure
4. **query_snowflake_columns()** - Get column info from Snowflake
5. **generate_config()** - Create JSON config file
6. **interactive_mode()** - Prompt for Snowflake credentials

#### Key Features to Preserve:
- Auto-detection of file patterns ({date_range} vs {month})
- Snowflake schema querying
- Manual column header input option
- Interactive credential collection
- Dry-run mode

### Phase 4: Update Existing Menu Functions

#### Changes Required:
1. **Replace all calls to `./run_loader.sh`** with direct Python CLI calls
2. **Replace all calls to `./drop_month.sh`** with direct Python CLI calls  
3. **Replace calls to `generate_config.sh`** with integrated function or CLI call
4. **Add prerequisite checking** before any Python operations
5. **Integrate parallel/batch logic** into menu operations

#### Menu Functions to Update:
- `quick_load_current_month()` - Call Python CLI directly
- `quick_load_last_month()` - Call Python CLI directly
- `quick_load_specific_file()` - Call Python CLI directly
- `menu_load_data()` - All sub-options to use Python CLI
- `menu_delete_data()` - Use Python CLI delete command
- `generate_config()` - Use Python CLI config-generate

### Phase 5: Add CLI Mode Support

#### Requirements:
- Support command-line arguments for automation
- Bypass menu for direct operations
- Examples:
  ```bash
  ./snowflake_etl.sh load --month 2024-01 --config config.json
  ./snowflake_etl.sh delete --table MY_TABLE --month 2024-01
  ./snowflake_etl.sh generate-config data/*.tsv
  ```

### Phase 6: Cleanup

1. **Remove deprecated scripts:**
   - run_loader.sh
   - drop_month.sh
   - generate_config.sh
   - recover_failed_load.sh (already deprecated)

2. **Update documentation:**
   - README.md - Update all examples
   - CLAUDE.md - Update wrapper references
   - Remove references to individual scripts

3. **Update test scripts:**
   - Modify test scripts to use unified script

## Implementation Order

1. **Start with check_prerequisites()** - Core safety function
2. **Add direct Python CLI calling** - Replace wrapper calls
3. **Migrate batch/parallel logic** - From run_loader.sh
4. **Add CLI argument parsing** - Enable automation
5. **Test thoroughly** - All menu paths and CLI modes
6. **Remove old scripts** - Final cleanup

## Benefits

1. **Simpler architecture** - One script to maintain
2. **Consistent behavior** - All operations in one place
3. **Easier updates** - No need to sync multiple scripts
4. **Better user experience** - Single entry point
5. **Reduced confusion** - No question about which script to use

## Testing Requirements

### Menu Mode Tests:
- All quick load operations
- All data operations (load, delete, validate)
- File tools and diagnostics
- Config generation

### CLI Mode Tests:
- Direct loading with various flags
- Deletion operations
- Config generation
- Batch operations
- Parallel processing

### Edge Cases:
- Missing prerequisites
- Invalid config files
- Network interruptions
- Large file handling
- Parallel job management

## Success Criteria

1. All existing functionality preserved
2. Single script handles all operations
3. Both menu and CLI modes work
4. No dependency on removed scripts
5. All tests pass
6. Documentation updated