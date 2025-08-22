# Snowflake ETL Pipeline Manager - Unified Wrapper

## Version 2.1.0 - Fully Implemented Recovery Functions

## Overview
Successfully implemented a comprehensive unified wrapper script (`snowflake_etl.sh`) that consolidates all Snowflake ETL operations into a single, security-hardened interface with both interactive menu and CLI modes. Version 2.1.0 completes all placeholder functions with full recovery and diagnostic capabilities.

## Key Features Implemented

### 1. **Security Enhancements**
- ✅ Eliminated `eval` - Uses safe array expansion for commands
- ✅ Eliminated `source` - Job files parsed as data, not executed
- ✅ Robust `flock` implementation with proper cleanup
- ✅ Input validation for all user inputs
- ✅ Safe file operations with proper escaping

### 2. **User Interface**
- ✅ Dialog/whiptail support with automatic fallback to text menus
- ✅ Breadcrumb navigation showing current menu path
- ✅ Color-coded output (configurable)
- ✅ Progress indicators for long operations
- ✅ Clear error messages with context

### 3. **Job Management**
- ✅ Background job execution for long-running tasks
- ✅ Real-time job status monitoring
- ✅ Automatic detection of crashed jobs
- ✅ Job logs with error details
- ✅ Clean up completed jobs

### 4. **Menu Structure**
```
Main Menu
├── 📦 Quick Load        - Common loading tasks
├── 🔄 Data Operations   - Load/Validate/Delete  
│   ├── Load Data
│   ├── Validate Data
│   ├── Delete Data
│   ├── Check Duplicates (v2.1.0 - now parameterized)
│   └── Compare Files
├── 🔧 File Tools        - Analyze/Compare/Generate
│   ├── Sample TSV File
│   ├── Generate Config
│   ├── Analyze File Structure
│   ├── Check for Issues
│   └── View File Stats
├── 🚑 Recovery & Fix    - Error recovery tools (v2.1.0 - fully implemented)
│   ├── Diagnose Failed Load
│   ├── Fix VARCHAR Errors
│   ├── Recover from Logs
│   ├── Clean Stage Files
│   └── Generate Clean Files
├── 📊 Job Status        - Monitor operations
└── ⚙️ Settings          - Configure defaults
```

### 5. **CLI Mode**
```bash
# Load data
./snowflake_etl.sh load --month 2024-01
./snowflake_etl.sh load --file data.tsv

# Validate
./snowflake_etl.sh validate --month 2024-01

# Delete
./snowflake_etl.sh delete --table TABLE --month 2024-01

# Monitor
./snowflake_etl.sh status
./snowflake_etl.sh clean
```

### 6. **State Management**
- Persistent preferences in `.etl_state/preferences`
- Job tracking in `.etl_state/jobs/`
- Lock files in `.etl_state/locks/`
- Comprehensive logging in `logs/`

## Integration Summary

### Scripts Integrated
1. **run_loader.sh** - All loading operations
2. **generate_config.sh** - Configuration generation
3. **tsv_sampler.sh** - File sampling and analysis
4. **drop_month.sh** - Safe data deletion
5. **compare_tsv_files.py** - File comparison
6. **validate_tsv_file.py** - File validation
7. **check_duplicates** - Parameterized duplicate detection (v2.1.0)
8. **recover_failed_load.sh** - Error recovery operations (v2.1.0)
9. **check_stage_and_performance.py** - Stage management (v2.1.0)
10. **diagnose_copy_error.py** - Error diagnostics (v2.1.0)

### Safety Features
- Multiple confirmation steps for deletions
- Type-to-confirm for destructive operations
- Preview modes before changes
- Automatic backup of preferences
- Lockfile protection against concurrent operations

## Usage Guide

### Interactive Mode
```bash
# Launch the menu
./snowflake_etl.sh

# Navigate using numbers
# 0 or Enter to go back/exit
# Breadcrumbs show current location
```

### Common Tasks

#### Quick Load Current Month
1. Launch: `./snowflake_etl.sh`
2. Select: Quick Load → Load Current Month
3. Confirm and monitor in Job Status

#### Batch Process Multiple Months
```bash
# CLI mode
./snowflake_etl.sh load --month 2024-01,2024-02,2024-03

# Or interactive
Quick Load → Load Data → Enter "all"
```

#### Check for Duplicates (v2.1.0 Enhanced)
1. Data Operations → Check Duplicates
2. Enter table name
3. Optional: Specify month range
4. Specify key columns (defaults to recordDate,assetId,fundId)
5. View detailed duplicate statistics and samples

#### Compare Problem Files
1. File Tools → Compare Files
2. Enter good file path
3. Enter problematic file path
4. Choose quick mode for large files

## Configuration

### Default Paths
- Config: `config/config.json`
- Data: `data/`
- Logs: `logs/`
- State: `.etl_state/`

### Preferences
Automatically saved:
- Last used config file
- Base path for TSV files
- Worker count preference
- Color output preference

## Migration from Old Scripts

### For Existing Users
1. **Old scripts still work** - Backward compatibility maintained
2. **Gradual migration** - Start using wrapper for new tasks
3. **Same config files** - No changes needed

### Command Mapping
| Old Command | New Command |
|------------|-------------|
| `./run_loader.sh --month 2024-01` | `./snowflake_etl.sh load --month 2024-01` |
| `./drop_month.sh --table X --month Y` | `./snowflake_etl.sh delete --table X --month Y` |
| `python3 compare_tsv_files.py` | Interactive: File Tools → Compare Files |

## Benefits

### Over Previous Setup
1. **Single entry point** - One command to remember
2. **Discoverability** - All features visible in menu
3. **Safety** - Centralized validation and confirmations
4. **Monitoring** - Unified job tracking
5. **Consistency** - Same interface for all operations

### Production Ready
- Security hardened based on expert review
- Robust error handling
- Comprehensive logging
- State recovery on crashes
- Safe concurrent operation handling

## Future Enhancements

### New in Version 2.1.0
- ✅ **Parameterized Duplicate Checking** - Specify custom key columns and date ranges
- ✅ **VARCHAR Error Recovery** - Automated cleanup and retry for date format issues
- ✅ **Stage File Management** - Clean up orphaned files in Snowflake stages
- ✅ **Clean File Generation** - Create sanitized versions of problematic TSV files
- ✅ **Log Recovery Tools** - Extract and diagnose errors from job logs

### Planned Features
- [ ] Email notifications for job completion
- [ ] Scheduled job support
- [ ] Data quality reports
- [ ] Performance metrics dashboard
- [ ] Automated retry on failures

### Not Implemented (By Design)
- REST API (unnecessary complexity)
- Generic undo/rollback (use specific recovery)
- Formal job queue (simple background management sufficient)

## Troubleshooting

### Common Issues

1. **"Another operation in progress"**
   - Check: `./snowflake_etl.sh status`
   - Clean locks: `rm .etl_state/locks/*`

2. **Jobs show as CRASHED**
   - Check logs in `logs/` directory
   - Clean up: `./snowflake_etl.sh clean`

3. **Dialog not working**
   - Install: `apt-get install dialog`
   - Or continue with text mode (automatic)

## Summary

The unified wrapper successfully consolidates all ETL operations while adding:
- Enhanced security
- Better user experience  
- Comprehensive job management
- Safe state handling
- Both interactive and automated modes

All critical feedback from code review has been addressed, making this production-ready for enterprise use.