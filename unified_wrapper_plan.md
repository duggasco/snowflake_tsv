# Unified Wrapper Script Integration Plan

## Current Script Inventory and Analysis

### 1. **run_loader.sh** (Main ETL Pipeline)
- **Purpose**: Load TSV files to Snowflake with quality checks
- **Key Features**:
  - Single/multi-month processing
  - Batch mode for all months
  - Parallel processing support
  - Direct file loading
  - Quality check controls (skip, validate-in-snowflake)
  - Dry run mode
  - Progress tracking with colors
- **Dependencies**: tsv_loader.py, config.json

### 2. **generate_config.sh** (Configuration Generator)
- **Purpose**: Auto-generate config.json from TSV files
- **Key Features**:
  - Auto-detect columns from TSV headers
  - Query Snowflake for table schema
  - Interactive Snowflake credential input
  - Support for headerless files
  - Dry run mode
- **Dependencies**: tsv_sampler.sh (optionally)

### 3. **tsv_sampler.sh** (File Analysis Tool)
- **Purpose**: Sample and analyze TSV files for structure
- **Key Features**:
  - Show file statistics (size, rows, columns)
  - Display sample data
  - Detect delimiters and encoding
  - Check for NULL bytes
  - Generate config snippet
- **Dependencies**: None (uses standard Unix tools)

### 4. **drop_month.sh** (Data Deletion Tool)
- **Purpose**: Safely delete monthly data from Snowflake
- **Key Features**:
  - Safety confirmations with warnings
  - Dry run mode
  - Preview mode
  - Multiple table support
  - JSON output for reports
- **Dependencies**: drop_month.py

### 5. **recover_failed_load.sh** (Error Recovery Tool)
- **Purpose**: Recover from failed COPY operations
- **Key Features**:
  - Diagnose Snowflake errors
  - Extract error details from logs
  - Provide recovery suggestions
  - Generate cleaned files
- **Dependencies**: diagnose_copy_error.py, recover_from_varchar_errors.py

## Common Patterns Identified

1. **Configuration**: All scripts use or generate config.json
2. **Color Output**: Consistent use of ANSI color codes
3. **Safety Features**: Dry run, preview, confirmation prompts
4. **Error Handling**: set -euo pipefail, error messages
5. **Progress Tracking**: Visual feedback for long operations
6. **Logging**: Debug logs to files, stdout for user feedback

## Proposed Unified Wrapper Design

### Architecture

```
snowflake_etl.sh (Main Menu)
├── 1. Data Loading Operations
│   ├── Load Single Month
│   ├── Load Multiple Months
│   ├── Load Direct Files
│   ├── Batch Load All Months
│   └── Parallel Processing
├── 2. Data Validation
│   ├── Validate in Snowflake
│   ├── File Quality Checks
│   ├── Compare TSV Files
│   └── Check Duplicates
├── 3. Configuration Management
│   ├── Generate Config from TSV
│   ├── Edit Config
│   ├── Test Connection
│   └── View Current Config
├── 4. File Analysis
│   ├── Sample TSV File
│   ├── Analyze File Structure
│   ├── Check for Issues
│   └── Compare Files
├── 5. Data Management
│   ├── Delete Month Data
│   ├── Delete Range
│   ├── Preview Deletion
│   └── Cleanup Stages
├── 6. Error Recovery
│   ├── Diagnose Failed Load
│   ├── Fix VARCHAR Errors
│   ├── Recover from Logs
│   └── Generate Clean Files
├── 7. Monitoring & Reports
│   ├── Check System Status
│   ├── View Recent Logs
│   ├── Generate Reports
│   └── Performance Stats
└── 8. Settings
    ├── Toggle Color Output
    ├── Set Default Paths
    ├── Configure Parallelism
    └── Set Log Level
```

### Menu System Design

#### Main Menu
```bash
╔════════════════════════════════════════════════════════╗
║           SNOWFLAKE ETL PIPELINE MANAGER              ║
╠════════════════════════════════════════════════════════╣
║  1) 📦 Data Loading      - Load TSV files             ║
║  2) ✓  Data Validation   - Validate data quality      ║
║  3) ⚙️  Configuration     - Manage configurations      ║
║  4) 🔍 File Analysis     - Analyze TSV files          ║
║  5) 🗑️  Data Management   - Delete/manage data         ║
║  6) 🔧 Error Recovery    - Fix failed operations      ║
║  7) 📊 Monitoring        - View logs and stats        ║
║  8) ⚡ Quick Actions     - Common tasks               ║
║  9) 🔨 Settings          - Configure options          ║
║  0) ❌ Exit                                            ║
╚════════════════════════════════════════════════════════╝
```

### Key Features

1. **Interactive Mode by Default**
   - Clear menu navigation
   - Input validation
   - Confirmation prompts for destructive operations
   - Progress indicators

2. **Non-Interactive Mode Support**
   - Command-line arguments for automation
   - Example: `./snowflake_etl.sh --load --month 2024-01`
   - Scriptable for cron jobs

3. **Smart Defaults**
   - Remember last used options
   - Auto-detect common paths
   - Suggest next logical action

4. **Unified Logging**
   - Central log directory
   - Timestamped operations
   - Color-coded severity levels

5. **State Management**
   - Track running operations
   - Resume interrupted tasks
   - Queue management for batch operations

### Implementation Strategy

#### Phase 1: Core Structure
1. Create main menu system
2. Integrate existing scripts as modules
3. Implement navigation and input handling
4. Add basic error handling

#### Phase 2: Enhanced Features
1. Add progress tracking across operations
2. Implement operation history
3. Add quick action shortcuts
4. Create configuration wizard

#### Phase 3: Advanced Features
1. Add job scheduling
2. Implement operation queuing
3. Add performance monitoring
4. Create reporting dashboard

### Benefits of Integration

1. **Single Entry Point**: Users only need to remember one command
2. **Discoverability**: All features visible in menu
3. **Consistency**: Unified interface and error handling
4. **Efficiency**: Common functions shared across operations
5. **Safety**: Centralized validation and confirmations
6. **Learning Curve**: Easier for new users to explore features

### Technical Considerations

1. **Modularity**: Keep existing scripts as callable functions
2. **Backward Compatibility**: Support direct script calls
3. **Configuration**: Central config file with overrides
4. **Dependencies**: Check all required tools on startup
5. **Performance**: Lazy loading of heavy operations

### Migration Path

1. **Week 1**: Implement basic menu structure
2. **Week 2**: Integrate loading and validation operations
3. **Week 3**: Add configuration and analysis tools
4. **Week 4**: Complete error recovery and monitoring
5. **Testing**: Comprehensive testing of all paths
6. **Documentation**: Update README and help texts

## Questions for Gemini Review

1. Should we use a TUI library (like dialog/whiptail) for better interface?
2. How should we handle long-running operations in the menu?
3. Should we implement a job queue for batch operations?
4. What's the best approach for maintaining backward compatibility?
5. Should we add REST API support for remote operations?
6. How to handle concurrent operations safely?
7. Should we implement undo/rollback functionality?
8. What level of operation logging is appropriate?