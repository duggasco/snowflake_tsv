# Config Generator Tool Plan

## Overview
Create a comprehensive configuration generator for the Snowflake TSV Loader that can analyze TSV files and generate complete config.json files.

## Requirements

### 1. File Analysis
- Analyze single or multiple TSV files
- Auto-detect column names (if headers present)
- Support headerless files with custom column names
- Detect data types from sample data
- Identify date columns automatically
- Extract date ranges from filenames

### 2. Pattern Detection
- Recognize file naming patterns:
  - `{month}` pattern (e.g., file_2024-01.tsv)
  - `{date_range}` pattern (e.g., file_20240101-20240131.tsv)
  - Custom patterns
- Extract date information from filenames

### 3. Snowflake Configuration
- Interactive or non-interactive mode
- Prompt for Snowflake credentials (optional)
- Generate table names from file names
- Support custom table name mapping

### 4. Output Options
- Generate complete config.json
- Merge with existing config
- Output to stdout or file
- Support dry-run mode

## Proposed Script: `generate_config.sh`

### Usage
```bash
# Analyze single file
./generate_config.sh data/file_20240101-20240131.tsv

# Analyze multiple files
./generate_config.sh data/*.tsv

# With custom headers for headerless files
./generate_config.sh --headers "col1,col2,col3,date_col" data/file.tsv

# Interactive mode for Snowflake credentials
./generate_config.sh --interactive data/*.tsv

# Output to specific file
./generate_config.sh -o config/my_config.json data/*.tsv

# Merge with existing config
./generate_config.sh --merge config/existing.json data/new_file.tsv
```

### Features

1. **Auto-detection**:
   - Column count and names
   - Data types (string, integer, float, date)
   - Date formats (YYYY-MM-DD, YYYYMMDD, MM/DD/YYYY)
   - File patterns and date ranges

2. **Smart Defaults**:
   - Table names from file names (sanitized)
   - Date column detection (looks for common patterns)
   - Sensible data type mapping

3. **Validation**:
   - Check file accessibility
   - Verify column consistency across files
   - Validate date ranges

4. **Output Format**:
```json
{
  "snowflake": {
    "account": "",
    "user": "",
    "password": "",
    "warehouse": "",
    "database": "",
    "schema": "",
    "role": ""
  },
  "files": [
    {
      "file_pattern": "factLendingBenchmark_{date_range}.tsv",
      "table_name": "FACTLENDINGBENCHMARK",
      "expected_columns": ["col1", "col2", "..."],
      "date_column": "RECORDDATEID"
    }
  ]
}
```

## Implementation Steps

### Phase 1: Core Functionality
1. Create basic file analyzer (extend tsv_sampler.sh)
2. Implement pattern detection
3. Generate basic config structure

### Phase 2: Enhanced Features
1. Add multi-file support
2. Implement smart column type detection
3. Add date column auto-detection

### Phase 3: Interactive Mode
1. Add prompts for Snowflake credentials
2. Allow column mapping customization
3. Support config merging

### Phase 4: Validation & Testing
1. Add validation checks
2. Create test cases
3. Document usage

## Benefits

1. **Quick Setup**: Generate configs in seconds instead of manual creation
2. **Consistency**: Ensure all configs follow the same structure
3. **Error Reduction**: Auto-detect columns reduces typos
4. **Documentation**: Generated configs are self-documenting
5. **Flexibility**: Support various file formats and patterns

## Integration with Existing Tools

- Works alongside `tsv_sampler.sh` for detailed analysis
- Compatible with `tsv_loader.py` config format
- Can be called from `run_loader.sh` for initial setup

## Example Workflow

```bash
# 1. Analyze new TSV files
./generate_config.sh data/new_files/*.tsv -o config/new_config.json

# 2. Review and edit generated config
vi config/new_config.json

# 3. Test with analyze-only
./run_loader.sh --config config/new_config.json --analyze-only

# 4. Run actual load
./run_loader.sh --config config/new_config.json --month 2024-01
```

## Success Criteria

- Can generate valid configs for all existing file types
- Reduces config creation time from 30+ minutes to <5 minutes
- Auto-detects 90%+ of settings correctly
- Handles edge cases gracefully (empty files, missing headers, etc.)