# Migration from Old Scripts to New CLI

## Command Mapping

### Loading Data

**Old:**
```bash
python3 tsv_loader.py --config config.json --base-path ./data --month 2024-01
python3 tsv_loader.py --config config.json --skip-qc --validate-in-snowflake
python3 tsv_loader.py --config config.json --validate-only --month 2024-01
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json load --base-path ./data --month 2024-01
python3 -m snowflake_etl.cli.main --config config.json load --skip-qc --validate-in-snowflake
python3 -m snowflake_etl.cli.main --config config.json validate --month 2024-01
```

### Deleting Data

**Old:**
```bash
python3 drop_month.py --config config.json --table MY_TABLE --month 2024-01 --dry-run
python3 drop_month.py --config config.json --table MY_TABLE --month 2024-01 --yes
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json delete --table MY_TABLE --month 2024-01 --dry-run
python3 -m snowflake_etl.cli.main --config config.json delete --table MY_TABLE --month 2024-01 --yes
```

### Validation

**Old:**
```bash
python3 tsv_loader.py --config config.json --validate-only --month 2024-01
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json validate --month 2024-01
python3 -m snowflake_etl.cli.main --config config.json validate --table MY_TABLE --output results.json
```

### Table Report

**Old:**
```bash
python3 generate_table_report.py --config-dir config --output-format both
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json report --output report.json
```

### Check Duplicates

**Old:**
```bash
python3 check_duplicates_interactive.py --config config.json --table MY_TABLE
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json check-duplicates --table MY_TABLE
```

### Compare Files

**Old:**
```bash
python3 compare_tsv_files.py file1.tsv file2.tsv --quick
```

**New:**
```bash
python3 -m snowflake_etl.cli.main --config config.json compare file1.tsv file2.tsv --quick
```

## Key Changes

1. **Single Entry Point**: All operations now go through `python3 -m snowflake_etl.cli.main`
2. **Subcommands**: Operations are now subcommands (load, delete, validate, etc.)
3. **Config Required**: Config is now a global required parameter
4. **Consistent Interface**: All operations follow the same pattern
5. **Better Organization**: Related options are grouped with their subcommands

## Environment Variables

The new CLI respects the same environment variables:
- `SNOWFLAKE_ETL_CONFIG`: Default config path
- `SNOWFLAKE_ETL_LOG_DIR`: Default log directory
- `SNOWFLAKE_ETL_LOG_LEVEL`: Default log level

## Script Updates Required

### snowflake_etl.sh
Replace all direct Python script calls with CLI calls.

### run_loader.sh  
Update the command building logic to use new CLI format.

### Job Management
The job management system remains the same - just the underlying commands change.