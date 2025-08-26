# Snowflake ETL CLI Syntax Guide

## IMPORTANT: Argument Order

Global flags MUST come BEFORE the subcommand. This is a requirement of Python's argparse when using subparsers.

### Correct Order:
```bash
python3 -m snowflake_etl [GLOBAL_FLAGS] SUBCOMMAND [SUBCOMMAND_OPTIONS]
```

### Global Flags (must come BEFORE subcommand):
- `--config CONFIG_FILE` or `-c CONFIG_FILE` (REQUIRED)
- `--log-dir LOG_DIR` (optional, default: logs)
- `--log-level {DEBUG,INFO,WARNING,ERROR}` (optional, default: INFO)  
- `--quiet` or `-q` (optional)

## Correct Examples

### Load Operation
```bash
# CORRECT - config before subcommand
python3 -m snowflake_etl --config config.json load --month 2024-01 --base-path /data

# WRONG - config after subcommand
python3 -m snowflake_etl load --config config.json --month 2024-01  # Will fail!
```

### Delete Operation
```bash
# CORRECT
python3 -m snowflake_etl --config config.json delete --table MY_TABLE --month 2024-01

# With optional global flags
python3 -m snowflake_etl --config config.json --quiet --log-level DEBUG delete --table MY_TABLE --month 2024-01

# WRONG
python3 -m snowflake_etl delete --config config.json --table MY_TABLE  # Will fail!
```

### Validate Operation
```bash
# CORRECT
python3 -m snowflake_etl --config config.json validate --table MY_TABLE --month 2024-01

# WRONG
python3 -m snowflake_etl validate --config config.json --table MY_TABLE  # Will fail!
```

### Report Operation
```bash
# CORRECT
python3 -m snowflake_etl --config config.json report --output report.txt

# WRONG
python3 -m snowflake_etl report --config config.json --output report.txt  # Will fail!
```

### Check Duplicates Operation
```bash
# CORRECT
python3 -m snowflake_etl --config config.json check-duplicates --table MY_TABLE --key-columns col1,col2

# WRONG
python3 -m snowflake_etl check-duplicates --config config.json --table MY_TABLE  # Will fail!
```

### Compare Operation
```bash
# CORRECT
python3 -m snowflake_etl --config config.json compare --file1 file1.tsv --file2 file2.tsv

# WRONG
python3 -m snowflake_etl compare --config config.json --file1 file1.tsv  # Will fail!
```

## Files Fixed (2025-08-26)

The following shell scripts have been corrected to use proper argument ordering:

1. **drop_month.sh** (Line 169)
   - Was: `python3 -m snowflake_etl delete --config $CONFIG`
   - Now: `python3 -m snowflake_etl --config $CONFIG delete`

2. **run_loader.sh** (Lines 131, 270)  
   - Was: `python3 -m snowflake_etl load` then appending `--config`
   - Now: `python3 -m snowflake_etl --config ${CONFIG_FILE} load`

3. **snowflake_etl.sh** - Already correct, no changes needed

## Why This Matters

Python's argparse library with subparsers requires that:
1. Parent parser arguments (global flags) are processed first
2. Then the subcommand is identified
3. Finally, subcommand-specific arguments are processed

Putting `--config` after the subcommand causes argparse to treat it as a subcommand argument, not a global flag, resulting in "unrecognized arguments" errors.

## Testing the Fix

To verify the fix works, run:
```bash
# Test delete command
./drop_month.sh --config config.json --table TEST_TABLE --month 2024-01 --dry-run

# Test load command  
./run_loader.sh --config config.json --month 2024-01 --validate-only
```

Both should now work without "unrecognized arguments: --config" errors.