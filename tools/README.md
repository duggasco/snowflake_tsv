# Tools Directory

This directory contains utility scripts and diagnostic tools.

## Available Tools

### Diagnostic Tools
- `diagnose_column_mismatch.py` - Diagnose column mismatch errors in Snowflake loads
- `diagnose_tuple_error.py` - Debug tuple formatting errors

### UI Tools
- `tsv_file_browser.py` - Interactive TSV file browser with curses UI
- `tsv_browser_integration.py` - Integration helper for file browser

## Usage

### Column Mismatch Diagnosis
```bash
python tools/diagnose_column_mismatch.py --log error.log
```

### TSV File Browser
```bash
python tools/tsv_file_browser.py /path/to/data
```

## Note
Main ETL tools are integrated into:
- `snowflake_etl.sh` - Interactive menu system
- `snowflake_etl/` - Python package with CLI