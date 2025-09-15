# CSV Processing User Guide

## Overview

The Snowflake ETL Pipeline now fully supports CSV files alongside TSV files. This guide covers everything you need to know about processing CSV files with the pipeline.

## Quick Start

### 1. Basic CSV Loading

```bash
# Load a single CSV file
./snowflake_etl.sh load --file sales_2024.csv

# Load all CSV files for a month
./snowflake_etl.sh load --base-path /data --month 2024-01

# Load compressed CSV files
./snowflake_etl.sh load --file sales_2024.csv.gz
```

### 2. Configuration

Create a configuration file with CSV-specific settings:

```json
{
  "snowflake": {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
  },
  "files": [
    {
      "file_pattern": "sales_{month}.csv",
      "table_name": "SALES_DATA",
      "file_format": "CSV",
      "delimiter": ",",
      "quote_char": "\"",
      "date_column": "sale_date",
      "expected_columns": ["sale_date", "product_id", "customer_id", "amount"],
      "duplicate_key_columns": ["sale_date", "product_id"]
    }
  ]
}
```

## File Format Detection

### Automatic Detection

The pipeline automatically detects file formats based on:

1. **File Extension**
   - `.csv` → CSV format with comma delimiter
   - `.tsv` → TSV format with tab delimiter
   - `.txt` → Analyzes content to detect delimiter

2. **Content Analysis**
   - Samples file content to identify delimiter
   - Detects quoted fields
   - Identifies header rows

### Manual Configuration

Override automatic detection by specifying format in config:

```json
{
  "file_format": "CSV",      // Force CSV format
  "delimiter": ",",           // Explicit delimiter
  "quote_char": "\""          // Quote character
}
```

## Common CSV Scenarios

### 1. Standard CSV Files

Most common format with comma delimiter and quoted fields:

```csv
"product_id","product_name","price","quantity"
"P001","Widget A","19.99","100"
"P002","Widget B","29.99","50"
```

Configuration:
```json
{
  "file_pattern": "products_{month}.csv",
  "file_format": "CSV",
  "delimiter": ",",
  "quote_char": "\""
}
```

### 2. CSV with Embedded Commas

When data contains commas, proper quoting is essential:

```csv
company,address,revenue
"Acme, Inc.","123 Main St, Suite 100",1000000
"Beta Corp","456 Oak Ave",500000
```

The pipeline handles this automatically when quote_char is set.

### 3. Pipe-Delimited Files

Common in data exports from legacy systems:

```csv
ID|Name|Date|Amount
001|John Smith|2024-01-01|100.50
002|Jane Doe|2024-01-02|200.75
```

Configuration:
```json
{
  "file_pattern": "data_{date_range}.txt",
  "file_format": "CSV",
  "delimiter": "|"
}
```

### 4. Mixed Format Directories

Process both CSV and TSV files in the same directory:

```json
{
  "files": [
    {
      "file_pattern": "sales_{month}.csv",
      "file_format": "CSV",
      "table_name": "SALES"
    },
    {
      "file_pattern": "inventory_{month}.tsv",
      "file_format": "TSV",
      "table_name": "INVENTORY"
    }
  ]
}
```

## Advanced Features

### 1. Custom Delimiters

Support for any single-character delimiter:

```json
{
  "delimiter": ";",    // Semicolon
  "delimiter": ":",    // Colon
  "delimiter": "~"     // Tilde
}
```

### 2. Headerless Files

Process files without headers:

```json
{
  "file_pattern": "data_{date}.csv",
  "expected_columns": ["col1", "col2", "col3", "col4"]
}
```

### 3. Compressed Files

Native support for gzipped files:

```bash
# Process compressed CSV directly
./snowflake_etl.sh load --file data.csv.gz

# Compress CSV for transfer
./snowflake_etl.sh compress --file large_file.csv
```

## Performance Optimization

### For Large CSV Files (>1GB)

1. **Skip file-based quality checks**:
```bash
./snowflake_etl.sh load --file large.csv --skip-qc --validate-in-snowflake
```

2. **Use appropriate warehouse size**:
```json
{
  "snowflake": {
    "warehouse": "LARGE_WH"  // For files >10GB
  }
}
```

3. **Enable parallel processing**:
```bash
./snowflake_etl.sh load --base-path /data --max-workers 8
```

### Memory Management

- Files are processed in streaming mode
- No full file loading into memory
- Chunked compression for large files

## Validation

### 1. Pre-Load Validation

Automatic checks before loading:
- Column count verification
- Date format validation
- Schema compliance
- Duplicate detection (if configured)

### 2. Post-Load Validation

Verify data in Snowflake:
```bash
./snowflake_etl.sh validate --table SALES_DATA --month 2024-01
```

Checks performed:
- Date completeness
- Row count verification
- Duplicate analysis
- Data distribution

### 3. Sample Files

Preview file structure before processing:
```bash
./snowflake_etl.sh sample --file sales.csv --rows 100
```

Output includes:
- Detected format and delimiter
- Column headers
- Sample data rows
- File statistics

## Troubleshooting

### Issue: Format Not Detected

**Symptom**: File processed with wrong delimiter

**Solution**:
1. Ensure correct file extension
2. Explicitly set format in config
3. Check for consistent delimiters in file

### Issue: Quoted Fields Not Handled

**Symptom**: Fields with commas split incorrectly

**Solution**:
```json
{
  "quote_char": "\"",
  "file_format": "CSV"
}
```

### Issue: Special Characters in Data

**Symptom**: Data corruption or load errors

**Solutions**:
- Use TSV format if data has many commas
- Use pipe delimiter for complex data
- Ensure proper UTF-8 encoding

### Issue: Performance Problems

**Symptom**: Slow processing of large CSV files

**Solutions**:
1. Skip file QC: `--skip-qc`
2. Use larger warehouse
3. Compress files first
4. Process in smaller batches

## Best Practices

### 1. File Naming

Use consistent patterns:
- `sales_2024-01.csv` for monthly files
- `inventory_20240101-20240131.csv` for date ranges
- Include format in filename for clarity

### 2. Configuration Management

- Keep separate configs for CSV and TSV files
- Use explicit format specifications
- Document delimiter choices

### 3. Data Quality

- Always validate after loading
- Set up duplicate key columns
- Use appropriate date formats

### 4. Performance

- Pre-compress large files
- Use appropriate warehouse sizes
- Process files in parallel when possible

## Migration from TSV

### Converting Existing Configs

Update existing TSV configs for CSV:

```json
// Old TSV config
{
  "file_pattern": "data_{month}.tsv",
  "table_name": "DATA_TABLE"
}

// New CSV config
{
  "file_pattern": "data_{month}.csv",
  "table_name": "DATA_TABLE",
  "file_format": "CSV",
  "delimiter": ","
}
```

### Processing Both Formats

During migration, process both formats:

```json
{
  "files": [
    {
      "file_pattern": "data_{month}.tsv",
      "file_format": "TSV",
      "table_name": "DATA_TABLE"
    },
    {
      "file_pattern": "data_{month}.csv",
      "file_format": "CSV",
      "table_name": "DATA_TABLE"
    }
  ]
}
```

## Command Reference

### Load Commands

```bash
# Load with automatic format detection
snowflake-etl load --file data.csv

# Force specific format
snowflake-etl load --file data.txt --format CSV --delimiter "|"

# Load compressed
snowflake-etl load --file data.csv.gz

# Skip quality checks
snowflake-etl load --file large.csv --skip-qc
```

### Analysis Commands

```bash
# Sample file
snowflake-etl sample --file data.csv

# Generate config from files
snowflake-etl config-generate --files *.csv

# Validate loaded data
snowflake-etl validate --table TABLE_NAME
```

### Utility Commands

```bash
# Compress file
snowflake-etl compress --file large.csv

# Compare files
snowflake-etl compare --file1 old.csv --file2 new.csv

# Check for issues
snowflake-etl check-issues --file problematic.csv
```

## Support

For additional help:
- Check logs in `logs/` directory
- Enable debug mode: `--log-level DEBUG`
- Review error messages for format-specific issues
- Consult the troubleshooting section in README

## Summary

The CSV support in Snowflake ETL Pipeline provides:
- ✅ Automatic format detection
- ✅ Multiple delimiter support
- ✅ Quoted field handling
- ✅ Compressed file support
- ✅ Mixed format processing
- ✅ Full backward compatibility

Process your CSV files with confidence using the same powerful features available for TSV files!