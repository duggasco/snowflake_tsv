# Config Generator Guide - How to Use generate_config.sh

## Overview
The `generate_config.sh` script helps create config.json files for the Snowflake TSV Loader. It can work in several modes depending on what information you provide.

## How It Works

### 1. Basic Mode (No Snowflake Connection)
When you just point to a TSV file without any Snowflake options:
```bash
./generate_config.sh data/factLendingBenchmark_20240101-20240131.tsv
```

**What happens:**
- Extracts table name from filename: `factLendingBenchmark_20240101-20240131.tsv` → `FACTLENDINGBENCHMARK`
- Detects pattern: `factLendingBenchmark_{date_range}.tsv`
- Counts columns in the file
- Generates generic column names: `column1, column2, column3...`
- Creates empty Snowflake credentials section

**Result:** A basic config that needs manual column name updates

### 2. With Snowflake Table Query (-t flag)
When you specify a Snowflake table name AND provide credentials:
```bash
./generate_config.sh -t TEST_CUSTOM_FACTLENDINGBENCHMARK -c config/existing.json data/file.tsv
```

**What happens:**
- Uses credentials from existing config to connect to Snowflake
- Queries the specified table's schema from `information_schema.columns`
- Gets actual column names and types from Snowflake
- Uses these column names in the generated config

**Requirements:**
- `-t TABLE_NAME`: The exact Snowflake table name
- `-c CONFIG_FILE`: An existing config with valid Snowflake credentials
- Snowflake connectivity (may fail if can't connect)

### 3. With Manual Column Headers (-h flag)
When you know the column names but can't/don't want to query Snowflake:
```bash
./generate_config.sh -h "RECORDDATE,RECORDDATEID,ASSETID,..." data/file.tsv
```

**What happens:**
- Uses your provided column names exactly as given
- No Snowflake connection needed
- Best for headerless TSV files when you know the schema

### 4. Interactive Mode (-i flag)
For entering Snowflake credentials interactively:
```bash
./generate_config.sh -i -t TABLE_NAME data/file.tsv
```

**What happens:**
- Prompts for Snowflake account, user, password, etc.
- Creates temporary config with these credentials
- Then queries Snowflake for column names (if -t provided)

## Important Notes

### Table Name Detection
The script does **NOT** automatically check if a Snowflake table exists. Instead:
- It **extracts** a table name from the filename (e.g., `factLendingBenchmark_*.tsv` → `FACTLENDINGBENCHMARK`)
- It **only queries** Snowflake if you explicitly use `-t TABLE_NAME` with credentials
- The extracted name is just a suggestion - you can override with `-t`

### When to Use Each Mode

**Use Basic Mode when:**
- You're just starting and want a template
- You don't have Snowflake access yet
- You'll manually update column names later

**Use -t with Snowflake when:**
- The Snowflake table already exists
- You want accurate column names from the database
- You have working Snowflake credentials

**Use -h manual headers when:**
- You know the exact column names
- Snowflake connection isn't available
- You're working with headerless TSV files

## Complete Examples

### Example 1: Generate config using existing Snowflake table
```bash
# This queries TEST_CUSTOM_FACTLENDINGBENCHMARK table for column names
./generate_config.sh \
  -t TEST_CUSTOM_FACTLENDINGBENCHMARK \
  -c config/existing_with_creds.json \
  -o config/new_config.json \
  data/factLendingBenchmark_20240101-20240131.tsv
```

### Example 2: Generate config with known column headers
```bash
# This uses your provided column names, no Snowflake connection
./generate_config.sh \
  -h "RECORDDATE,RECORDDATEID,ASSETID,DXIDENTIFIER,ISIN" \
  -o config/my_config.json \
  data/myfile.tsv
```

### Example 3: Interactive mode for new setup
```bash
# This prompts for credentials, then queries Snowflake
./generate_config.sh \
  -i \
  -t MY_SNOWFLAKE_TABLE \
  -o config/fresh_config.json \
  data/*.tsv
```

### Example 4: Basic template generation
```bash
# This creates a basic template with generic column names
./generate_config.sh data/some_file.tsv > config/template.json
```

## Workflow Recommendations

### For New Files
1. Start with basic generation to get the structure
2. If table exists in Snowflake, re-run with `-t` flag
3. Otherwise, manually update column names

### For Existing Tables
1. Always use `-t TABLE_NAME` with `-c credentials.json`
2. This ensures column names match exactly with Snowflake

### For Headerless TSVs
1. Get column names from documentation or Snowflake
2. Use `-h "col1,col2,col3..."` to specify them
3. Or use `-t` if table exists in Snowflake

## Troubleshooting

### "Could not query Snowflake"
- Check credentials in the config file
- Verify network connectivity
- Ensure table name is correct
- Try with `-h` flag instead

### Wrong table name detected
- Use `-t` to override: `-t CORRECT_TABLE_NAME`
- The auto-detection is just a guess from filename

### Empty column array
- Snowflake query failed or returned no results
- Table might not exist
- Try `-h` with manual column names

## Key Takeaway
The script is a **helper tool** that can:
- Auto-detect patterns from filenames
- Query Snowflake for column info (when told to)
- Generate proper config structure

But it does **NOT** automatically:
- Connect to Snowflake without credentials
- Verify table existence without `-t` flag
- Create tables in Snowflake