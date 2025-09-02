# Deprecated Scripts Directory

⚠️ **WARNING**: These scripts are deprecated as of v3.4.0

All functionality has been consolidated into `snowflake_etl.sh`.

## Deprecated Scripts

### Wrapper Scripts (Removed in v3.4.0)
- `run_loader.sh` - Loading operations → Use snowflake_etl.sh menu
- `drop_month.sh` - Deletion operations → Use snowflake_etl.sh menu
- `generate_config.sh` - Config generation → Use snowflake_etl.sh menu
- `tsv_sampler.sh` - TSV sampling → Use snowflake_etl.sh menu
- `recover_failed_load.sh` - Recovery → Use `python -m snowflake_etl diagnose-error`

## Migration Guide

| Old Command | New Method |
|-------------|------------|
| `./run_loader.sh --month 2024-01` | Menu: Quick Load > Month options |
| `./drop_month.sh --table X` | Menu: Snowflake Operations > Delete |
| `./generate_config.sh files` | Menu: File Tools > Generate Config |
| `./tsv_sampler.sh file.tsv` | Menu: File Tools > Sample TSV |
| `./recover_failed_load.sh` | `python -m snowflake_etl diagnose-error` |

## Why Deprecated?

These scripts were consolidated to:
- Reduce maintenance overhead
- Eliminate inter-script dependencies
- Provide a unified user experience
- Improve performance with direct function calls

## DO NOT USE
These scripts are kept only for reference. Use `snowflake_etl.sh` instead.