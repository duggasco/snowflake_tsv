# Full Table Report Generation - Implementation Plan

## Overview
Add a "Generate Full Table Report" feature that analyzes all tables across all configuration files, providing comprehensive statistics and validation results.

## Requirements
1. **Iterate through all config files** in the config directory
2. **For each table in each config:**
   - Get total row count
   - Get column count and list
   - Run full validation (date completeness, duplicates, anomalies)
   - Collect statistics (date ranges, avg rows/day, etc.)
3. **Generate comprehensive summary report** at the end
4. **Display in job logs** for viewing through Job Status menu

## Technical Design

### 1. Core Python Script: `generate_table_report.py`

```python
#!/usr/bin/env python3
"""
Generate comprehensive report for all tables across all configs
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List
import snowflake.connector
import logging
from datetime import datetime
from dataclasses import dataclass, asdict

@dataclass
class TableReport:
    """Report data for a single table"""
    config_file: str
    table_name: str
    status: str  # SUCCESS, ERROR, SKIPPED
    row_count: int = 0
    column_count: int = 0
    columns: List[str] = None
    date_column: str = None
    date_range_start: str = None
    date_range_end: str = None
    unique_dates: int = 0
    expected_dates: int = 0
    missing_dates: List[str] = None
    gaps: int = 0
    anomalous_dates: int = 0
    duplicate_keys: int = 0
    duplicate_rows: int = 0
    avg_rows_per_day: float = 0
    validation_status: str = "NOT_RUN"
    validation_details: Dict = None
    error_message: str = None
    execution_time: float = 0

class TableReportGenerator:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.reports = []
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        # Set up logging
        pass
        
    def generate_all_reports(self):
        """Main method to generate reports for all tables"""
        configs = self._load_all_configs()
        
        for config_path, config_data in configs.items():
            self._process_config(config_path, config_data)
            
        return self._generate_summary()
    
    def _load_all_configs(self) -> Dict:
        """Load all JSON configs from config directory"""
        configs = {}
        for config_file in self.config_dir.glob("*.json"):
            try:
                with open(config_file) as f:
                    configs[str(config_file)] = json.load(f)
            except Exception as e:
                self.logger.error(f"Failed to load {config_file}: {e}")
        return configs
    
    def _process_config(self, config_path: str, config_data: Dict):
        """Process all tables in a single config"""
        sf_config = config_data.get('snowflake', {})
        
        # Connect to Snowflake
        try:
            conn = snowflake.connector.connect(**sf_config)
        except Exception as e:
            self.logger.error(f"Failed to connect for {config_path}: {e}")
            return
            
        # Process each file/table definition
        for file_config in config_data.get('files', []):
            table_name = file_config.get('table_name')
            if table_name:
                report = self._analyze_table(
                    conn, config_path, table_name, file_config
                )
                self.reports.append(report)
                
        conn.close()
    
    def _analyze_table(self, conn, config_path: str, 
                       table_name: str, file_config: Dict) -> TableReport:
        """Analyze a single table"""
        report = TableReport(
            config_file=os.path.basename(config_path),
            table_name=table_name
        )
        
        try:
            cursor = conn.cursor()
            
            # 1. Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            report.row_count = cursor.fetchone()[0]
            
            # 2. Get column information
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cursor.fetchall()]
            report.columns = columns
            report.column_count = len(columns)
            
            # 3. Run validation if date column exists
            date_column = file_config.get('date_column')
            if date_column and report.row_count > 0:
                validation_results = self._validate_table(
                    cursor, table_name, date_column, file_config
                )
                report.validation_details = validation_results
                # Update report with validation results
                self._update_report_from_validation(report, validation_results)
            
            report.status = "SUCCESS"
            
        except Exception as e:
            report.status = "ERROR"
            report.error_message = str(e)
            
        return report
    
    def _validate_table(self, cursor, table_name: str, 
                        date_column: str, file_config: Dict) -> Dict:
        """Run comprehensive validation on table"""
        # Similar to SnowflakeDataValidator.validate_date_completeness
        # but for ALL data, not just a specific date range
        pass
    
    def _generate_summary(self) -> str:
        """Generate formatted summary report"""
        # Create beautiful ASCII table summary
        pass
```

### 2. Integration with snowflake_etl.sh

```bash
# Add to menu_snowflake_operations
generate_full_table_report() {
    echo -e "${BLUE}Generating comprehensive table report...${NC}"
    echo -e "${YELLOW}This will analyze all tables in all config files${NC}"
    
    if confirm_action "Generate full table report for all configs?"; then
        # Count total tables to process
        local total_tables=0
        for config in config/*.json; do
            [[ -f "$config" ]] || continue
            local table_count=$(jq '.files | length' "$config" 2>/dev/null || echo 0)
            total_tables=$((total_tables + table_count))
        done
        
        echo -e "${BLUE}Found $total_tables table(s) to analyze across all configs${NC}"
        
        # Start job
        with_lock start_background_job "full_table_report_$(date +%Y%m%d_%H%M%S)" \
            python3 generate_table_report.py --config-dir config --output-format both
    fi
}
```

### 3. Report Format Design

```
================================================================================
                     SNOWFLAKE TABLES COMPREHENSIVE REPORT
                          Generated: 2025-01-22 14:30:00
================================================================================

SUMMARY STATISTICS
------------------
Total Configurations: 2
Total Tables Analyzed: 5
Successful: 4
Failed: 1
Total Rows Processed: 125,450,000
Total Validation Issues: 3

================================================================================
DETAILED TABLE REPORTS
================================================================================

[1] Config: factLendingBenchmark_config.json
--------------------------------------------------------------------------------

Table: FACTLENDINGBENCHMARK
Status: SUCCESS
Statistics:
  - Total Rows: 50,000,000
  - Columns: 41
  - Date Range: 2024-01-01 to 2024-12-31
  - Unique Dates: 365
  - Avg Rows/Day: 136,986

Validation Results: PASSED
  - Date Completeness: 365/365 (100%)
  - Gaps Detected: 0
  - Anomalous Dates: 0
  - Duplicate Keys: 0
  
Column List:
  RECORDDATE, RECORDDATEID, ASSETID, FUNDID, LENDINGRATE, 
  QUANTITY, MARKETVALUE, ... (36 more)

--------------------------------------------------------------------------------

Table: FACTASSETDETAILS
Status: SUCCESS
Statistics:
  - Total Rows: 25,000,000
  - Columns: 15
  - Date Range: 2024-01-01 to 2024-12-31
  - Unique Dates: 365
  - Avg Rows/Day: 68,493

Validation Results: WARNING
  - Date Completeness: 363/365 (99.5%)
  - Missing Dates: 2024-07-04, 2024-12-25
  - Gaps Detected: 2
  - Anomalous Dates: 5 (low row counts)
    * 2024-01-01: 12,000 rows (17% of average)
    * 2024-07-05: 15,000 rows (22% of average)
    ...
  - Duplicate Keys: 150 (affecting 300 rows)

--------------------------------------------------------------------------------

[2] Config: generated_config.json
--------------------------------------------------------------------------------

Table: TEST_CUSTOM_TABLE
Status: ERROR
Error: Table does not exist in schema

================================================================================
VALIDATION SUMMARY
================================================================================

Tables with Issues (3):
1. FACTASSETDETAILS - 2 missing dates, 5 anomalies, 150 duplicates
2. METRICS_TABLE - 10 missing dates
3. REPORT_DATA - 1,000 duplicate keys

Recommended Actions:
- Investigate missing dates in FACTASSETDETAILS (holidays?)
- Review duplicate key constraints
- Verify TEST_CUSTOM_TABLE creation

================================================================================
Report Complete - Execution Time: 45.3 seconds
================================================================================
```

### 4. Features to Include

1. **Parallel Processing Option**
   - Process multiple tables concurrently
   - Use multiprocessing for large numbers of tables

2. **Export Options**
   - JSON format for programmatic access
   - CSV export for Excel analysis
   - HTML report with charts (future)

3. **Filtering Options**
   - Specific config file only
   - Specific tables only
   - Skip validation for faster reports

4. **Caching**
   - Cache results for X minutes
   - Option to force refresh

### 5. Implementation Steps

1. **Create generate_table_report.py**
   - Core analysis logic
   - Snowflake connection pooling
   - Parallel processing support

2. **Update snowflake_etl.sh**
   - Add menu option
   - Job management integration

3. **Format report output**
   - ASCII tables for terminal
   - Color coding for issues
   - Summary at bottom for job logs

4. **Testing**
   - Multiple configs
   - Large tables
   - Missing tables
   - Permission errors

### 6. Job Log Integration

The report will be structured so that when viewed through Job Status -> View Logs:
- Progress indicators during execution
- Full detailed report at the end
- Summary always visible at bottom of log

### 7. Error Handling

- Connection failures per config
- Missing tables
- Permission errors
- Timeout handling for large tables
- Graceful degradation (partial reports)

## Benefits

1. **Comprehensive Overview** - See all tables at once
2. **Proactive Monitoring** - Identify issues before they impact downstream
3. **Documentation** - Automatic inventory of all tables
4. **Validation** - Consistent quality checks across all data
5. **Audit Trail** - Historical reports for compliance

## Next Steps

1. Implement core Python script
2. Add menu integration  
3. Test with existing configs
4. Add export capabilities
5. Document usage