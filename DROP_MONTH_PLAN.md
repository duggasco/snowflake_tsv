# DROP_MONTH Feature Implementation Plan
*Created: 2025-08-21*

## Executive Summary
Add functionality to safely delete data from Snowflake tables by month, leveraging existing config structure and safety patterns from the ETL pipeline.

## Design Decision: Separate Script
**Decision**: Create a new `drop_month.py` script separate from `tsv_loader.py`

**Rationale**:
- **Safety**: Clear separation between loading (additive) and deletion (destructive) operations
- **Principle of Least Surprise**: Script name clearly indicates its purpose
- **Permission Control**: Different access controls can be applied in production
- **Single Responsibility**: Each script has one clear purpose

## Architecture Overview

### 1. Core Components

#### SnowflakeMonthDropper Class
```python
class SnowflakeMonthDropper:
    def __init__(self, connection_params: Dict, dry_run=False):
        """Initialize with Snowflake connection and safety mode"""
        
    def analyze_deletion(self, table_name, date_column, year_month):
        """Analyze impact without deleting - returns row count and sample"""
        
    def delete_month_data(self, table_name, date_column, year_month):
        """Execute deletion with transaction management"""
        
    def validate_date_column(self, table_name, date_column):
        """Verify date column exists and has expected format"""
```

#### CLI Interface
```bash
# Single month, single table
python drop_month.py --config config.json --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01

# Multiple months, multiple tables
python drop_month.py --config config.json --tables table1,table2 --months 2024-01,2024-02

# Dry run with preview
python drop_month.py --config config.json --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 --dry-run --preview

# Skip confirmation
python drop_month.py --config config.json --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 --yes

# From bash wrapper
./drop_month.sh --batch --months 2024-01,2024-02,2024-03
```

### 2. Safety Features (Priority Order)

#### Level 1: Critical Safety
1. **Interactive Confirmation (Default)**
   - Show exact impact: table, month, row count
   - Require explicit "yes" to proceed
   - Example: "⚠️ DELETE 1,234,567 rows from TEST_CUSTOM_FACTLENDINGBENCHMARK for 2024-01? This CANNOT be undone! Type 'yes' to confirm:"

2. **Dry Run Mode (`--dry-run`)**
   - Execute everything except DELETE and COMMIT
   - Show exact SQL that would run
   - Display row counts that would be affected
   - No actual data modification

3. **Preview Mode (`--preview`)**
   - With dry-run, show sample of rows to be deleted
   - `SELECT * FROM table WHERE date_condition LIMIT 10`
   - Visual confirmation of correct targeting

#### Level 2: Transaction Safety
1. **Atomic Operations**
   ```sql
   BEGIN TRANSACTION;
   -- Record pre-delete state
   SELECT COUNT(*) FROM table WHERE condition;
   -- Execute delete
   DELETE FROM table WHERE condition;
   -- Verify delete count matches expected
   COMMIT;
   -- On any error: ROLLBACK;
   ```

2. **Snowflake Time Travel Awareness**
   - Check DATA_RETENTION_TIME_IN_DAYS before deletion
   - Log timestamp before deletion for recovery reference
   - Include recovery instructions in logs

#### Level 3: Audit & Monitoring
1. **Comprehensive Logging**
   ```
   logs/drop_month_YYYYMMDD_HHMMSS.log
   - User/Role executing
   - Exact command and parameters
   - Connection details (excluding passwords)
   - SQL queries generated
   - Row counts affected
   - Transaction IDs
   - Success/failure status
   ```

2. **Result Summary**
   ```json
   {
     "execution_time": "2025-08-21T10:30:00",
     "user": "svcsla",
     "operations": [
       {
         "table": "TEST_CUSTOM_FACTLENDINGBENCHMARK",
         "month": "2024-01",
         "rows_deleted": 1234567,
         "status": "success",
         "duration_seconds": 45.2
       }
     ],
     "total_rows_deleted": 1234567
   }
   ```

### 3. Implementation Details

#### Date Range Calculation
```python
def get_date_range_for_month(year_month: str, format='YYYYMMDD'):
    """Convert YYYY-MM to start/end dates in YYYYMMDD format"""
    year, month = year_month.split('-')
    start_date = f"{year}{month:02d}01"
    
    # Calculate last day of month
    if month == 12:
        end_date = f"{year}{month:02d}31"
    else:
        next_month = datetime(int(year), int(month) + 1, 1)
        last_day = (next_month - timedelta(days=1)).day
        end_date = f"{year}{month:02d}{last_day:02d}"
    
    return int(start_date), int(end_date)
```

#### SQL Generation
```python
def generate_delete_sql(table_name, date_column, start_date, end_date):
    """Generate DELETE SQL with proper formatting"""
    return f"""
    DELETE FROM {table_name}
    WHERE {date_column} >= {start_date} 
      AND {date_column} <= {end_date}
    """
```

#### Performance Optimization
```python
def assess_deletion_impact(cursor, table_name, date_column, start_date, end_date):
    """Determine if standard DELETE or CTAS is more efficient"""
    
    # Get total row count
    total_rows = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    
    # Get deletion row count
    delete_rows = cursor.execute(f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE {date_column} >= {start_date} AND {date_column} <= {end_date}
    """).fetchone()[0]
    
    deletion_percentage = (delete_rows / total_rows) * 100 if total_rows > 0 else 0
    
    if deletion_percentage > 20:
        return {
            'method': 'CTAS_RECOMMENDED',
            'reason': f'Deleting {deletion_percentage:.1f}% of table',
            'warning': 'Consider using table rewrite for better performance'
        }
    else:
        return {
            'method': 'STANDARD_DELETE',
            'reason': f'Deleting {deletion_percentage:.1f}% of table',
            'warning': None
        }
```

### 4. Integration with Existing System

#### Config Reuse
- Use same JSON config structure as `tsv_loader.py`
- Leverage `snowflake` connection parameters
- Use `files` array to get table names and date columns

#### Logging Integration
- Follow same logging patterns as existing system
- Use same log directory structure
- Compatible format for log aggregation

#### Progress Tracking
- Use tqdm for progress bars (if available)
- Show progress for multiple table/month combinations
- Similar visual style to loader

### 5. Error Handling

#### Connection Errors
- Retry logic with exponential backoff
- Clear error messages about connection issues
- Fail safe (no deletion on connection problems)

#### Data Validation Errors
- Verify table exists
- Verify date column exists
- Check date column data type compatibility
- Validate month format (YYYY-MM)

#### Transaction Errors
- Always ROLLBACK on any error
- Log exact error for debugging
- Provide recovery instructions

### 6. Testing Strategy

#### Unit Tests
- Date range calculation
- SQL generation
- Config parsing
- Safety check logic

#### Integration Tests
- Mock Snowflake connection
- Test transaction rollback
- Verify dry-run doesn't delete
- Test confirmation prompts

#### Manual Testing Checklist
- [ ] Dry run shows correct counts
- [ ] Preview shows expected rows
- [ ] Confirmation prompt works
- [ ] --yes flag bypasses prompt
- [ ] Transaction rollback on error
- [ ] Logs capture all operations
- [ ] Multiple tables work correctly
- [ ] Multiple months work correctly

### 7. Documentation Updates

#### README.md Addition
```markdown
## Data Deletion (drop_month.py)

Remove data from Snowflake tables by month:

```bash
# Dry run to preview impact
python drop_month.py --config config.json --table MY_TABLE --month 2024-01 --dry-run

# Delete with confirmation
python drop_month.py --config config.json --table MY_TABLE --month 2024-01

# Delete multiple months from multiple tables
python drop_month.py --config config.json --tables table1,table2 --months 2024-01,2024-02
```

**Safety Features:**
- Interactive confirmation required (bypass with --yes)
- Dry run mode for impact analysis
- Transaction-based with automatic rollback
- Comprehensive audit logging
- Snowflake Time Travel for recovery
```

#### CLAUDE.md Addition
- Add drop_month.py to list of key components
- Document safety philosophy
- Include recovery procedures

### 8. Implementation Phases

#### Phase 1: Core Functionality (Day 1)
- [ ] Create drop_month.py with basic structure
- [ ] Implement SnowflakeMonthDropper class
- [ ] Add config parsing
- [ ] Basic DELETE functionality with transactions

#### Phase 2: Safety Features (Day 2)
- [ ] Add dry-run mode
- [ ] Implement confirmation prompts
- [ ] Add preview functionality
- [ ] Create comprehensive logging

#### Phase 3: Integration & Testing (Day 3)
- [ ] Create drop_month.sh bash wrapper
- [ ] Add unit tests
- [ ] Integration testing
- [ ] Documentation updates

#### Phase 4: Advanced Features (Future)
- [ ] CTAS optimization for large deletes
- [ ] Batch processing multiple configs
- [ ] Email notifications for deletions
- [ ] Web UI for deletion management

### 9. Success Criteria

- **Safety**: Zero accidental deletions in testing
- **Clarity**: Users understand exactly what will be deleted
- **Auditability**: Complete record of all deletions
- **Performance**: Efficient deletion even for large datasets
- **Recovery**: Clear path to restore deleted data if needed

### 10. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Accidental deletion | Multiple confirmation layers, dry-run default |
| Wrong month/table | Preview mode, clear impact display |
| Connection loss mid-delete | Transaction rollback |
| Permission issues | Separate script with restricted access |
| Performance impact | Impact assessment, off-hours scheduling |
| No recovery path | Time Travel verification, backup reminder |

---
*This plan provides a comprehensive roadmap for safely implementing month-based data deletion*