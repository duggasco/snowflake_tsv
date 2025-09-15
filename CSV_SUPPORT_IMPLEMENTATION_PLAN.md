# CSV Support Implementation Plan
*Created: 2025-09-04*
*Target Version: 3.5.0*

## Executive Summary

This document outlines the implementation plan for adding CSV (comma-separated values) support to the existing Snowflake ETL Pipeline, which currently only supports TSV (tab-separated values) files. The implementation will maintain full backward compatibility while adding automatic format detection and explicit format configuration options.

## Goals & Requirements

### Primary Goals
1. **Add CSV Support**: Enable processing of .csv and .csv.gz files
2. **Maintain TSV Compatibility**: Zero breaking changes for existing TSV workflows
3. **Automatic Detection**: Intelligently detect file format when possible
4. **Explicit Configuration**: Allow users to specify format when needed
5. **Consistent Experience**: Same features for CSV as TSV (compression, validation, etc.)

### Non-Functional Requirements
- Performance parity with TSV processing
- No increase in memory consumption
- Clear error messages for format mismatches
- Comprehensive logging of format detection

## Architecture Overview

### Current State
- Hardcoded tab delimiter (`\t`) in multiple locations
- TSV-specific file pattern matching
- TSV-focused documentation and UI labels
- Fixed Snowflake COPY format with tab delimiter

### Target State
- Configurable delimiter throughout the pipeline
- Support for multiple file extensions (.tsv, .csv, .txt)
- Format-aware processing with automatic detection
- Dynamic Snowflake COPY format based on file type

## Implementation Components

### 1. Configuration Layer Changes

#### 1.1 File Configuration Model (`models/file_config.py`)
```python
@dataclass
class FileConfig:
    # ... existing fields ...
    delimiter: str = '\t'  # New field with default
    file_format: str = 'TSV'  # New field: 'TSV', 'CSV', or 'AUTO'
    quote_char: Optional[str] = '"'  # New field for quoted fields
```

#### 1.2 JSON Configuration Schema
```json
{
  "files": [{
    "file_pattern": "data_{date_range}.csv",
    "file_format": "CSV",  // Optional: TSV, CSV, or AUTO (default: AUTO)
    "delimiter": ",",       // Optional: explicit delimiter (default: auto-detect)
    "quote_char": "\"",     // Optional: quote character (default: ")
    // ... existing fields ...
  }]
}
```

### 2. Delimiter Detection Module

Create new module: `snowflake_etl/utils/format_detector.py`

```python
class FormatDetector:
    """Intelligent file format and delimiter detection"""
    
    @staticmethod
    def detect_format(file_path: str, sample_lines: int = 10) -> Dict:
        """
        Detect file format based on:
        1. File extension (.csv, .tsv, .txt)
        2. Content analysis (delimiter frequency)
        3. Header structure
        
        Returns:
            {
                'format': 'CSV' | 'TSV',
                'delimiter': ',' | '\t' | '|' | ';',
                'has_header': bool,
                'quote_char': '"' | "'" | None,
                'confidence': float  # 0.0 to 1.0
            }
        """
        
    @staticmethod
    def validate_delimiter(file_path: str, delimiter: str) -> bool:
        """Validate that the specified delimiter works for the file"""
```

### 3. Core Module Updates

#### 3.1 Data Quality Validator (`validators/data_quality.py`)
- Already has delimiter parameter - just needs to use it from config
- Update default from `'\t'` to detection-based

#### 3.2 Snowflake Loader (`core/snowflake_loader.py`)
```python
def _build_copy_query(self, table_name: str, stage_name: str, 
                      file_format: str = 'TSV', delimiter: str = '\t') -> str:
    """Build COPY INTO query with dynamic format settings."""
    
    # Map format to appropriate delimiter
    field_delimiter = delimiter if delimiter else (',' if file_format == 'CSV' else '\t')
    
    return f"""
    COPY INTO {table_name}
    FROM {stage_name}
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = '{field_delimiter}'
        SKIP_HEADER = 0
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ESCAPE_UNENCLOSED_FIELD = '\\134'
        NULL_IF = ('NULL', 'null', '', '\\N')
        COMPRESSION = 'AUTO'
    )
    ON_ERROR = 'ABORT_STATEMENT'
    PURGE = TRUE
    """
```

#### 3.3 File Pattern Matching (`__main__.py`)
Update pattern matching to accept multiple extensions:
```python
# Current: only checks .tsv and .tsv.gz
patterns_to_check = [file_pattern]
if not file_pattern.endswith('.gz'):
    patterns_to_check.append(file_pattern + '.gz')

# New: check multiple formats
extensions = ['.tsv', '.csv', '.txt']
patterns_to_check = []
for ext in extensions:
    patterns_to_check.append(base_pattern + ext)
    patterns_to_check.append(base_pattern + ext + '.gz')
```

### 4. UI/UX Updates

#### 4.1 Shell Script Updates (`snowflake_etl.sh`)
- Update menu labels from "TSV" to "TSV/CSV"
- Update file selection prompts
- Add format selection when ambiguous

#### 4.2 Help Text & Messages
- Update all user-facing messages
- Add format indicators in progress displays
- Show detected format in logs

### 5. Testing Strategy

#### 5.1 Unit Tests
- Test delimiter detection algorithm
- Test format validation
- Test COPY query generation with different formats

#### 5.2 Integration Tests
- Process sample CSV files
- Process mixed TSV/CSV batches
- Test auto-detection accuracy
- Test explicit format override

#### 5.3 Regression Tests
- Ensure all existing TSV workflows still work
- Verify performance hasn't degraded
- Check memory usage remains constant

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
1. Create FormatDetector module with detection logic
2. Update FileConfig model with new fields
3. Add delimiter parameter threading through modules
4. Update Snowflake COPY query generation

### Phase 2: File Discovery & Pattern Matching (Week 1)
1. Update file pattern matching for multiple extensions
2. Modify file discovery logic
3. Update config generation to detect format
4. Add format validation to config parser

### Phase 3: Processing Pipeline (Week 2)
1. Update DataQualityValidator to use config delimiter
2. Update SnowflakeLoader with dynamic COPY format
3. Update progress displays to show format
4. Add format info to logs

### Phase 4: UI & Documentation (Week 2)
1. Update shell script menus and prompts
2. Update CLI help text
3. Update README and CLAUDE.md
4. Create CSV processing examples

### Phase 5: Testing & Validation (Week 3)
1. Create comprehensive test suite
2. Test with real CSV files
3. Performance benchmarking
4. User acceptance testing

## Migration Strategy

### For Existing Users
1. **No Action Required**: Existing TSV configs continue to work
2. **Opt-in CSV**: Users explicitly configure CSV files
3. **Auto-Detection**: Can enable AUTO mode for mixed formats

### Configuration Migration
- Add migration tool to update existing configs with format field
- Default all existing configs to TSV format for safety
- Provide validation tool to check configs

## Risk Analysis

### Technical Risks
1. **Delimiter Ambiguity**: Some files might use multiple delimiters
   - Mitigation: Confidence scoring and user override options
   
2. **Performance Impact**: Detection adds overhead
   - Mitigation: Cache detection results, sample-based detection
   
3. **Quote Handling**: CSV quote escaping more complex than TSV
   - Mitigation: Leverage Python csv module, extensive testing

### Operational Risks
1. **User Confusion**: Mixed formats in same directory
   - Mitigation: Clear logging, format indicators
   
2. **Breaking Changes**: Accidental TSV workflow breakage
   - Mitigation: Extensive regression testing, gradual rollout

## Success Metrics

1. **Functionality**: Successfully process CSV files with same features as TSV
2. **Performance**: CSV processing within 5% of TSV performance
3. **Compatibility**: Zero regression in existing TSV workflows
4. **Usability**: Auto-detection accuracy >95% for standard files
5. **Adoption**: Documentation and examples for CSV processing

## Code Examples

### Example 1: Using CSV with Config
```json
{
  "files": [{
    "file_pattern": "sales_{month}.csv",
    "table_name": "SALES_DATA",
    "file_format": "CSV",
    "delimiter": ",",
    "date_column": "sale_date",
    "expected_columns": ["sale_date", "product", "amount"]
  }]
}
```

### Example 2: Auto-Detection
```bash
# Let the system detect format automatically
./snowflake_etl.sh --config config.json load \
  --base-path /data/mixed_formats \
  --month 2024-01
```

### Example 3: Explicit Format Override
```bash
# Force CSV interpretation of .txt files
./snowflake_etl.sh --config config.json load \
  --file data.txt \
  --format CSV \
  --delimiter ","
```

## Documentation Updates Required

1. **README.md**: Add CSV examples and format configuration
2. **CLAUDE.md**: Update technical details with format handling
3. **CHANGELOG.md**: Document v3.5.0 CSV support
4. **Config Examples**: Create CSV-specific config templates
5. **User Guide**: Add format detection troubleshooting

## Conclusion

This implementation plan provides a comprehensive approach to adding CSV support while maintaining full backward compatibility. The phased approach allows for incremental development and testing, reducing risk. The automatic detection feature will make the system more user-friendly while explicit configuration options provide full control when needed.

## Next Steps

1. Review and approve implementation plan
2. Create feature branch `feature/csv-support`
3. Begin Phase 1 implementation
4. Set up CSV test data sets
5. Schedule testing milestones