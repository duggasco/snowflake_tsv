# Snowflake ETL Pipeline - Refactoring Plan
*Created: 2025-01-22*
*Current Version: 2.12.0*

## Executive Summary

After successfully implementing a unified wrapper interface (`snowflake_etl.sh`), the project has evolved from scattered individual scripts to a cohesive system. However, the Python layer still reflects its origins with significant code duplication. This refactoring plan addresses technical debt while preserving the successful wrapper pattern.

## Current State Analysis

### Architecture Overview
```
User Interface Layer:
├── snowflake_etl.sh (2318 lines) - Main wrapper/menu system
├── Supporting Scripts:
│   ├── run_loader.sh (896 lines) - ETL operations
│   ├── drop_month.sh (238 lines) - Data deletion
│   ├── generate_config.sh (627 lines) - Config generation
│   ├── recover_failed_load.sh (312 lines) - Recovery operations
│   └── tsv_sampler.sh (312 lines) - File analysis

Python Implementation Layer:
├── Core Operations:
│   ├── tsv_loader.py - Main ETL logic
│   ├── drop_month.py - Data deletion
│   └── generate_table_report.py - Reporting
├── Utilities:
│   ├── check_duplicates_interactive.py
│   ├── check_snowflake_table.py
│   ├── check_stage_and_performance.py
│   ├── compare_tsv_files.py
│   ├── validate_tsv_file.py
│   └── diagnose_copy_error.py
└── UI Components:
    ├── tsv_file_browser.py
    └── tsv_browser_integration.py
```

### Key Issues Identified

#### 1. Code Duplication (Critical)
- **Snowflake Connections**: 7 files independently implement connection logic
- **Config Loading**: 10 files duplicate JSON loading (22 occurrences total)
- **Logging Setup**: Each script has its own logging configuration
- **Progress Tracking**: tqdm setup repeated across multiple files
- **Error Handling**: Common patterns duplicated without standardization

#### 2. Lack of Modularity
- No shared utility modules
- No package structure for Python code
- Classes from tsv_loader.py imported ad-hoc by only 2 scripts
- No centralized error handling or retry logic

#### 3. Shell Script Overlap
- Some utility functions duplicated between shell scripts
- Config file selection logic repeated
- Job management functions spread across scripts

## Proposed Refactoring

### Phase 1: Python Layer Reorganization (Priority: HIGH)

#### 1.1 Create Package Structure
```
snowflake_etl/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── loader.py (refactored from tsv_loader.py)
│   ├── deleter.py (refactored from drop_month.py)
│   └── reporter.py (refactored from generate_table_report.py)
├── utils/
│   ├── __init__.py
│   ├── snowflake_connection.py
│   ├── config_manager.py
│   ├── logging_setup.py
│   ├── progress_tracker.py
│   └── error_handler.py
├── validators/
│   ├── __init__.py
│   ├── data_validator.py
│   ├── file_validator.py
│   └── duplicate_checker.py
├── tools/
│   ├── __init__.py
│   ├── file_browser.py
│   ├── stage_manager.py
│   ├── file_comparator.py
│   └── diagnostics.py
└── cli/
    ├── __init__.py
    └── command_dispatcher.py
```

#### 1.2 Centralized Components

##### snowflake_connection.py
```python
class SnowflakeConnectionManager:
    """Singleton connection manager with pooling and retry logic"""
    _instance = None
    _connections = {}
    
    def get_connection(self, config_path=None, config_dict=None):
        """Get or create connection with automatic retry"""
        
    def execute_with_retry(self, query, params=None, max_retries=3):
        """Execute query with exponential backoff"""
```

##### config_manager.py
```python
class ConfigManager:
    """Centralized configuration loading and validation"""
    _cache = {}
    
    def load_config(self, path):
        """Load and cache configuration"""
        
    def validate_config(self, config):
        """Validate configuration schema"""
        
    def get_snowflake_config(self):
        """Extract Snowflake connection parameters"""
        
    def get_file_configs(self):
        """Get file processing configurations"""
```

##### logging_setup.py
```python
class LogManager:
    """Unified logging configuration"""
    
    def setup_logger(self, name, level='INFO', quiet=False):
        """Create standardized logger"""
        
    def get_logger(self, name):
        """Get existing or create new logger"""
```

##### progress_tracker.py
```python
class UnifiedProgressTracker:
    """Centralized progress tracking with parallel support"""
    
    def create_progress_bar(self, total, desc, position=None):
        """Create tqdm progress bar with proper positioning"""
        
    def update_progress(self, bar, n=1):
        """Thread-safe progress update"""
```

### Phase 2: Migration Strategy (Priority: HIGH)

#### 2.1 Backward Compatibility Layer
Create thin wrapper scripts to maintain existing interfaces:

```python
# tsv_loader.py (compatibility wrapper)
#!/usr/bin/env python3
"""Backward compatibility wrapper for tsv_loader"""
from snowflake_etl.core.loader import main

if __name__ == "__main__":
    main()
```

#### 2.2 Gradual Migration Steps
1. **Week 1**: Create package structure and utility modules
2. **Week 2**: Migrate core operations (tsv_loader, drop_month, reporter)
3. **Week 3**: Migrate validators and tools
4. **Week 4**: Update shell scripts to use new structure

### Phase 3: Shell Script Consolidation (Priority: MEDIUM)

#### 3.1 Extract Common Functions
Create `lib/common_functions.sh`:
```bash
# Common utility functions
select_config_file() { ... }
get_tables_from_config() { ... }
show_colored_message() { ... }
check_prerequisites() { ... }
```

#### 3.2 Simplify Individual Scripts
- Source common functions: `. lib/common_functions.sh`
- Remove duplicated code
- Focus each script on its unique functionality

### Phase 4: Advanced Improvements (Priority: LOW)

#### 4.1 Connection Pooling
- Implement connection pool for parallel operations
- Reuse connections across multiple operations
- Add connection health checks

#### 4.2 Unified CLI
Create single Python entry point:
```python
# snowflake_etl_cli.py
python snowflake_etl_cli.py load --config config.json --month 2024-01
python snowflake_etl_cli.py delete --table MY_TABLE --month 2024-01
python snowflake_etl_cli.py validate --config config.json
```

#### 4.3 Configuration Schema
- Add JSON schema validation
- Support YAML configurations
- Environment variable overrides

## Implementation Priority

### Immediate (Sprint 1)
1. Create `snowflake_etl` package structure
2. Implement `utils/snowflake_connection.py`
3. Implement `utils/config_manager.py`
4. Migrate `tsv_loader.py` to use new utils

### Short-term (Sprint 2)
1. Migrate remaining Python scripts
2. Create backward compatibility wrappers
3. Update documentation
4. Test all wrapper menu options

### Medium-term (Sprint 3)
1. Consolidate shell script functions
2. Implement connection pooling
3. Add comprehensive unit tests
4. Performance benchmarking

### Long-term (Future)
1. Unified CLI interface
2. REST API layer
3. Web dashboard
4. Kubernetes deployment

## Benefits

### Immediate Benefits
- **Reduced Maintenance**: Single point of change for connections, configs
- **Improved Reliability**: Centralized retry logic and error handling
- **Faster Development**: New features can leverage existing utilities
- **Better Testing**: Modular structure enables unit testing

### Long-term Benefits
- **Scalability**: Connection pooling and better resource management
- **Extensibility**: Clear structure for adding new features
- **Team Collaboration**: Clearer code organization
- **Performance**: Reduced overhead from duplicate initializations

## Risk Mitigation

### Risks
1. **Breaking Changes**: Existing scripts might fail
2. **Testing Gaps**: Hidden dependencies might break
3. **Learning Curve**: Team needs to understand new structure

### Mitigation Strategies
1. **Backward Compatibility**: Keep wrapper scripts for existing interfaces
2. **Comprehensive Testing**: Test each menu option after changes
3. **Gradual Migration**: Refactor one component at a time
4. **Documentation**: Update all docs during migration
5. **Version Control**: Tag stable version before refactoring

## Success Metrics

### Technical Metrics
- [ ] Code duplication reduced by >70%
- [ ] All wrapper menu options functional
- [ ] Unit test coverage >80%
- [ ] No performance degradation
- [ ] Zero breaking changes for users

### Quality Metrics
- [ ] Reduced bug reports
- [ ] Faster feature implementation
- [ ] Improved code review times
- [ ] Better onboarding for new developers

## Next Steps

1. **Review and Approve**: Team review of this plan
2. **Create Feature Branch**: `feature/python-refactoring`
3. **Start Phase 1**: Create package structure
4. **Weekly Check-ins**: Monitor progress and adjust

## Appendix: Detailed Duplication Analysis

### Snowflake Connection Duplication
Files with independent connection logic:
- tsv_loader.py (lines 487-520)
- drop_month.py (lines 45-78)
- check_snowflake_table.py (lines 25-58)
- check_stage_and_performance.py (lines 30-63)
- validate_tsv_file.py (lines 150-183)
- diagnose_copy_error.py (lines 40-73)
- generate_table_report.py (lines 85-118)

### Config Loading Duplication
Total occurrences: 22 across 10 files
Average lines per implementation: ~15-20

### Estimated Savings
- Lines of code reduction: ~1,500-2,000 lines
- Maintenance points: From 12 to 1 for core logic
- Testing surface: Reduced by ~60%

---
*This refactoring plan provides a roadmap to transform the codebase from its organic growth state into a well-structured, maintainable system while preserving all existing functionality.*