# Session Context Initialization Prompt

Use this prompt at the beginning of each session to load full project context:

---

## PROMPT TO USE:

Please review the following documentation files to understand the full context of this Snowflake ETL Pipeline project:

1. First, read CLAUDE.md to understand the project overview, architecture, and key capabilities
2. Then read PLAN.md to understand the project vision, roadmap, and architectural decisions
3. Review TODO.md to see current progress, completed work, and pending tasks
4. Check CHANGELOG.md for recent changes and version history
5. Review BUGS.md for known issues and recently fixed problems
6. Finally, check README.md for user-facing documentation and usage examples

After reviewing these files, please confirm your understanding by providing a brief summary of:
- The project's current version and status
- Key architectural components (ApplicationContext, DI pattern, etc.)
- Recent improvements and fixes
- Any active issues or pending work
- The main capabilities (ETL processing, validation, reporting, etc.)

This will ensure you have full context for our work session.

---

## ALTERNATIVE SHORTER PROMPT:

Please load and review all .md documentation files in the project root (CLAUDE.md, PLAN.md, TODO.md, CHANGELOG.md, BUGS.md, README.md) to understand the full context of this Snowflake ETL pipeline project (v3.0.2). This is a production-ready system for processing 50GB+ TSV files with comprehensive validation, using dependency injection architecture and supporting parallel processing. Confirm your understanding of the current architecture, recent fixes, and project status.

---

## KEY CONTEXT POINTS TO REMEMBER:

### Project Identity
- **Name**: Snowflake ETL Pipeline Manager
- **Version**: 3.0.2 (as of 2025-08-26)
- **Purpose**: High-performance ETL for massive TSV files (up to 50GB)
- **Architecture**: Dependency Injection with ApplicationContext

### Core Capabilities
1. **Data Loading**: Streaming processing, async COPY, parallel execution
2. **Validation**: File-based and Snowflake-based validation
3. **Data Quality**: Anomaly detection, duplicate checking, gap analysis
4. **Reporting**: Comprehensive table reports with full data visibility
5. **Data Management**: Safe month-based deletion with audit trails

### Technical Stack
- **Language**: Python 3.x
- **Database**: Snowflake
- **Key Libraries**: snowflake-connector-python, pandas, numpy, tqdm
- **Architecture Pattern**: Dependency Injection (no singletons)
- **CLI**: Unified entry point (`sfl` command)

### Recent Evolution
- **v2.x**: Monolithic scripts with singleton patterns
- **v3.0.0**: Complete refactoring to DI architecture
- **v3.0.1**: Unicode fixes, connection pooling improvements
- **v3.0.2**: Report display fixes for complete data visibility

### Performance Benchmarks
- 50GB file processing: ~4 hours optimized
- Billion+ row validation: ~35ms
- Supports files up to 500M rows

### Project Structure
```
snowflake_etl/
├── core/           # ApplicationContext, base operations
├── operations/     # Load, Delete, Validate, Report operations
├── validators/     # Data quality and Snowflake validators
├── utils/          # Connection manager, config, logging
├── models/         # Data classes and models
├── ui/             # Progress tracking and display
└── cli/            # Command-line interface
```

### Important Design Decisions
1. **Why DI over Singletons**: Testability, clarity, flexibility
2. **Why Streaming**: Memory efficiency for 50GB+ files
3. **Why Async COPY**: Prevents 5-minute timeouts on large files
4. **Connection Pooling**: Reused across operations (pool size 10)
5. **Progress Tracking**: Abstract interface with multiple implementations

### Current Focus Areas
- Performance optimization for 100GB+ files
- Memory usage reduction
- Enhanced error recovery
- Advanced validation rules

### Key Files to Reference
- `tsv_loader.py`: Main ETL orchestration (being phased out)
- `snowflake_etl/cli/main.py`: New unified CLI entry point
- `snowflake_etl/core/application_context.py`: DI container
- `snowflake_etl/operations/`: All main operations
- Config files in `config/`: JSON configurations for different datasets

### Testing & Quality
- Comprehensive test suite in `tests/`
- Integration tests with mock Snowflake
- Performance benchmarking included
- Validates billion-row tables efficiently

### Known Limitations
- Date format support limited to specific formats
- Memory usage high for file-based QC on 50GB+ files
- No automatic log cleanup
- Progress bars can overlap if terminal resized

---

## CONTEXT VERIFICATION CHECKLIST

After loading context, verify understanding of:

✓ Current version (3.0.2) and recent fixes
✓ Dependency Injection architecture with ApplicationContext
✓ Main operations: Load, Delete, Validate, Report
✓ Performance characteristics and optimizations
✓ Recent report display fixes (no truncation, correct percentages)
✓ Connection pooling and async execution for large files
✓ File patterns and configuration structure
✓ Known issues and workarounds
✓ Testing approach and quality measures
✓ Git workflow and documentation standards

---

## USEFUL COMMANDS FOR CONTEXT

```bash
# Check current version and status
grep "Current Version" *.md

# See recent changes
head -50 CHANGELOG.md

# Check active issues
grep "Known Issues" BUGS.md -A 20

# Review current tasks
grep "Priority" TODO.md -A 10

# Understand architecture
grep "Architecture" CLAUDE.md -A 20
```

---

*This prompt ensures consistent context loading across all sessions and maintains continuity in development work.*