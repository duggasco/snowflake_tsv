# AGENTS.md

## Build/Lint/Test Commands
- Build: `pip install -e .`
- Lint: `ruff check .`
- Test single: `python -m pytest tests/test_cli.py::test_basic_functionality -v`
- Test all: `bash test_scripts/run_all_tests.sh`

## Code Style Guidelines
- Imports: Standard library first, then third-party, then local
- Formatting: Use ruff for auto-formatting
- Types: Type hints required for all functions
- Naming: snake_case for variables/functions, PascalCase for classes
- Error Handling: Use specific exceptions and log appropriately
- Docstrings: Required for all public functions
- Ruff config: Follow project's ruff settings in setup.cfg

## Project Context (from CLAUDE.md)
- This is a Snowflake ETL pipeline for large TSV files (up to 50GB) with data quality checks, progress tracking, and parallel processing
- Core components: FileAnalyzer, DataQualityChecker, SnowflakeDataValidator, SnowflakeLoader
- Key features: Streaming processing, async COPY support, duplicate detection, parallel processing with tqdm progress bars
- Testing approach includes pytest fixtures and comprehensive test suites

## Git Workflow
- always use git add .

## Database Design
- always use UUIDs instead of int IDs

## Communication Guidelines
- never respond with the phrase "you're absolutely right" post corrective feedback

## Task Management
- always complete your todos before marking as complete - especially testing
- always keep and update todos in TODO.md in project root
- always create and keep updated a TODO.md which works in tandem with our PLAN.md and our todos list

## Bug Tracking
- document bugs in BUGS.md as soon as they are identified
- include context, potential causes, and eventual solutions when logging bugs

## Documentation
- add major changes - as well as useful context around these changes to CHANGELOG.md after actioning and successful testing
- always track significant changes to our codebase in CHANGELOG.md
- IMPORTANT: Never add bugs, todos, or changelog entries to CLAUDE.md - use the dedicated files:
  - BUGS.md for known issues, troubleshooting, and bug fixes
  - TODO.md for future enhancements and tasks
  - CHANGELOG.md for historical changes and updates
  - PLAN.md for current project planning and context

## Workflow Best Practices
- always read PLAN.md & TODO.md after compact
- after initial planning phase, keep plan and relevant plan context in PLAN.md in project root. update alongside TODO.md
- always create a comprehensive plan and add to PLAN.md. update this file as you work with any useful context

## Testing
- always use our playwright mcp for web testing instead of installing in local environment
- always use our playwright mcp when testing web/frontend

## Collaboration
- always consult gemini mcp when planning or creating code. question their assumptions and have them question yours. only after you come to a consensus should you implement
- always consult with gemini mcp when planning and coding - challenge their assumptions and have them challenge yours.  have them code and plan - and compare and contrast your plans and code with theirs.  come to parity before changing anything to ensure optimal route is taken

## Environment Management
- use docker and venv when possible to maintain isolation and prevent changing host environment
- never use emojis