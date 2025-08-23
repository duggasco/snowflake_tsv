# Prompt for Next Session - Copy This Exactly

Continue Phase 4 Implementation of Snowflake ETL Refactoring

Please begin by reading these critical context files in order:
1. /root/snowflake/CONTEXT_HANDOVER.md - Contains session summary and CRITICAL BUG to fix
2. /root/snowflake/TODO.md - Current task list and Phase 4 objectives  
3. /root/snowflake/CHANGELOG.md - Recent accomplishments and architecture decisions
4. /root/snowflake/PLAN.md - Overall refactoring plan and status

## Current Status
We just completed 90% of Phase 4 (Shell Script Consolidation) but discovered a CRITICAL BUG that blocks the interactive menu from working. The duplicate show_menu() function in snowflake_etl.sh (lines 343-435) must be removed immediately.

## Key Context
- We are on v3.0.0-alpha of a major refactoring from singleton to dependency injection architecture
- Phase 1-3 are complete (core infrastructure, component extraction, all operations)
- Phase 4 is 90% done - libraries extracted but menu broken
- Test environment exists in test_venv/ with Snowflake connector installed

## Collaboration Pattern with Gemini
Our established workflow for optimal results:
1. You write code first - Don't rely on Gemini to write initial implementations
2. Get Gemini's critique - Ask for specific improvements and alternatives
3. Request Gemini's version - Have them write to files with _gemini suffix
4. Compare and debate - Discuss tradeoffs until reaching optimal solution
5. Implement best approach - Merge the best ideas from both versions

Example: "Review my implementation and provide critique. Write your alternative version to [filename]_gemini.py so we can compare approaches."

## Important Technical Decision from Phase 3
We REJECTED Gemini's overly cautious connection pooling suggestion. They wanted to open/close connections per table, but our thread-local pooling approach is ~25x faster. Remember to challenge their suggestions when you have better performance insights.

## Priority Tasks for This Session
1. FIX THE DUPLICATE show_menu() FUNCTION (lines 343-435 in snowflake_etl.sh)
2. Test that the interactive menu works: `echo "0" | ./snowflake_etl.sh`
3. Update Python script calls to use `python -m snowflake_etl` instead of individual scripts
4. Complete Phase 4 testing
5. Begin Phase 5 if time permits

## Commands to Run Tests
```bash
# Activate test environment
source test_venv/bin/activate

# Test Python CLI
python -m snowflake_etl --help

# Test interactive menu (after fix)
./snowflake_etl.sh

# Test with piped input
echo "0" | ./snowflake_etl.sh
```

## File Locations
- Main script: /root/snowflake/snowflake_etl.sh (NEEDS FIX)
- Libraries: /root/snowflake/lib/*.sh (completed)
- Python package: /root/snowflake/snowflake_etl/ (completed)
- Test scripts: test_menu.sh, test_args.sh, test_menu_debug.sh

Begin by reading CONTEXT_HANDOVER.md which has detailed instructions on the exact bug and how to fix it.