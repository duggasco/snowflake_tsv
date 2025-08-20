# BUGS.md

## Bug Tracking Log

### [2025-08-20] BUG-001: OS Module Import Scope Error

#### Status: RESOLVED ✅

#### Summary
SnowflakeLoader.load_file_to_stage_and_table() method failed with "local variable 'os' referenced before assignment" error

#### Symptoms
- All Snowflake upload attempts failed immediately
- Processes completed in 0.6-0.8 seconds instead of expected ~5 minutes per file
- Error message in logs: "Failed to load TEST_CUSTOM_FACTLENDINGBENCHMARK: local variable 'os' referenced before assignment"
- Affected all parallel month processing attempts

#### Root Cause
- `import os` and `import time` statements were inside the try block (lines 501-502)
- When code reached the finally block, os module was not in scope
- Line 594 attempted to call os.remove() without os being available

#### Resolution
- Moved `import os` and `import time` to the beginning of the method (lines 487-488)
- Placed imports before try/except/finally blocks
- Ensures modules are available throughout entire method scope

#### Files Changed
- tsv_loader.py (lines 487-488, removed duplicate imports from lines 501-502)

#### Testing Notes
- Requires testing on remote machine with Snowflake connector installed
- Verify file uploads complete successfully
- Check that compressed files are properly cleaned up after upload

#### Lessons Learned
- Always import modules at the beginning of functions/methods if they're needed in finally blocks
- Local imports within try blocks won't be available in exception handlers or finally blocks
- Quick completion times can indicate early failures rather than optimization