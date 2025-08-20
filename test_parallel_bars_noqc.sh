#!/bin/bash

# Test parallel progress bars WITHOUT QC (simulating --skip-qc or --validate-in-snowflake)

echo "Testing parallel progress bars WITHOUT QC (2 lines per job)..."

# Only 2 lines per job when QC is skipped
lines_per_job=2
total_lines=$((3 * ${lines_per_job}))

# Create spacing
for ((i=0; i<${total_lines}; i++)); do
    echo ""
done

# Move cursor back up
tput cuu ${total_lines}

# Run 3 parallel jobs with different positions (no QC)
(export TSV_JOB_POSITION=0 TEST_MONTH=2024-01 SHOW_QC=0 && source test_venv/bin/activate && python test_progress_bars_qc.py) &
(export TSV_JOB_POSITION=1 TEST_MONTH=2024-02 SHOW_QC=0 && source test_venv/bin/activate && python test_progress_bars_qc.py) &
(export TSV_JOB_POSITION=2 TEST_MONTH=2024-03 SHOW_QC=0 && source test_venv/bin/activate && python test_progress_bars_qc.py) &

# Wait for all jobs
wait

# Add spacing at the end
for ((i=0; i<${total_lines}; i++)); do
    echo ""
done

echo "Test complete! Each job should show only 2 progress bars (Files and Compression)"