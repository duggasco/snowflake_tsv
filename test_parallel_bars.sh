#!/bin/bash

# Test parallel progress bars with multiple jobs

echo "Testing parallel progress bars with 3 jobs..."
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""

# Move cursor back up
tput cuu 12

# Run 3 parallel jobs with different positions
(export TSV_JOB_POSITION=0 TEST_MONTH=2024-01 && source test_venv/bin/activate && python test_progress_bars.py) &
(export TSV_JOB_POSITION=1 TEST_MONTH=2024-02 && source test_venv/bin/activate && python test_progress_bars.py) &
(export TSV_JOB_POSITION=2 TEST_MONTH=2024-03 && source test_venv/bin/activate && python test_progress_bars.py) &

# Wait for all jobs
wait

echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo "Test complete!"