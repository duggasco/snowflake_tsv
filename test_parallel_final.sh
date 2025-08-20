#!/bin/bash

# Final test of parallel progress bars with actual data

echo "Testing parallel processing with actual TSV files..."

# Create test environment
export SKIP_QC="--skip-qc"  # Skip QC to test 2-line mode

# Run three months in parallel
./run_loader.sh --months 012024,022024,032024 --parallel 3 --analyze-only --quiet

echo -e "\nTest complete! Check if:"
echo "1. Each job shows only 2 progress bars (Processing and Compression)"
echo "2. Progress bars are properly stacked without overlap"
echo "3. Each bar is labeled with its month [2024-01], [2024-02], [2024-03]"
echo "4. No stale/duplicate bars remain after completion"