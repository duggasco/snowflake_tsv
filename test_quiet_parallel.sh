#!/bin/bash

# Test parallel progress bars specifically in --quiet mode

echo "Testing parallel progress bars in --quiet mode..."
echo "This should show ONLY progress bars, no other output"
echo "=================================================="
echo ""

# Create larger test files to see progress better
for month in 012024 022024 032024; do
    mkdir -p data/$month
    # Create a file with more rows for visible progress
    echo -e "recorddate\tvalue1\tvalue2\tvalue3" > data/$month/test_20240101-20240131.tsv
    for i in {1..100}; do
        echo -e "2024-01-01\t$RANDOM\t$RANDOM\t$RANDOM" >> data/$month/test_20240101-20240131.tsv
    done
done

# Run with quiet mode and skip QC (2 progress bars per job)
echo "Running 3 months in parallel with --quiet --skip-qc..."
./run_loader.sh --months 012024,022024,032024 --parallel 3 --skip-qc --analyze-only --quiet

echo ""
echo "=================================================="
echo "Test complete! Check that:"
echo "1. Only progress bars were shown (no other output)"
echo "2. Each job had 2 bars: Files and Compression"
echo "3. Bars were labeled with month: [2024-01], [2024-02], [2024-03]"
echo "4. No overlapping or artifacts"