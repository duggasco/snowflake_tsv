#!/bin/bash

# Test script to simulate parallel bash execution with --quiet mode

echo "Testing parallel execution with --quiet mode"
echo "============================================"

# Create some test data files
mkdir -p test_data
for month in 01 02 03; do
    echo -e "col1\tcol2\tcol3" > test_data/file_2024${month}01-2024${month}31.tsv
    for i in {1..100}; do
        echo -e "data1\tdata2\tdata3" >> test_data/file_2024${month}01-2024${month}31.tsv
    done
done

# Test 1: Sequential with --quiet
echo -e "\nTest 1: Sequential processing with --quiet"
./run_loader.sh --month 012024 --quiet --dry-run --base-path test_data 2>&1 | head -20

# Test 2: Parallel with --quiet (3 jobs)
echo -e "\nTest 2: Parallel processing with --quiet (3 jobs)"
./run_loader.sh --months 012024,022024,032024 --parallel 3 --quiet --dry-run --base-path test_data 2>&1 | head -40

# Clean up
rm -rf test_data

echo -e "\nTest complete"