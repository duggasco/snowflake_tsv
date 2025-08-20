#!/bin/bash

# Test script to verify parallel progress bars work correctly
# This simulates running multiple months in parallel

echo "Testing parallel progress bars..."

# Create test data directories if they don't exist
mkdir -p data/012024
mkdir -p data/022024
mkdir -p data/032024

# Create small test TSV files
echo -e "recorddate\tvalue" > data/012024/test_20240101-20240131.tsv
echo -e "2024-01-01\t100" >> data/012024/test_20240101-20240131.tsv

echo -e "recorddate\tvalue" > data/022024/test_20240201-20240229.tsv  
echo -e "2024-02-01\t200" >> data/022024/test_20240201-20240229.tsv

echo -e "recorddate\tvalue" > data/032024/test_20240301-20240331.tsv
echo -e "2024-03-01\t300" >> data/032024/test_20240301-20240331.tsv

# Run parallel processing with 3 jobs
echo "Running 3 months in parallel with --analyze-only to test progress bars..."
./run_loader.sh --months 012024,022024,032024 --parallel 3 --analyze-only --quiet

echo "Test complete. Check if progress bars were stacked correctly."