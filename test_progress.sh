#!/bin/bash

# Test script to simulate a job with progress output

echo "Starting test job..."
echo "This simulates what tsv_loader.py would output"
echo ""

# Simulate progress bars
for i in {1..5}; do
    echo "Processing file $i of 5..."
    echo "████████████████████ 100% - File $i complete"
    sleep 1
done

echo ""
echo "Validation in progress..."
for i in {10..100..10}; do
    printf "\rValidating: %d%%" $i
    sleep 0.5
done
echo ""

echo "Job completed successfully!"