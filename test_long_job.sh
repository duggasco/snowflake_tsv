#!/bin/bash

# Simulate a longer-running job with progress updates

echo "Starting long-running ETL job..."
echo "Initializing..."
sleep 2

for i in {1..10}; do
    echo "[$(date +%H:%M:%S)] Processing batch $i of 10..."
    echo "  Loading rows: $((i * 1000)) / 10000"
    echo "  Progress: $(( i * 10 ))%"
    sleep 1
done

echo "[$(date +%H:%M:%S)] Job completed successfully!"
echo "Total rows processed: 10000"