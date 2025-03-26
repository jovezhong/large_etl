#!/bin/bash

# Initialize total size counter
total_bytes=0
total_mb=0
failed_urls=()
base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_"

echo "Fetching file sizes for fhvhv_tripdata from 2019-02 to 2025-01..."
echo "--------------------------------------------------------------"

# Loop through years and months
for year in {2019..2025}; do
    # Determine end month based on year
    if [ $year -eq 2025 ]; then
        end_month=1
    else
        end_month=12
    fi

    # Determine start month based on year
    if [ $year -eq 2019 ]; then
        start_month=2
    else
        start_month=1
    fi

    # Loop through months
    for month in $(seq -f "%02g" $start_month $end_month); do
        url="${base_url}${year}-${month}.parquet"

        # Get file size using curl with HEAD request
        size=$(curl -sI "$url" | grep -i "content-length" | awk '{print $2}' | tr -d '\r')

        # Check if size was obtained
        if [ -z "$size" ]; then
            echo "Failed to get size for $url"
            failed_urls+=("$url")
            continue
        fi

        # Calculate size in MB
        size_mb=$(echo "scale=2; $size/1024/1024" | bc)

        # Add to total
        total_bytes=$((total_bytes + size))
        total_mb=$(echo "scale=2; $total_mb + $size_mb" | bc)

        echo "$year-$month: $size_mb MB"
    done
done

# Convert total to human-readable format
total_gb=$(echo "scale=2; $total_mb/1024" | bc)

echo "--------------------------------------------------------------"
echo "Total size: $total_gb GB"

if [ ${#failed_urls[@]} -gt 0 ]; then
    echo "--------------------------------------------------------------"
    echo "Failed to get size for the following URLs:"
    for url in "${failed_urls[@]}"; do
        echo "- $url"
    done
fi
