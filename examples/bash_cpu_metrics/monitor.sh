#!/bin/bash

# Set default stream name or use provided argument
STREAM_NAME=${1:-cpu_stats}

# Attempt to create the stream and capture output
output=$(sequin stream add "$STREAM_NAME" 2>&1)
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Created new stream: $STREAM_NAME"
else
    if echo "$output" | grep -q "has already been taken"; then
        echo "Using existing stream: $STREAM_NAME"
    else
        echo "Error creating stream: $output"
        exit 1
    fi
fi

# Function to get CPU load
get_cpu_load() {
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    echo $load
}

# Function to get CPU utilization per core
get_cpu_utilization() {
    core=$1
    util=$(ps -A -o %cpu | awk '{s+=$1} END {print s}')
    echo $util
}

# Main loop
while true; do
    load=$(get_cpu_load)
    
    # Get the number of CPU cores
    num_cores=$(sysctl -n hw.ncpu)
    
    for core in $(seq 0 $((num_cores-1))); do
        util=$(get_cpu_utilization $core)
        
        # Prepare JSON payload
        payload="{\"load\": $load, \"utilization\": $util}"
        
        # Send data to Sequin stream
        sequin stream send "$STREAM_NAME" "core_$core.load" "$payload"
        sequin stream send "$STREAM_NAME" "core_$core.util" "$payload"
    done

    echo "Sent data for all cores"
    
    # Wait for 5 seconds before the next iteration
    sleep 5
done
