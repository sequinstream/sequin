#!/bin/bash

WITH_TIMESTAMPS=false
STREAM_NAME="cpu_stats"

# Process command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --with-timestamps)
            WITH_TIMESTAMPS=true
            shift
            ;;
        *)
            STREAM_NAME="$1"
            shift
            ;;
    esac
done

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
    
    # Get current timestamp if --with-timestamps is set
    timestamp=$([ "$WITH_TIMESTAMPS" = true ] && date +%s || echo "")

    for core in $(seq 0 $((num_cores-1))); do
        util=$(get_cpu_utilization $core)
        
        # Prepare JSON payload
        payload="{\"load\": $load, \"utilization\": $util}"
        
        # Prepare message key with optional timestamp
        load_key="core_${core}.load${timestamp:+.$timestamp}"
        util_key="core_${core}.util${timestamp:+.$timestamp}"

        # Send data to Sequin stream
        sequin stream send "$STREAM_NAME" "$load_key" "$payload"
        sequin stream send "$STREAM_NAME" "$util_key" "$payload"
    done

    echo "Sent data for all cores"
    
    # Wait for 5 seconds before the next iteration
    sleep 5
done