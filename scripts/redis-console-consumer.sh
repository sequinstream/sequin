#!/bin/bash

# Default values
FROM_BEGINNING=""
STREAM_KEY=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        "from-beginning")
            FROM_BEGINNING="0"
            shift
            ;;
        *)
            # First non-flag argument is treated as the stream key
            if [ -z "$STREAM_KEY" ]; then
                STREAM_KEY="$1"
            else
                echo "Unknown option: $1"
                exit 1
            fi
            shift
            ;;
    esac
done

if [ -z "$STREAM_KEY" ]; then
    echo "Usage: $0 <stream-key> [from-beginning]"
    exit 1
fi

# Set the starting ID based on from-beginning flag
START_ID=${FROM_BEGINNING:-"$"}

# Continuously read from the stream
while true; do
    redis-cli XREAD BLOCK 0 STREAMS "$STREAM_KEY" "$START_ID"
    # Update START_ID to the last received ID for the next iteration
    START_ID="$"
done 