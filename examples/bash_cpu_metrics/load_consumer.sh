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

# Set the appropriate filter and consumer name based on --with-timestamps flag
if [ "$WITH_TIMESTAMPS" = true ]; then
    FILTER="*.load.>"
    CONSUMER_NAME="load_consumer_with_timestamps"
else
    FILTER="*.load"
    CONSUMER_NAME="load_consumer"
fi

# Attempt to create the consumer and capture output
output=$(sequin consumer add "$STREAM_NAME" "$CONSUMER_NAME" --filter="$FILTER" 2>&1 --defaults)
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "Created new consumer: $CONSUMER_NAME for stream: $STREAM_NAME"
else
    if echo "$output" | grep -q "already exists" || echo "$output" | grep -q "stream_id: has already been taken"; then
        echo "Using existing consumer: $CONSUMER_NAME for stream: $STREAM_NAME"
    else
        echo "Error creating consumer: $output"
        exit 1
    fi
fi

# Main loop to consume messages
while true; do
    # Receive message from the consumer
    message=$(sequin consumer receive "$STREAM_NAME" "$CONSUMER_NAME")
    
    # Check if a message was received
    if [ -n "$message" ] && [ "$message" != "No messages available." ]; then
        # Extract the ack_id from the message and clean it up
        ack_id=$(echo "$message" | grep -o 'Ack ID: [^ ]*' | cut -d ' ' -f 3 | sed 's/):$//')
        
        # Verify if ack_id is a valid UUID
        if [[ $ack_id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
            # Extract the message content (assuming it's JSON)
            content=$(echo "$message" | sed -n '/^{/,/^}/p')
            
            # Process the message
            echo "Received message content: $content"
            echo "Processing message for 1 second..."
            sleep 1
            
            # Acknowledge the message
            ack_response=$(sequin consumer ack "$STREAM_NAME" "$CONSUMER_NAME" "$ack_id" 2>&1)
            if [[ $ack_response == *"Message acknowledged with Ack ID"* ]]; then
                echo "Successfully acknowledged message with ack_id: $ack_id"
            else
                echo "Failed to acknowledge message with ack_id: $ack_id"
                echo "Response: $ack_response"
            fi
        else
            echo "Invalid ack_id format: $ack_id"
        fi
    else
        echo "No message received. Waiting..."
        sleep 5
    fi
done