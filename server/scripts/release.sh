#!/bin/bash

# Change to the server directory
cd "$(dirname "$0")/.." || exit

# Check if we're in the correct directory
if [ ! -f "Dockerfile" ]; then
    echo "Error: Dockerfile not found. Make sure you're in the server directory."
    exit 1
fi

# Execute the Docker build command
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t sequin/sequin:latest \
    . \
    --push

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Build and push completed successfully."
else
    echo "Error: Build or push failed."
    exit 1
fi
