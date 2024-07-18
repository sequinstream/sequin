#!/bin/bash

# Function to handle errors
handle_error() {
    echo "Error: $1" >&2
    exit 1
}

# Get the absolute path of the script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root directory (parent of scripts)
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Debug: Print directories
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Change to the cli directory and run its release script
"$PROJECT_ROOT/cli/release.sh" || handle_error "CLI release script failed"

# Change to the server directory and run its release script
"$PROJECT_ROOT/server/scripts/release.sh" || handle_error "Server release script failed"

echo "Release process completed successfully"