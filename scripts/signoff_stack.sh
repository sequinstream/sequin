#!/usr/bin/env bash

# Signs off a Graphite stack

# Abort on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'

# Change to the project root directory
cd "$(dirname "$0")/.." || exit

echo -e "${BLUE}Starting signoff process for the Graphite Stack${RESET}"

# Move to the bottom of the stack
gt bottom

signoff_stack() {
    if ./scripts/signoff.sh; then
        if output=$(gt up 2>&1) && [[ $output == *"Already at the top most branch in the stack"* ]]; then
            echo -e "${GREEN}Successfully signed off on all PRs in the stack${RESET}"
        else
            signoff_stack
        fi
    else
        echo -e "${RED}Signoff failed. Stopping the process.${RESET}"
        exit 1
    fi
}

signoff_stack