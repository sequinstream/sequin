#!/usr/bin/env bash

# Abort sign off on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
RESET='\033[0m'

# Start the benchmark timer
SECONDS=0

# Change to the project root directory
cd "$(dirname "$0")/.." || exit

# Repository introspection
OWNER=$(gh repo view --json owner --jq .owner.login)
REPO=$(gh repo view --json name --jq .name)
SHA=$(git rev-parse HEAD)
USER=$(git config user.name)

# Function to run a command and check its exit status
run_step() {
    local cmd="$1"
    echo -e "${BLUE}Run $cmd${RESET}"
    if eval "$cmd"; then
        echo -e "${GREEN}Completed $cmd in $((SECONDS - start_time)) seconds${RESET}"
    else
        echo -e "${RED}Failed to run $cmd${RESET}" >&2
        exit 1
    fi
}

# Check if repository is clean
if [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't sign off on a dirty repository!${RESET}" >&2
    git status
    exit 1
fi

# Check if already signed off
if gh api --method GET -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/${OWNER}/${REPO}/commits/${SHA}/statuses" | \
    jq -e '.[] | select(.context == "signoff" and .state == "success" and .description | startswith("Signed off by '"$USER"'"))' > /dev/null; then
    echo -e "${YELLOW}Commit ${SHA} has already been signed off. No action needed.${RESET}"
    exit 0
fi

echo -e "${BLUE}Attempting to sign off on ${SHA} in ${OWNER}/${REPO} as ${USER}${RESET}"

cd server

# Run steps
run_step "mix format --check-formatted"
run_step "MIX_ENV=prod mix compile --warnings-as-errors"
run_step "mix test"

cd ../cli

run_step "go test ./cli"
run_step "go fmt ./..."
run_step "go vet ./..."
run_step "go build -o /dev/null ./..."

# Report successful sign off to GitHub
description="Signed off by ${USER} (${SECONDS} seconds)"
if gh api --method POST --silent \
    -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/${OWNER}/${REPO}/statuses/${SHA}" \
    -f "context=signoff" -f "state=success" -f "description=${description}"; then
    echo -e "${GREEN}Reported success to GitHub${RESET}"
else
    echo -e "${RED}Failed to report success to GitHub${RESET}" >&2
    exit 1
fi

echo -e "${GREEN}Signed off on ${SHA} in ${SECONDS} seconds${RESET}"