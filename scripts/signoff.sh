#!/usr/bin/env bash

# Abort sign off on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
RESET='\033[0m'

# Parse command line arguments
DIRTY=false
EXCLUDE=""
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dirty) DIRTY=true ;;
        --exclude) EXCLUDE="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

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
    start_time=$SECONDS
    if eval "$cmd"; then
        echo -e "${GREEN}Completed $cmd in $((SECONDS - start_time)) seconds${RESET}"
    else
        echo -e "${RED}Failed to run $cmd${RESET}" >&2
        exit 1
    fi
}

# Check if repository is clean (skip if --dirty is used)
if [[ "$DIRTY" = false ]] && [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't sign off on a dirty repository. Use 'make signoff-dirty' (or --dirty if calling directly) to override.${RESET}" >&2
    git status
    exit 1
fi

# If dirty flag is set, show git status
if [[ "$DIRTY" = true ]]; then
    echo -e "${YELLOW}Warning: Running signoff on a dirty repository.${RESET}"
    echo -e "${YELLOW}Current git status:${RESET}"
    git status --short
    echo ""
fi

# Check if already signed off
if gh api --method GET -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/${OWNER}/${REPO}/commits/${SHA}/statuses" | \
    jq -e '.[] | select(.context == "signoff" and .state == "success" and .description | startswith("Signed off by '"$USER"'"))' > /dev/null; then
    echo -e "${YELLOW}Commit ${SHA} has already been signed off. No action needed.${RESET}"
    exit 0
fi

echo -e "${BLUE}Attempting to sign off on ${SHA} in ${OWNER}/${REPO} as ${USER}${RESET}"

# Run steps
run_step "mix deps.get --check-locked"
run_step "mix format --check-formatted"
run_step "MIX_ENV=prod mix compile --warnings-as-errors"
# Pass max cases if asked to do so
MIX_TEST_CMD="mix test --max-failures 1"
if [ -n "${MIX_MAX_CASES+x}" ]; then
   MIX_TEST_CMD="$MIX_TEST_CMD --max-cases $MIX_MAX_CASES"
fi
if [ -n "$EXCLUDE" ]; then
   MIX_TEST_CMD="$MIX_TEST_CMD --exclude $EXCLUDE"
fi
run_step "$MIX_TEST_CMD"


# Run CLI tests
cd cli
run_step "go test ./cli"

# -------------------------------------------------------------
# Make sure Go sources are already formatted – fail otherwise
# -------------------------------------------------------------
run_step 'if unformatted=$(gofmt -s -l .); [ -n "$unformatted" ]; then \
    echo -e "${RED}The following Go files need to be formatted. Run '\''go fmt ./...'\'' and commit the changes:${RESET}\n$unformatted"; \
    exit 1; fi'

# Static analysis & build checks
run_step "go vet ./..."
run_step "go build -o /dev/null ./..."
# Return to the project root
cd ..

# Run cspell check
run_step "make spellcheck"

# Run mintlify broken-links check
run_step "make check-links"

cd assets
# Prettier formatting check
run_step "npm run format:check"
# TypeScript checking
run_step "npm run tsc"

cd ..

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
