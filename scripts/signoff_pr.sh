#!/usr/bin/env bash

# Abort on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'

# Check if PR number is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: PR number is required${RESET}"
    echo "Usage: $0 <pr-number>"
    exit 1
fi

PR_NUMBER=$1
TEMP_REMOTE="pr-remote"
REPO_OWNER=$(gh repo view --json owner --jq .owner.login)
REPO_NAME=$(gh repo view --json name --jq .name)

echo -e "${BLUE}Getting information for PR #${PR_NUMBER}...${RESET}"

# Get PR details using GitHub CLI
PR_DATA=$(gh pr view "$PR_NUMBER" --json headRefName,headRepositoryOwner,headRepository,isCrossRepository,headRefOid)

# Extract information from PR data
HEAD_BRANCH=$(echo "$PR_DATA" | jq -r '.headRefName')
HEAD_REPO_OWNER=$(echo "$PR_DATA" | jq -r '.headRepositoryOwner.login')
HEAD_REPO_NAME=$(echo "$PR_DATA" | jq -r '.headRepository.name')
IS_CROSS_REPO=$(echo "$PR_DATA" | jq -r '.isCrossRepository')
HEAD_COMMIT=$(echo "$PR_DATA" | jq -r '.headRefOid')

# Create a local branch name
LOCAL_BRANCH="pr-${PR_NUMBER}-${HEAD_BRANCH}"

echo -e "${BLUE}PR #${PR_NUMBER} information:${RESET}"
echo -e "  Branch: ${YELLOW}${HEAD_BRANCH}${RESET}"
echo -e "  Repository: ${YELLOW}${HEAD_REPO_OWNER}/${HEAD_REPO_NAME}${RESET}"
echo -e "  Commit: ${YELLOW}${HEAD_COMMIT}${RESET}"

# Save the current branch to return to it later
CURRENT_BRANCH=$(git branch --show-current)

# Function to handle cleanup
cleanup() {
    local result=$?
    
    # Return to the original branch
    echo -e "${BLUE}Returning to the original branch: ${CURRENT_BRANCH}${RESET}"
    git checkout "$CURRENT_BRANCH" >/dev/null 2>&1 || echo -e "${RED}Failed to return to original branch${RESET}"
    
    # Ask if we should remove the local branch
    if [ -n "$(git branch --list "$LOCAL_BRANCH")" ]; then
        read -r -p "$(echo -e ${YELLOW}Do you want to delete the local branch ${LOCAL_BRANCH}? [Y/n] ${RESET})" response
        response=${response:-Y}
        if [[ "$response" =~ ^[Yy]$ ]]; then
            git branch -D "$LOCAL_BRANCH" >/dev/null 2>&1 && echo -e "${GREEN}Deleted branch ${LOCAL_BRANCH}${RESET}" || echo -e "${RED}Failed to delete branch ${LOCAL_BRANCH}${RESET}"
        fi
    fi
    
    # Remove the temporary remote if it exists
    if git remote | grep -q "$TEMP_REMOTE"; then
        git remote remove "$TEMP_REMOTE" >/dev/null 2>&1 && echo -e "${GREEN}Removed temporary remote ${TEMP_REMOTE}${RESET}" || echo -e "${RED}Failed to remove temporary remote${RESET}"
    fi
    
    exit $result
}

# Set the cleanup function to run on script exit
trap cleanup EXIT

# Check out the PR branch
if [ "$IS_CROSS_REPO" = "true" ]; then
    echo -e "${BLUE}PR is from a fork. Setting up remote and fetching...${RESET}"
    
    # Check if the remote already exists
    if git remote | grep -q "$TEMP_REMOTE"; then
        echo -e "${YELLOW}Remote ${TEMP_REMOTE} already exists. Removing it first.${RESET}"
        git remote remove "$TEMP_REMOTE"
    fi
    
    # Add the remote repository
    FORK_URL="https://github.com/${HEAD_REPO_OWNER}/${HEAD_REPO_NAME}.git"
    echo -e "${BLUE}Adding remote: ${FORK_URL}${RESET}"
    git remote add "$TEMP_REMOTE" "$FORK_URL"
    
    # Fetch the branch
    echo -e "${BLUE}Fetching branch ${HEAD_BRANCH} from remote...${RESET}"
    git fetch "$TEMP_REMOTE" "$HEAD_BRANCH"
    
    # Create and check out a local branch
    echo -e "${BLUE}Creating local branch ${LOCAL_BRANCH}...${RESET}"
    git checkout -B "$LOCAL_BRANCH" "$TEMP_REMOTE/$HEAD_BRANCH"
else
    echo -e "${BLUE}PR is from the same repository. Checking out branch...${RESET}"
    
    # Fetch the latest changes
    echo -e "${BLUE}Fetching latest changes...${RESET}"
    git fetch origin
    
    # Create and check out a local branch
    echo -e "${BLUE}Creating local branch ${LOCAL_BRANCH}...${RESET}"
    git checkout -B "$LOCAL_BRANCH" "origin/$HEAD_BRANCH"
fi

# Verify we're on the correct commit
CURRENT_COMMIT=$(git rev-parse HEAD)
if [ "$CURRENT_COMMIT" != "$HEAD_COMMIT" ]; then
    echo -e "${RED}Warning: Current commit (${CURRENT_COMMIT}) doesn't match the PR head commit (${HEAD_COMMIT})${RESET}"
    read -r -p "$(echo -e ${YELLOW}Continue anyway? [y/N] ${RESET})" continue_response
    continue_response=${continue_response:-N}
    if [[ ! "$continue_response" =~ ^[Yy]$ ]]; then
        echo -e "${RED}Aborting...${RESET}"
        exit 1
    fi
fi

echo -e "${GREEN}Successfully checked out PR #${PR_NUMBER} to branch ${LOCAL_BRANCH}${RESET}"
echo -e "${BLUE}Running signoff script...${RESET}"

# Run the signoff script
./scripts/signoff.sh

echo -e "${GREEN}Signoff process completed for PR #${PR_NUMBER}${RESET}"
