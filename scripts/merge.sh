#!/usr/bin/env bash

# Abort on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'
YELLOW='\033[0;33m'

# Check for force flag
FORCE=false
if [ "$1" = "--force" ]; then
    FORCE=true
    echo -e "${YELLOW}Force merge option detected. Signoff check will be bypassed.${RESET}"
fi

# Repository introspection
OWNER=$(gh repo view --json owner --jq .owner.login)
REPO=$(gh repo view --json name --jq .name)
SHA=$(git rev-parse HEAD)
LOCAL_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

echo -e "${BLUE}Fetching open PRs matching current branch and SHA...${RESET}"

PR_DETAILS=$(gh pr list --repo "${OWNER}/${REPO}" --json number,headRefName,headRefOid,mergeable \
    --jq "[.[] | select(.headRefOid == \"${SHA}\" and .headRefName == \"${LOCAL_BRANCH_NAME}\")]")

echo -e "${BLUE}PR Details: ${PR_DETAILS}${RESET}"

PR_COUNT=$(echo "${PR_DETAILS}" | jq length)

if [ "${PR_COUNT}" -eq 0 ]; then
    echo -e "${RED}No open pull request found for branch ${LOCAL_BRANCH_NAME} with SHA ${SHA}. Merge aborted.${RESET}"
    exit 1
elif [ "${PR_COUNT}" -eq 1 ]; then
    PR_NUMBER=$(echo "${PR_DETAILS}" | jq -r '.[0].number')
    MERGEABLE_STATUS=$(echo "${PR_DETAILS}" | jq -r '.[0].mergeable')

    echo -e "${BLUE}PR Number: ${PR_NUMBER}${RESET}"
    echo -e "${BLUE}Mergeable Status: ${MERGEABLE_STATUS}${RESET}"

    if [ "${MERGEABLE_STATUS}" = "MERGEABLE" ]; then
        if [ "$FORCE" = true ]; then
            echo -e "${YELLOW}Bypassing signoff check due to force option.${RESET}"
        else
            STATUSES=$(gh api --method GET -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
                "/repos/${OWNER}/${REPO}/commits/${SHA}/statuses")

            if ! echo "${STATUSES}" | jq -e '.[] | select(.state == "success" and .context == "signoff")' > /dev/null; then
                echo -e "${RED}Commit has not been successfully signed off or signoff not found. Merge aborted.${RESET}"
                echo -e "${YELLOW}Use 'make merge-force' to bypass this check.${RESET}"
                exit 1
            fi
        fi

        echo -e "${GREEN}Proceeding with merge...${RESET}"

        if gh pr merge "${PR_NUMBER}" --repo "${OWNER}/${REPO}" --rebase --admin; then
            echo -e "${GREEN}Pull request #${PR_NUMBER} merged successfully with rebase.${RESET}"

            # Check if this is a Graphite tracked branch
            GT_STATE=$(gt state 2>/dev/null || echo '{}')
            IS_GRAPHITE_BRANCH=$(echo "${GT_STATE}" | jq "has(\"${LOCAL_BRANCH_NAME}\")")

            # Prompt to delete local branch and pull changes
            read -p "$(echo -e "${BLUE}Delete local branch '${LOCAL_BRANCH_NAME}' and pull latest changes? [Y/n] ${RESET}")" RESPONSE
            RESPONSE=${RESPONSE:-Y}  # Default to Y if empty

            if [[ "$RESPONSE" =~ ^[Yy]$ ]]; then
                # If this was a Graphite branch, run gt sync
                if [ "${IS_GRAPHITE_BRANCH}" = "true" ]; then
                    echo -e "${BLUE}Detected Graphite branch, running gt sync...${RESET}"
                    gt sync
                else
                    echo -e "${BLUE}Checking out main branch...${RESET}"
                    git checkout main

                    echo -e "${BLUE}Deleting local branch '${LOCAL_BRANCH_NAME}'...${RESET}"
                    git branch -D "${LOCAL_BRANCH_NAME}"

                    echo -e "${BLUE}Pulling latest changes...${RESET}"
                    git pull
                fi

                echo -e "${GREEN}Branch cleanup complete.${RESET}"
            else
                echo -e "${YELLOW}Branch cleanup skipped.${RESET}"
            fi
        else
            echo -e "${RED}Failed to merge pull request #${PR_NUMBER}.${RESET}"
            exit 1
        fi
    else
        echo -e "${RED}Pull request #${PR_NUMBER} is not mergeable. Merge aborted.${RESET}"
        exit 1
    fi
else
    echo -e "${RED}Multiple open pull requests found for branch ${LOCAL_BRANCH_NAME} with SHA ${SHA}. Merge aborted.${RESET}"
    exit 1
fi
