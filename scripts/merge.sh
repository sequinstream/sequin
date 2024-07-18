#!/usr/bin/env bash

# Abort on any error
set -e

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'

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
        STATUSES=$(gh api --method GET -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
            "/repos/${OWNER}/${REPO}/commits/${SHA}/statuses")

        if echo "${STATUSES}" | jq -e '.[] | select(.state == "success" and .context == "signoff")' > /dev/null; then
            echo -e "${GREEN}Commit was successfully signed off. Proceeding with merge...${RESET}"

            if gh pr merge "${PR_NUMBER}" --repo "${OWNER}/${REPO}" --rebase --admin; then
                echo -e "${GREEN}Pull request #${PR_NUMBER} merged successfully with rebase.${RESET}"
            else
                echo -e "${RED}Failed to merge pull request #${PR_NUMBER}.${RESET}"
                exit 1
            fi
        else
            echo -e "${RED}Commit has not been successfully signed off or signoff not found. Merge aborted.${RESET}"
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