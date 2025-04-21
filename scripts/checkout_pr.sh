#!/usr/bin/env bash
#
# checkout_pr.sh – Check out the branch for a given GitHub Pull Request.
# Similar to signoff_pr.sh but **only** performs the checkout (no sign‑off),
# adding a temporary remote for cross‑repository PRs when necessary.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'

if [[ -z "${1:-}" ]]; then
  echo -e "${RED}Error: PR number is required${RESET}" >&2
  echo "Usage: $0 <pr-number>" >&2
  exit 1
fi

PR_NUMBER=$1
TEMP_REMOTE="pr-remote-${PR_NUMBER}"

echo -e "${BLUE}Fetching metadata for PR #${PR_NUMBER}...${RESET}"

# Query GitHub for PR metadata
PR_JSON=$(gh pr view "$PR_NUMBER" --json headRefName,headRepositoryOwner,headRepository,isCrossRepository,headRefOid)

HEAD_BRANCH=$(jq -r '.headRefName' <<<"$PR_JSON")
HEAD_REPO_OWNER=$(jq -r '.headRepositoryOwner.login' <<<"$PR_JSON")
HEAD_REPO_NAME=$(jq -r '.headRepository.name'  <<<"$PR_JSON")
IS_CROSS_REPO=$(jq -r '.isCrossRepository'     <<<"$PR_JSON")
HEAD_COMMIT=$(jq  -r '.headRefOid'             <<<"$PR_JSON")

LOCAL_BRANCH="pr-${PR_NUMBER}-${HEAD_BRANCH}"

echo -e "${BLUE}→ Branch:${RESET}   ${YELLOW}${HEAD_BRANCH}${RESET}"
echo -e "${BLUE}→ Repo:${RESET}     ${YELLOW}${HEAD_REPO_OWNER}/${HEAD_REPO_NAME}${RESET}"
echo -e "${BLUE}→ Commit:${RESET}   ${YELLOW}${HEAD_COMMIT}${RESET}"

if [[ "$IS_CROSS_REPO" == "true" ]]; then
  echo -e "${BLUE}PR originates from a fork – adding temporary remote${RESET}"
  [[ $(git remote) =~ $TEMP_REMOTE ]] && git remote remove "$TEMP_REMOTE"
  git remote add "$TEMP_REMOTE" "https://github.com/${HEAD_REPO_OWNER}/${HEAD_REPO_NAME}.git"
  git fetch "$TEMP_REMOTE" "$HEAD_BRANCH"
  git checkout -B "$LOCAL_BRANCH" "$TEMP_REMOTE/$HEAD_BRANCH"
else
  echo -e "${BLUE}PR originates from the main repository${RESET}"
  git fetch origin "$HEAD_BRANCH"
  git checkout -B "$LOCAL_BRANCH" "origin/$HEAD_BRANCH"
fi

# Confirm we are on the expected commit
if [[ "$(git rev-parse HEAD)" != "$HEAD_COMMIT" ]]; then
  echo -e "${YELLOW}Warning:${RESET} Checked‑out commit differs from PR head" >&2
fi

echo -e "${GREEN}Checked out PR #${PR_NUMBER} to ${LOCAL_BRANCH}${RESET}"

echo -e "\n${YELLOW}NOTE:${RESET} When you are done with this branch you can run 'make delete-branch' to clean it up."

