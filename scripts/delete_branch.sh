#!/usr/bin/env bash
#
# delete_branch.sh – deletes the **current** branch after user confirmation.
# If the branch tracks a remote that is **not** 'origin', offer to remove that
# remote too.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RESET='\033[0m'

CURRENT_BRANCH=$(git symbolic-ref --short HEAD)

# Prevent accidents on protected branches
case "$CURRENT_BRANCH" in
  main|master|develop)
    echo -e "${RED}Refusing to delete protected branch '${CURRENT_BRANCH}'.${RESET}" >&2
    exit 1
    ;;
esac

read -r -p "$(echo -e ${YELLOW}Delete local branch '${CURRENT_BRANCH}'? [y/N] ${RESET})" confirm
confirm=${confirm:-N}
if [[ ! $confirm =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 0
fi

# Determine upstream
UPSTREAM=$(git for-each-ref --format='%(upstream:short)' "$(git symbolic-ref -q HEAD)" || true)
REMOTE=""
if [[ -n "$UPSTREAM" ]]; then
  REMOTE="${UPSTREAM%%/*}"
fi

# Switch away and delete branch
git checkout main >/dev/null 2>&1 || git checkout -
git branch -D "$CURRENT_BRANCH"
echo -e "${GREEN}Deleted local branch '${CURRENT_BRANCH}'.${RESET}"

# Handle remote cleanup
if [[ -n "$REMOTE" && "$REMOTE" != origin ]]; then
  read -r -p "$(echo -e ${YELLOW}Remove remote '${REMOTE}' as well? [y/N] ${RESET})" rm_remote
  rm_remote=${rm_remote:-N}
  if [[ $rm_remote =~ ^[Yy]$ ]]; then
    git remote remove "$REMOTE"
    echo -e "${GREEN}Remote '${REMOTE}' removed.${RESET}"
  fi
fi
