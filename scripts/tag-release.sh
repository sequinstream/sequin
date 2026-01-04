#!/bin/bash
#
# Tag Release Script
#
# This script prompts for a version number and pushes a tag to GitHub.
# The tag push triggers the GitHub Actions release workflow which handles:
#   - Building CLI binaries for all platforms
#   - Building and pushing Docker images
#   - Generating release notes with Claude Code
#   - Creating the GitHub release
#   - Updating the Homebrew formula
#
# Usage: ./scripts/tag-release.sh
#
# For emergency local releases (if GitHub Actions is unavailable),
# use: make release-local

set -e

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'

# Set the working directory to the project root
cd "$(dirname "$0")/.." || exit

# Check for uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Error: You have uncommitted changes. Please commit or stash them first.${RESET}"
    git status --short
    exit 1
fi

# Make sure we're on main branch
current_branch=$(git branch --show-current)
if [[ "$current_branch" != "main" ]]; then
    echo -e "${YELLOW}Warning: You're on branch '$current_branch', not 'main'.${RESET}"
    read -p "Continue anyway? [y/N] " continue_anyway
    if [[ ! "$continue_anyway" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# Fetch latest from origin (main branch and tags)
echo "Fetching latest from origin..."
git fetch origin main --tags

# Get the latest tag
latest_tag=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
echo -e "Current version: ${CYAN}$latest_tag${RESET}"

# Calculate default next version (increment patch version)
if [[ $latest_tag =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    patch="${BASH_REMATCH[3]}"
    new_patch=$((patch + 1))
    default_new_tag="v$major.$minor.$new_patch"
else
    default_new_tag=""
fi

# Prompt for the new version
echo ""
echo "Enter the new version tag."
echo "  - For a patch release, press Enter to use the default"
echo "  - For a minor release (breaking changes), bump the minor version (e.g., v0.13.0)"
echo ""
read -p "New version${default_new_tag:+ [$default_new_tag]}: " new_version

# Use default if user didn't enter anything
if [ -z "$new_version" ] && [ -n "$default_new_tag" ]; then
    new_version="$default_new_tag"
fi

# Validate version format
if [[ ! "$new_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "${RED}Error: Invalid version format. Expected format: v0.0.0${RESET}"
    exit 1
fi

# Check if tag already exists
if git rev-parse "$new_version" >/dev/null 2>&1; then
    echo -e "${RED}Error: Tag $new_version already exists.${RESET}"
    exit 1
fi

# Verify the commit has been signed off
echo ""
echo "Verifying commit signoff..."
COMMIT_SHA=$(git rev-parse origin/main)
SIGNOFF_STATUS=$(gh api \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/sequinstream/sequin/commits/$COMMIT_SHA/statuses" 2>/dev/null || echo "[]")

SIGNOFF_SUCCESS=$(echo "$SIGNOFF_STATUS" | jq '[.[] | select(.context=="signoff" and .state=="success")] | length')
SIGNOFF_PENDING=$(echo "$SIGNOFF_STATUS" | jq '[.[] | select(.context=="signoff" and .state=="pending")] | length')

if [ "$SIGNOFF_SUCCESS" -gt 0 ]; then
    echo -e "${GREEN}✓ Commit has been signed off${RESET}"
elif [ "$SIGNOFF_PENDING" -gt 0 ]; then
    echo -e "${YELLOW}⏳ Signoff is still running. Please wait for it to complete.${RESET}"
    echo "   Check status at: https://github.com/sequinstream/sequin/commit/$COMMIT_SHA"
    exit 1
else
    echo -e "${RED}✗ Commit has not been signed off.${RESET}"
    echo ""
    echo "This could mean:"
    echo "  1. The signoff workflow hasn't started yet (wait a moment)"
    echo "  2. The signoff workflow failed (check GitHub Actions)"
    echo "  3. This commit was never pushed to main"
    echo ""
    echo "Check status at: https://github.com/sequinstream/sequin/commit/$COMMIT_SHA"
    exit 1
fi

# Show what will happen
echo ""
echo -e "${CYAN}This will:${RESET}"
echo "  1. Create tag $new_version on commit $(git rev-parse --short origin/main) (origin/main)"
echo "  2. Push the tag to GitHub"
echo "  3. Trigger the GitHub Actions release workflow"
echo ""
echo "The GitHub Actions workflow will then:"
echo "  - Build CLI binaries for all platforms"
echo "  - Build Docker images (amd64 + arm64)"
echo "  - Generate release notes with Claude Code"
echo "  - Create the GitHub release with all assets"
echo "  - Update the Homebrew formula"
echo ""

read -p "Proceed? [Y/n] " proceed
if [[ "$proceed" =~ ^[Nn]$ ]]; then
    echo -e "${YELLOW}Aborted.${RESET}"
    exit 0
fi

# Create and push the tag
echo ""
echo "Creating tag $new_version on origin/main..."
git tag "$new_version" origin/main

echo "Pushing tag to GitHub..."
git push origin "$new_version"

echo ""
echo -e "${GREEN}Tag $new_version pushed successfully!${RESET}"
echo ""
echo "GitHub Actions release workflow has been triggered."
echo "Watch progress at: https://github.com/sequinstream/sequin/actions"
echo ""
echo "Once complete, the release will be available at:"
echo "  https://github.com/sequinstream/sequin/releases/tag/$new_version"

