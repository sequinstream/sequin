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

# Parse arguments
SKIP_SIGNOFF=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --no-signoff) SKIP_SIGNOFF=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Set the working directory to the project root
cd "$(dirname "$0")/.." || exit

# Fetch latest from origin (main branch and tags)
# Note: We always tag origin/main, so local branch/state doesn't matter
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

# Verify the commit has been signed off (via GitHub Actions check run)
# Polls until signoff passes, fails, or times out
COMMIT_SHA=$(git rev-parse origin/main)
COMMIT_SHORT=$(git rev-parse --short origin/main)

if [ "$SKIP_SIGNOFF" = true ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Skipping signoff check (--no-signoff)${RESET}"
else
    echo ""
    echo "Verifying commit signoff..."

    MAX_ATTEMPTS=60  # 10 minutes max (60 * 10 seconds)
    ATTEMPT=1

    while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        # GitHub Actions creates "check runs", not "commit statuses" - use the check-runs API
        CHECK_RUNS=$(gh api \
            -H "Accept: application/vnd.github+json" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            "/repos/sequinstream/sequin/commits/$COMMIT_SHA/check-runs" 2>/dev/null || echo '{"check_runs":[]}')

        SIGNOFF_SUCCESS=$(echo "$CHECK_RUNS" | jq '[.check_runs[] | select(.name=="signoff" and .conclusion=="success")] | length')
        SIGNOFF_FAILED=$(echo "$CHECK_RUNS" | jq '[.check_runs[] | select(.name=="signoff" and .conclusion=="failure")] | length')
        SIGNOFF_PENDING=$(echo "$CHECK_RUNS" | jq '[.check_runs[] | select(.name=="signoff" and .status=="in_progress")] | length')
        SIGNOFF_QUEUED=$(echo "$CHECK_RUNS" | jq '[.check_runs[] | select(.name=="signoff" and .status=="queued")] | length')

        if [ "$SIGNOFF_SUCCESS" -gt 0 ]; then
            echo -e "${GREEN}✓ Commit $COMMIT_SHORT has been signed off${RESET}"
            break
        elif [ "$SIGNOFF_FAILED" -gt 0 ]; then
            echo -e "${RED}✗ Signoff failed for commit $COMMIT_SHORT${RESET}"
            echo "  Check the workflow run: https://github.com/sequinstream/sequin/commit/$COMMIT_SHA"
            exit 1
        elif [ "$SIGNOFF_PENDING" -gt 0 ] || [ "$SIGNOFF_QUEUED" -gt 0 ]; then
            if [ $ATTEMPT -eq 1 ]; then
                echo -e "${YELLOW}⏳ Signoff is running for commit $COMMIT_SHORT, waiting...${RESET}"
            fi
            printf "."
            sleep 10
            ATTEMPT=$((ATTEMPT + 1))
        else
            # No signoff check found yet - might not have started
            if [ $ATTEMPT -eq 1 ]; then
                echo -e "${YELLOW}⏳ Waiting for signoff workflow to start...${RESET}"
            fi
            printf "."
            sleep 10
            ATTEMPT=$((ATTEMPT + 1))
        fi
    done

    # Check if we exited the loop due to timeout
    if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
        echo ""
        echo -e "${RED}✗ Timeout waiting for signoff (10 minutes)${RESET}"
        echo "  Check status at: https://github.com/sequinstream/sequin/commit/$COMMIT_SHA"
        exit 1
    fi

    # Add newline after dots
    if [ $ATTEMPT -gt 1 ]; then
        echo ""
    fi
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
echo "Waiting for release workflow to start..."
sleep 5

# Find and watch the release workflow run
RUN_ID=$(gh run list --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')

if [ -n "$RUN_ID" ]; then
    echo "Watching release workflow (run $RUN_ID)..."
    echo ""
    gh run watch "$RUN_ID"
    
    # Check final status
    RUN_STATUS=$(gh run view "$RUN_ID" --json conclusion --jq '.conclusion')
    echo ""
    if [ "$RUN_STATUS" = "success" ]; then
        echo -e "${GREEN}Release workflow completed successfully!${RESET}"
        echo ""
        echo "Release available at:"
        echo "  https://github.com/sequinstream/sequin/releases/tag/$new_version"
    else
        echo -e "${RED}Release workflow failed with status: $RUN_STATUS${RESET}"
        echo "Check the workflow run for details:"
        echo "  https://github.com/sequinstream/sequin/actions/runs/$RUN_ID"
    fi
else
    echo -e "${YELLOW}Could not find workflow run. Check manually:${RESET}"
    echo "  https://github.com/sequinstream/sequin/actions"
fi

