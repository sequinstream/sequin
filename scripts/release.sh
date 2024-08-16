#!/bin/bash
set -e

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
RESET='\033[0m'

# Parse command line arguments
DIRTY=false
DRY_RUN=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dirty) DIRTY=true ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Function to get the latest tag from GitHub
get_latest_tag() {
    git fetch --tags
    git describe --tags --abbrev=0
}

# Function to create a GitHub release with assets
create_github_release() {
    local tag=$1
    local repo="sequinstream/sequin"
    local assets_dir="release_assets"
    
    # Create a release using GitHub CLI
    gh release create "$tag" \
        --repo "$repo" \
        --title "Release $tag" \
        --notes "Release notes for $tag" \
        --generate-notes
}

if [[ "$DIRTY" == false ]] && [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't release a dirty repository. Use 'make release-dirty' (or --dirty if calling directly) to override.${RESET}" >&2
    git status
    exit 1
fi

# If dirty flag is set, show git status
if [[ "$DIRTY" = true ]]; then
    echo -e "${YELLOW}Warning: Running release on a dirty repository.${RESET}"
    echo -e "${YELLOW}Current git status:${RESET}"
    git status --short
    echo ""
fi

# Get the latest tag
latest_tag=$(get_latest_tag)
echo "Current version: $latest_tag"

# Prompt for the new version
read -p "Enter the new version: " new_version

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Check if we're in the correct directory
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}Error: Dockerfile not found. Make sure you're in the server directory.${RESET}"
    exit 1
fi

# Execute the Docker build command
echo "Building and pushing Docker image..."
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t sequin/sequin:latest \
    -t sequin/sequin:"$new_version" \
    . \
    --push

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Docker image built and pushed successfully.${RESET}"
else
    echo -e "${RED}Error: Docker build or push failed.${RESET}"
    exit 1
fi

# Create a GitHub release for the new tag and upload assets
create_github_release "$new_version"

echo -e "${GREEN}GitHub release created for $new_version with assets${RESET}"
echo -e "${GREEN}Release process completed successfully!${RESET}"