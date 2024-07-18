#!/bin/bash
set -e

RED='\033[0;31m'
RESET='\033[0m'

# Parse command line arguments
DIRTY=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dirty) DIRTY=true ;;
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

    # Upload assets to the release
    for asset in "$assets_dir"/*.zip; do
        gh release upload "$tag" "$asset" --repo "$repo"
    done
}

if [[ "$DIRTY" == false ]] && [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't release a dirty repository. Use 'make release-dirty' (or --dirty if calling directly) to override.${RESET}" >&2
    git status
    exit 1
fi

# Set the working directory to this directory
cd "$(dirname "$0")" || exit

settings_file="../.settings.json"
if [ ! -f "$settings_file" ]; then
    echo "Error: .settings.json file not found. Please run 'make init' in the project's root dir to create it and set the homebrewDir."
    exit 1
fi

homebrew_dir=$(jq -r '.homebrewDir // empty' "$settings_file")

if [ -z "$homebrew_dir" ]; then
    echo "Error: homebrewDir not set in top-level .settings.json. Please set it and try again."
    exit 1
fi

# Get the latest tag
latest_tag=$(get_latest_tag)
echo "Current version: $latest_tag"

# Prompt for the new version
read -p "Enter the new version: " new_version

# Create compiled releases
./build_releases.sh $new_version release_assets

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Create a GitHub release for the new tag and upload assets
create_github_release "$new_version"

# Clean up assets
rm -rf release_assets

echo "GitHub release created for $new_version with assets"

# Switch to homebrew-sequin directory
cd "$homebrew_dir" || exit

# Pull the latest changes from the homebrew-sequin repository
git pull origin main

# Update the version in sequin.rb
sed -i '' "s/tag: \".*\"/tag: \"$new_version\"/" sequin.rb

# Commit and push the changes
git add sequin.rb
git commit -m "Bump sequin-cli version to $new_version"
git push origin main

echo "Homebrew formula updated and pushed to GitHub"