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

    # Upload assets to the release
    for asset in "$assets_dir"/*.zip; do
        gh release upload "$tag" "$asset" --repo "$repo"
    done
}

# Function to calculate SHA256 checksum
calculate_sha256() {
    local file=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        shasum -a 256 "$file" | cut -d ' ' -f 1
    else
        sha256sum "$file" | cut -d ' ' -f 1
    fi
}

# Function to update Homebrew formula with new version and SHA256 checksums
update_homebrew_formula() {
    local version=$1
    local formula_file="$homebrew_dir/sequin.rb"
    local assets_dir="release_assets"

    # Update version
    sed -i.bak "s|version \".*\"|version \"$version\"|" "$formula_file"

    # Update SHA256 checksums
    local darwin_arm64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-darwin-arm64.zip")
    local darwin_amd64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-darwin-amd64.zip")
    local linux_arm64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-linux-arm64.zip")
    local linux_amd64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-linux-amd64.zip")

    sed -i.bak "s|sha256 \".*\"|sha256 \"$darwin_arm64_sha\"|" "$formula_file"
    sed -i.bak "s|sha256 \".*\"|sha256 \"$darwin_amd64_sha\"|" "$formula_file"
    sed -i.bak "s|sha256 \".*\"|sha256 \"$linux_arm64_sha\"|" "$formula_file"
    sed -i.bak "s|sha256 \".*\"|sha256 \"$linux_amd64_sha\"|" "$formula_file"

    # Remove backup files
    rm "${formula_file}.bak"
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

# Update Homebrew formula
update_homebrew_formula "$new_version"

if [[ "$DRY_RUN" == true ]]; then
    echo -e "${YELLOW}Dry run mode: The following actions were performed locally:${RESET}"
    echo "1. Created compiled releases for version $new_version"
    echo "2. Updated Homebrew formula with new version and SHA256 checksums"

    echo -e "\n${GREEN}Changes in Homebrew formula:${RESET}"
    if [ -f "$homebrew_dir/sequin.rb" ]; then
        diff -u <(git show HEAD:"$homebrew_dir/sequin.rb") "$homebrew_dir/sequin.rb" || true
    else
        echo "Unable to show diff: $homebrew_dir/sequin.rb not found"
    fi

    echo -e "\n${YELLOW}The following actions would be performed in a real run:${RESET}"
    echo "1. Create and push git tag $new_version"
    echo "2. Create GitHub release for $new_version and upload assets"
    echo "3. Commit and push changes to Homebrew formula"

    # Revert changes to Homebrew formula
    if [ -f "$homebrew_dir/sequin.rb" ]; then
        git -C "$homebrew_dir" checkout sequin.rb
        echo -e "\n${GREEN}Changes to Homebrew formula have been reverted.${RESET}"
    fi

    # Clean up assets
    rm -rf release_assets

    exit 0
fi

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Create a GitHub release for the new tag and upload assets
create_github_release "$new_version"

# Commit and push the changes
git add "$homebrew_dir/sequin.rb"
git commit -m "Bump sequin-cli version to $new_version and update SHA256 checksums"
git push origin main

echo "Homebrew formula updated with new version and SHA256 checksums, and pushed to GitHub"

# Clean up assets
rm -rf release_assets

echo "GitHub release created for $new_version with assets"