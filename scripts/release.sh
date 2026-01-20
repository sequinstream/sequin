#!/bin/bash
#
# EMERGENCY LOCAL RELEASE SCRIPT
#
# ⚠️  This script is for emergency use only!
#
# The primary release flow is:
#   make release  →  pushes tag  →  GitHub Actions handles everything
#
# Only use this script if:
#   - GitHub Actions is down
#   - There's an urgent release that can't wait
#   - You need to debug the release process locally
#
# This script requires:
#   - Docker daemon running locally
#   - .settings.json with homebrewDir and sentryDSN configured
#   - Local access to the homebrew-sequin repository
#
# Usage: make release-local
#
set -e

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
RESET='\033[0m'

# Function to check if Docker daemon is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}Error: Docker daemon is not running.${RESET}"
        exit 1
    fi
}

# Check Docker daemon status early
check_docker

# Parse command line arguments
DIRTY=false
DRY_RUN=false
GITHUB_ACTIONS=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dirty) DIRTY=true ;;
        --dry-run) DRY_RUN=true ;;
        --github-actions) GITHUB_ACTIONS=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Function to get the latest tag from GitHub
get_latest_tag() {
    git fetch --tags
    git describe --tags --abbrev=0
}

# Function to perform in-place sed substitution compatible with both GNU and BSD sed
inplace_sed() {
    local expression=$1
    local file=$2
    if sed --version >/dev/null 2>&1; then
        # GNU sed
        sed -i.bak "$expression" "$file"
    else
        # BSD sed (macOS)
        sed -i '' "$expression" "$file"
        rm -f "${file}.bak"  # Remove backup file if not needed
    fi
}

# Function to create and upload docker archive
create_docker_archive() {
    local assets_dir=$1
    echo "Creating docker archive..."
    cp -r docker sequin-docker-compose
    zip -r "$assets_dir/sequin-docker-compose.zip" sequin-docker-compose/
    rm -rf sequin-docker-compose
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

    # Create docker archive
    create_docker_archive "$assets_dir"

    # Upload all assets to the release
    for asset in "$assets_dir"/*.zip; do
        gh release upload "$tag" "$asset" --repo "$repo"
    done
}

# Function to calculate SHA256 checksum
calculate_sha256() {
    local file=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        shasum -a 256 "$file" | awk '{ print $1 }'
    else
        sha256sum "$file" | awk '{ print $1 }'
    fi
}

# Function to update Homebrew formula with new version and SHA256 checksums
update_homebrew_formula() {
    local version=$1
    local formula_file="$homebrew_dir/sequin.rb"
    local assets_dir="release_assets"

    # Update version
    inplace_sed "s@version \".*\"@version \"$version\"@" "$formula_file"

    # Update SHA256 checksums
    local darwin_arm64_sha
    local darwin_amd64_sha
    local linux_arm64_sha
    local linux_amd64_sha

    darwin_arm64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-darwin-arm64.zip")
    darwin_amd64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-darwin-amd64.zip")
    linux_arm64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-linux-arm64.zip")
    linux_amd64_sha=$(calculate_sha256 "$assets_dir/sequin-cli-${version}-linux-amd64.zip")

    # Output SHA values for debugging
    echo "darwin_arm64_sha: $darwin_arm64_sha"
    echo "darwin_amd64_sha: $darwin_amd64_sha"
    echo "linux_arm64_sha: $linux_arm64_sha"
    echo "linux_amd64_sha: $linux_amd64_sha"

    inplace_sed "s@sha256 \".*\" # tag:darwin-arm64@sha256 \"$darwin_arm64_sha\" # tag:darwin-arm64@" "$formula_file"
    inplace_sed "s@sha256 \".*\" # tag:darwin-amd64@sha256 \"$darwin_amd64_sha\" # tag:darwin-amd64@" "$formula_file"
    inplace_sed "s@sha256 \".*\" # tag:linux-arm64@sha256 \"$linux_arm64_sha\" # tag:linux-arm64@" "$formula_file"
    inplace_sed "s@sha256 \".*\" # tag:linux-amd64@sha256 \"$linux_amd64_sha\" # tag:linux-amd64@" "$formula_file"
}

# Function to build the CLI for multiple platforms
build_cli() {
    local version=$1
    local release_assets_dir="release_assets"
    local package_name="sequin-cli"
    local platforms=(
        "windows/amd64"
        "windows/386"
        "darwin/amd64"
        "darwin/arm64"
        "linux/amd64"
        "linux/386"
        "linux/arm"
        "linux/arm64"
    )

    mkdir -p "$release_assets_dir"
    cd cli

    for platform in "${platforms[@]}"; do
        IFS="/" read -r GOOS GOARCH <<< "$platform"
        output_name="${package_name}-${version}-${GOOS}-${GOARCH}"
        if [ "$GOOS" = "windows" ]; then
            output_name+=".exe"
        fi

        echo "Building $output_name"
        env GOOS="$GOOS" GOARCH="$GOARCH" go build -tags prod -o "$output_name" .
        if [ $? -ne 0 ]; then
            echo 'An error has occurred! Aborting the script execution...'
            exit 1
        fi

        zip_name="../$release_assets_dir/${package_name}-${version}-${GOOS}-${GOARCH}.zip"
        zip -r "$zip_name" "$output_name"
        rm "$output_name"
    done

    cd ..
}

# Function to build and push Docker image
build_and_push_docker() {
    local version=$1
    local use_github_actions=${2:-false}
    
    # Check if we're in the correct directory
    if [ ! -f "Dockerfile" ]; then
        echo -e "${RED}Error: Dockerfile not found. Make sure you're in the server directory.${RESET}"
        exit 1
    fi

    if [ "$use_github_actions" = true ]; then
        echo "Triggering Docker builds in GitHub Actions..."
        gh workflow run docker-build.yml -f version="$version"

        # Add delay to allow GitHub Actions to start the workflow
        sleep 5
        
        # Wait for workflow to complete
        echo "Waiting for Docker builds to complete in GitHub Actions..."
        gh run watch $(gh run list --workflow=docker-build.yml --limit 1 --json databaseId --jq '.[0].databaseId')
    else
        echo "Building and pushing Docker images locally..."
        # Build amd64 with architecture-specific tag
        echo "Building amd64 image..."
        docker buildx build \
            --platform linux/amd64 \
            --build-arg SELF_HOSTED=1 \
            --build-arg RELEASE_VERSION="$version" \
            --build-arg SENTRY_DSN="$sentry_dsn" \
            --cache-from "type=registry,ref=sequin/sequin:buildcache-amd64" \
            --cache-to "type=registry,ref=sequin/sequin:buildcache-amd64,mode=max" \
            --provenance=false \
            -t sequin/sequin:${version}-amd64 \
            . \
            --push

        if [ $? -ne 0 ]; then
            echo -e "${RED}Error: Docker build failed for amd64.${RESET}"
            exit 1
        fi

        # Build arm64 with architecture-specific tag
        echo "Building arm64 image..."
        docker buildx build \
            --platform linux/arm64 \
            --build-arg SELF_HOSTED=1 \
            --build-arg RELEASE_VERSION="$version" \
            --build-arg SENTRY_DSN="$sentry_dsn" \
            --cache-from "type=registry,ref=sequin/sequin:buildcache-arm64" \
            --cache-to "type=registry,ref=sequin/sequin:buildcache-arm64,mode=max" \
            --provenance=false \
            -t sequin/sequin:${version}-arm64 \
            . \
            --push

        if [ $? -ne 0 ]; then
            echo -e "${RED}Error: Docker build failed for arm64.${RESET}"
            exit 1
        fi

        echo "Creating multi-arch images using imagetools..."

        # Create version-specific multi-arch image
        docker buildx imagetools create -t sequin/sequin:${version} \
            sequin/sequin:${version}-amd64 \
            sequin/sequin:${version}-arm64

        # Create latest multi-arch image
        docker buildx imagetools create -t sequin/sequin:latest \
            sequin/sequin:${version}-amd64 \
            sequin/sequin:${version}-arm64

        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Docker images created and pushed successfully.${RESET}"
        else
            echo -e "${RED}Error: Failed to create multi-arch images.${RESET}"
            exit 1
        fi
    fi
}

if [[ "$DIRTY" == false ]] && [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't release a dirty repository. Use '--dirty' to override.${RESET}" >&2
    git status
    exit 1
fi

# If dirty flag is set, show git status
if [[ "$DIRTY" == true ]]; then
    echo -e "${YELLOW}Warning: Running release on a dirty repository.${RESET}"
    echo -e "${YELLOW}Current git status:${RESET}"
    git status --short
    echo ""
fi

# Set the working directory to the project root
cd "$(dirname "$0")" || exit
cd ..

settings_file=".settings.json"
if [ ! -f "$settings_file" ]; then
    echo "Error: .settings.json file not found. Please run 'make init' in the project's root dir to create it and set the homebrewDir."
    exit 1
fi

# Read all required settings early
homebrew_dir=$(jq -r '.homebrewDir // empty' "$settings_file")
sentry_dsn=$(jq -r '.sentryDSN // empty' "$settings_file")

# Validate all required settings
if [ -z "$homebrew_dir" ]; then
    echo "Error: homebrewDir not set in top-level .settings.json. Please set it and try again."
    exit 1
fi

if [ -z "$sentry_dsn" ]; then
    echo "Error: sentryDSN not set in top-level .settings.json. Please set it and try again."
    exit 1
fi

# Change to homebrew_dir and pull latest changes
echo "Updating Homebrew formula repository..."
current_dir=$(pwd)
cd "$homebrew_dir" || exit
git pull origin main
cd "$current_dir" || exit

# Get the latest tag
latest_tag=$(get_latest_tag)
echo "Current version: $latest_tag"

# Calculate default next version (increment patch version)
if [[ $latest_tag =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    patch="${BASH_REMATCH[3]}"
    # Increment patch version
    new_patch=$((patch + 1))
    default_new_tag="$major.$minor.$new_patch"
    # If original tag had a 'v' prefix, add it to the default
    if [[ $latest_tag == v* ]]; then
        default_new_tag="v$default_new_tag"
    fi
else
    # If we can't parse the version, don't provide a default
    default_new_tag=""
fi

# Prompt for the new version
read -p "Enter the new version${default_new_tag:+ ($default_new_tag)}: " new_version

# Use default if user didn't enter anything
if [ -z "$new_version" ] && [ -n "$default_new_tag" ]; then
    new_version="$default_new_tag"
fi

# Build the CLI
build_cli "$new_version"

# Update Homebrew formula
update_homebrew_formula "$new_version"

if [[ "$DRY_RUN" == true ]]; then
    echo -e "${YELLOW}Dry run mode: The following actions were performed locally:${RESET}"
    echo "1. Built CLI binaries for version $new_version"
    echo "2. Updated Homebrew formula with new version and SHA256 checksums"
    echo "3. Build and push Docker image for $new_version"
    
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
    echo "4. Build and push Docker image for $new_version"

    # Revert changes to Homebrew formula
    if [ -f "$homebrew_dir/sequin.rb" ]; then
        git -C "$homebrew_dir" checkout sequin.rb
        echo -e "\n${GREEN}Changes to Homebrew formula have been reverted.${RESET}"
    fi

    # Clean up assets
    rm -rf release_assets

    exit 0
fi

# Build and push Docker image
build_and_push_docker "$new_version" "$GITHUB_ACTIONS"

# Prompt for confirmation to proceed
read -p "Docker images built and pushed. If you used GitHub Actions, please verify the action succeeded. Do you want to proceed with the release? [Y/n] " proceed
if [[ "$proceed" =~ ^[Nn]$ ]]; then
    echo -e "${YELLOW}Release aborted by user.${RESET}"
    exit 0
fi

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Create a GitHub release for the new tag and upload assets
create_github_release "$new_version"

# Commit and push the changes to the Homebrew formula repository
(
    cd "$homebrew_dir" || exit
    git add sequin.rb
    git commit -m "Bump sequin-cli version to $new_version and update SHA256 checksums"
    git push origin main
)

echo "Homebrew formula updated with new version and SHA256 checksums, and pushed to GitHub"

# Clean up assets
rm -rf release_assets

echo -e "${GREEN}GitHub release created for $new_version with assets${RESET}"
echo -e "${GREEN}Release process completed successfully!${RESET}"