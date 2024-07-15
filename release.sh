#!/bin/bash

# Function to get the latest tag from GitHub
get_latest_tag() {
    git fetch --tags
    git describe --tags --abbrev=0
}

# Function to create a GitHub release
create_github_release() {
    local tag=$1
    local repo="sequin-io/sequin-cli"
    
    # Create a release using GitHub CLI
    gh release create "$tag" \
        --repo "$repo" \
        --title "Release $tag" \
        --notes "Release notes for $tag" \
        --generate-notes
}

# Set the working directory to sequin-cli
cd /Users/carterpedersen/Sequin/sequin-cli || exit

# Get the latest tag
latest_tag=$(get_latest_tag)
echo "Current version: $latest_tag"

# Prompt for the new version
read -p "Enter the new version (e.g., v0.1.3): " new_version

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Create a GitHub release for the new tag
create_github_release "$new_version"

echo "GitHub release created for $new_version"

# Switch to homebrew-sequin directory
cd /Users/carterpedersen/Sequin/homebrew-sequin || exit

# Pull the latest changes from the homebrew-sequin repository
git pull origin main

# Update the version in sequin.rb
sed -i '' "s/tag: \".*\"/tag: \"$new_version\"/" sequin.rb

# Commit and push the changes
git add sequin.rb
git commit -m "Bump sequin-cli version to $new_version"
git push origin main

echo "Homebrew formula updated and pushed to GitHub"