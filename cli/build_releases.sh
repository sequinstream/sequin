#!/bin/bash

package_name="sequin-cli"
if [ -z "$1" ]; then
    echo "Error: Version not provided. Please provide a version as the first argument."
    exit 1
fi
version="$1"
if [ -z "$2" ]; then
    echo "Error: Release assets directory not provided. Please provide a release assets directory as the second argument."
    exit 1
fi
release_assets_dir="$2"
platforms=("windows/amd64" "windows/386" "darwin/amd64" "darwin/arm64" "linux/amd64" "linux/386" "linux/arm" "linux/arm64")

# Create release_assets directory if it doesn't exist
mkdir -p $release_assets_dir

for platform in "${platforms[@]}"
do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}
    output_name=$package_name'-'$version'-'$GOOS'-'$GOARCH
    if [ $GOOS = "windows" ]; then
        output_name+='.exe'
    fi    

    env GOOS=$GOOS GOARCH=$GOARCH go build -o $output_name .
    if [ $? -ne 0 ]; then
        echo 'An error has occurred! Aborting the script execution...'
        exit 1
    fi
    
    zip_name="$release_assets_dir/$package_name-$version-$GOOS-$GOARCH.zip"
    zip -r $zip_name $output_name
    rm $output_name
done