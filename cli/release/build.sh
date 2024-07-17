#!/bin/bash

package_name="sequin-cli"
if [ -z "$1" ]; then
    echo "Error: Version not provided. Please provide a version as the first argument."
    exit 1
fi
version="$1"
platforms=("windows/amd64" "windows/386" "darwin/amd64" "darwin/arm64" "linux/amd64" "linux/386" "linux/arm" "linux/arm64")

# Create release_assets directory if it doesn't exist
mkdir -p release_assets

for platform in "${platforms[@]}"
do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}
    output_name=$package_name'-'$version'-'$GOOS'-'$GOARCH
    if [ $GOOS = "windows" ]; then
        output_name+='.exe'
    fi    

    env GOOS=$GOOS GOARCH=$GOARCH go build -o $output_name ./cli
    if [ $? -ne 0 ]; then
        echo 'An error has occurred! Aborting the script execution...'
        exit 1
    fi
    
    zip_name="release_assets/$package_name-$version-$GOOS-$GOARCH.zip"
    zip -r $zip_name $output_name
    rm $output_name
done