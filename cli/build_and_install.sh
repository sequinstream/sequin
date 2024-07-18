#!/bin/bash

# Set the project root directory
PROJECT_ROOT=$(pwd)

# Check if the prod flag is set
if [ "$1" == "prod" ]; then
    BUILD_TAGS="-tags prod"
    echo "Building sequin-cli (production version)..."
else
    BUILD_TAGS=""
    echo "Building sequin-cli (development version)..."
fi

# Build the binary
go build $BUILD_TAGS -o sequin

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

# Create a directory in the user's home for the binary
INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"

# Move the binary to the installation directory
echo "Installing sequin to $INSTALL_DIR..."
mv sequin "$INSTALL_DIR/"

# Add the installation directory to PATH if it's not already there
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo "Adding $INSTALL_DIR to PATH..."
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.zshrc"
    export PATH="$INSTALL_DIR:$PATH"
fi

echo "Installation complete."