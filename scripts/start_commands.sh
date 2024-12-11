#!/bin/bash

# Exit if any command returns a non-zero status.
set -euo pipefail

# Only set RELEASE_NODE if AUTO_ASSIGN_RELEASE_NODE is true and RELEASE_NODE is not already set
if [ "${AUTO_ASSIGN_RELEASE_NODE:-false}" = "true" ]; then
    # Generate random string (8 characters)
    RANDOM_ID=$(head -c 8 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 8)
    export RELEASE_NODE="sequin-${RANDOM_ID}@${RELEASE_HOST:-$(hostname -i | awk '{print $1}')}"
    echo "Generated node name: $RELEASE_NODE"
fi

migrate() {
  echo "Starting migrations"
  ./prod/rel/sequin/bin/sequin eval "Sequin.Release.migrate"
  echo 'Migrations complete'
}

apply_config() {
  echo "Applying config"
  ./prod/rel/sequin/bin/sequin eval "Sequin.YamlLoader.apply!"
  echo "Config applied"
}

start_application() {
  echo "Starting the app"
  PHX_SERVER=true ./prod/rel/sequin/bin/sequin start
}

# Main script execution starts here
echo "Starting: start_commands.sh"

migrate
apply_config
start_application
