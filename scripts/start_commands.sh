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

set_agent_address() {
  # Use STATSD_HOST if set, otherwise get from ECS metadata endpoint
  if [ -n "${STATSD_HOST:-}" ]; then
    AGENT_ADDRESS="$STATSD_HOST"
  else
    AGENT_ADDRESS=$(curl -s -S http://169.254.169.254/latest/meta-data/local-ipv4 || echo "meta-data curl failed")
  fi

  echo "Datadog Agent Address: $AGENT_ADDRESS"

  export AGENT_ADDRESS
  export OTEL_EXPORTER_OTLP_ENDPOINT="http://$AGENT_ADDRESS:4318"
}

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

if [ "${SELF_HOSTED:-0}" = "0" ]; then
    echo "Setting agent address for cloud deployment"
    set_agent_address
fi

migrate
apply_config
start_application
