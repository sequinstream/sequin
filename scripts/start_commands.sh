#!/bin/bash

# Exit if any command returns a non-zero status.
set -euo pipefail

# Define app home directory - used for file operations where app user needs write access
APP_HOME_DIR="/home/app"

# Only set RELEASE_NODE if AUTO_ASSIGN_RELEASE_NODE is true and RELEASE_NODE is not already set
if [ "${AUTO_ASSIGN_RELEASE_NODE:-false}" = "true" ]; then
    # Generate random string (8 characters)
    RANDOM_ID=$(head -c 8 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 8)
    export RELEASE_NODE="sequin-${RANDOM_ID}@${RELEASE_HOST:-$(hostname -i | awk '{print $1}')}"
    echo "Generated node name: $RELEASE_NODE"
fi

set_agent_address() {
  # Use STATSD_HOST if set, otherwise get from metadata endpoint
  if [ -n "${STATSD_HOST:-}" ]; then
    AGENT_ADDRESS="$STATSD_HOST"
  else
    # Check if running on Fargate (ECS_CONTAINER_METADATA_URI_V4 will be set)
    if [ -n "${ECS_CONTAINER_METADATA_URI_V4:-}" ]; then
      # Get container metadata from Fargate endpoint
      METADATA=$(curl -s -S "${ECS_CONTAINER_METADATA_URI_V4}/task")
      # Extract container IP from metadata
      AGENT_ADDRESS=$(echo "$METADATA" | jq -r '.Containers[0].Networks[0].IPv4Addresses[0]' 2>/dev/null || echo "fargate-metadata-parse-failed")
    else
      # Use EC2 metadata endpoint as fallback
      TOKEN=$(curl -s -S -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
      AGENT_ADDRESS=$(curl -s -S -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4 || echo "ec2-metadata-curl-failed")
    fi
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

  # Check if CONFIG_FILE_YAML is provided (base64 encoded YAML)
  if [ -n "${CONFIG_FILE_YAML:-}" ]; then
    echo "CONFIG_FILE_YAML environment variable found, decoding and piping to application"
    echo "${CONFIG_FILE_YAML}" \
        | base64 -d \
        | sequin config interpolate - \
        | ./prod/rel/sequin/bin/sequin eval "Sequin.YamlLoader.apply_from_stdin!"
    echo "Config applied from environment variable"
    unset CONFIG_FILE_YAML
  else
    if [ -n "${CONFIG_FILE_PATH:-}" ] && [ -f "${CONFIG_FILE_PATH:-}" ]; then
      echo "Interpolating and applying config from ${CONFIG_FILE_PATH}"
      sequin config interpolate "${CONFIG_FILE_PATH}" \
          | ./prod/rel/sequin/bin/sequin eval "Sequin.YamlLoader.apply_from_stdin!"
      echo "Config applied from file"
    else
      echo "No config file found or path is empty, skipping config loading"
    fi
  fi
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
