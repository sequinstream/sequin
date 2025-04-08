#!/usr/bin/env bash

set -ex

IMAGE_PREFIX="sequin/sequin"

export IMAGE_PREFIX
export IMAGE_VERSION

# Pull the AMD64 image
docker pull "$IMAGE_PREFIX:$IMAGE_VERSION"

# Directory for our test configuration
TEST_CONFIG="test-config"
REAL_CONFIG="docker"
REAL_COMPOSE_YML="docker-compose.yaml"
TEST_COMPOSE_YML="docker-compose.test.yml"

mkdir -p "$TEST_CONFIG"

# Patch docker-compose file with correct image version
cat <"$REAL_CONFIG/$REAL_COMPOSE_YML" \
  | yq '.services.sequin.image = strenv(IMAGE_PREFIX) + ":" + strenv(IMAGE_VERSION)' \
  | cat >"$TEST_CONFIG/$TEST_COMPOSE_YML"

cp -r "$REAL_CONFIG"/* "$TEST_CONFIG"

cd "$TEST_CONFIG"

# Start the services
docker compose -f "$TEST_COMPOSE_YML" up -d

 # Wait for the application to start (30 seconds max)
echo "Waiting for application to start..."
for i in {1..30}; do
  healthy=$(curl -s http://localhost:7376/health | jq -r .ok)
  if [[ $healthy == "true" ]]; then
    echo "Application started successfully!"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "Application failed to start within 30 seconds"
    docker compose -f "$TEST_COMPOSE_YML" logs
    exit 1
  fi
  sleep 1
done

echo "App is healthy - smoke test passed successfully!"

# Cleanup
docker compose -f "$TEST_COMPOSE_YML" down
rm -rf "$TEST_COMPOSE_YML" test-config
