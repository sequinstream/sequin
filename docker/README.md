# Sequin Docker Setup

This directory contains the Docker configuration files for running Sequin and its dependencies.

## Docker Compose Files

### `docker-compose.yaml`

The main docker-compose file for running Sequin locally. It includes:

- **sequin**: The main Sequin application
- **sequin_postgres**: PostgreSQL database for Sequin's internal use and sample data
- **sequin_redis**: Redis for caching and message processing
- **sequin_prometheus**: Prometheus for metrics collection
- **sequin_grafana**: Grafana for metrics visualization with pre-configured dashboards

To start Sequin with the full stack:

```bash
docker compose up
```

This will make Sequin available at http://localhost:7376.

### `docker-compose.ci.yaml`

A simplified version of the docker-compose configuration specifically designed for your CI pipeline or test environment:

- No Prometheus or Grafana services
- No sample data initialization
- Uses environment variable `SEQUIN_IMAGE_VERSION` to control the image version

## Using Sequin in CI Pipelines

We recommend downloading `docker-compose.ci.yaml` and committing a copy to your own repo.
Or, your CI pipeline can have a step to download the Sequin CI compose file:
```bash
curl -O https://raw.githubusercontent.com/sequinstream/sequin/main/docker/docker-compose.ci.yaml -o sequin-ci.yaml
```

### Basic approach

The simplest method is to reference the CI configuration directly:

```bash
docker compose -f docker-compose.ci.yaml up -d
# Run your tests
docker compose -f docker-compose.ci.yaml down
```

### Using `include`

You can use the `include` feature to include Sequin and its dependencies in your own compose file:

1. Inside your own compose file (e.g. `docker-compose.yml`), add an include directive to include the Sequin configuration:

   ```yaml
   include:
     - docker-compose.ci.yaml
   
   services:
     # Your application services
     myapp:
       ...
   ```

2. Create an override file (e.g., `sequin-config.yml`) to customize Sequin for your specific test setup. These attributes will be merged into the `sequin` service definition in Sequin's `docker-compose.ci.yaml` file:

   ```yaml
   services:
     sequin:
       # Overwrite the `image` attribute
       image: sequin/sequin:${SEQUIN_VERSION:-latest}  # Pin version via CI environment variable
       volumes:
         # Overwrite the `volumes` attribute to mount a custom config file
         - ./test-config.yml:/config/test-config.yml
       environment:
         # Overwrite the `environment` attribute to inject your own environment variables into Sequin's docker container
         - CONFIG_FILE_PATH=/config/test-config.yml
         - TEST_DB_HOST=test_db
         - TEST_DB_NAME=test_database
   ```

3. In your CI script, run both files together:

   ```bash
   # Start the services
   docker compose -f my-ci-compose.yml -f sequin-config.yml up -d
   ```

## More docs

For more information on how to use Sequin, visit the [official documentation](https://sequinstream.com/docs). 
