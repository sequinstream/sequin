# Installing Sequin

Sequin is composed of a server and a CLI. The server is best run as a Docker container. The CLI is a binary and is pre-compiled for most operating systems and architectures.

## Sequin server installation

Sequin server can be installed and started alongside Postgres so you can quickly get started. Alternatively, Sequin can be added to an existing `docker-compose.yaml` file in your project.

### Docker compose

The easiest way to get started is to use the `docker-compose.yaml` file in the repository to start a Postgres instance and Sequin server.

Git clone the repository and run `docker compose up -d` in the `docker` directory:

```bash
git clone git@github.com:sequinstream/sequin.git
cd sequin/docker
docker compose up -d
```

### Docker compose with existing project

If you are adding Sequin to an existing `docker-compose.yaml` file in your project, you can add the following to the `services` section:

```yaml
services:
  ...
  sequin:
    image: sequin/sequin:latest
    ports:
      - "7376:7376"
    environment:
      - PG_PORT=<port>
      - PG_HOSTNAME=postgres
      - PG_DATABASE=sequin
      - PG_USERNAME=postgres
      - PG_PASSWORD=postgres
      - SECRET_KEY_BASE=wDPLYus0pvD6qJhKJICO4dauYPXfO/Yl782Zjtpew5qRBDp7CZvbWtQmY0eB13If
      - VAULT_KEY=2Sig69bIpuSm2kv0VQfDekET2qy8qUZGI8v3/h3ASiY=
    depends_on:
      - postgres
```

Make sure to configure the `PG_PORT` environment variable to match the port of your Postgres instance, usually `5432`.

## Sequin CLI installation

Install with Homebrew:

```
brew tap sequin-io/sequin git@github.com:sequin-io/homebrew-sequin
brew install sequin
```

Install with shell:

```bash
curl -sf https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh | sh
```
