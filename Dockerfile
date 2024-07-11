# ---- Build Stage ----
FROM hexpm/elixir:1.17.2-erlang-27.0.1-debian-buster-20240612-slim as build

# install build dependencies
# telnet for debugging purposes
RUN apt-get update && apt-get install build-essential git curl -y

# Clean up
RUN rm -rf /var/lib/apt/lists/*

# Set environment variables for building the application
ENV MIX_ENV=prod
ENV LANG=C.UTF-8

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Create the application build directory
RUN mkdir /app
WORKDIR /app

# install mix dependencies
COPY mix.exs mix.lock config/ ./

RUN mix deps.get --only prod && \
    mix deps.compile

COPY . .

RUN MIX_ENV=prod mix release

# ---- App Stage ----
FROM hexpm/elixir:1.17.2-erlang-27.0.1-debian-buster-20240612-slim as app

ENV LANG=C.UTF-8

# Install openssl
RUN apt-get update && apt-get install -y openssl curl ssh jq telnet netcat htop && \
    rm -rf /var/lib/apt/lists/*

# Copy over the build artifact from the previous step and create a non root user
RUN useradd --create-home app
WORKDIR /home/app
COPY --from=build --chown=app /app/_build .

COPY .iex.exs .
COPY scripts/start_commands.sh /scripts/start_commands.sh
RUN chmod +x /scripts/start_commands.sh

USER app

# Make port 4000 available to the world outside this container
EXPOSE 4000

# run the start-up script which run migrations and then the app
ENTRYPOINT ["/scripts/start_commands.sh"]
