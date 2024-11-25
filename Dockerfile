ARG ELIXIR_VERSION=1.17.2
ARG OTP_VERSION=27.0.1
ARG DEBIAN_VERSION=buster-20240612-slim

ARG BUILDER_IMAGE="hexpm/elixir:${ELIXIR_VERSION}-erlang-${OTP_VERSION}-debian-${DEBIAN_VERSION}"
ARG RUNNER_IMAGE="debian:${DEBIAN_VERSION}"

ARG SELF_HOSTED=0

# ---- Build Stage ----
FROM ${BUILDER_IMAGE} AS builder

# Pass the SELF_HOSTED arg as an environment variable
ARG SELF_HOSTED
ENV SELF_HOSTED=${SELF_HOSTED}

# install build dependencies
RUN apt-get update -y && apt-get install -y build-essential git curl \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

# install nodejs for build stage
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs

# prepare build dir
RUN mkdir /app
WORKDIR /app

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Set environment variables for building the application
ENV MIX_ENV="prod"
ENV LANG=C.UTF-8
ENV ERL_FLAGS="+JPperf true"

# install mix dependencies
COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV
RUN mkdir config

# copy compile-time config files before we compile dependencies
# to ensure any relevant config change will trigger the dependencies
# to be re-compiled.
COPY config/config.exs config/${MIX_ENV}.exs config/
RUN mix deps.compile

COPY priv priv

COPY lib lib

COPY assets assets

# install all npm packages in assets directory
WORKDIR /app/assets
RUN npm install

# change back to build dir
WORKDIR /app

# compile assets
RUN mix assets.deploy

# Compile the release
RUN mix compile

# Changes to config/runtime.exs don't require recompiling the code
COPY config/runtime.exs config/

COPY rel rel
RUN mix release

# start a new build stage so that the final image will only contain
# the compiled release and other runtime necessities
# ---- App Stage ----
FROM ${RUNNER_IMAGE} AS app

# Pass the SELF_HOSTED arg again in this stage
ARG SELF_HOSTED
ENV SELF_HOSTED=${SELF_HOSTED}

RUN apt-get update -y && \
    apt-get install -y libstdc++6 openssl libncurses5 locales ca-certificates curl ssh jq telnet netcat htop \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

# Set the locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Copy over the build artifact from the previous step and create a non root user
RUN useradd --create-home app
WORKDIR /home/app
COPY --from=builder --chown=app /app/_build .

COPY .iex.exs .
COPY scripts/start_commands.sh /scripts/start_commands.sh
RUN chmod +x /scripts/start_commands.sh

USER app

# Make port 4000 available to the world outside this container
EXPOSE 4000

# run the start-up script which run migrations and then the app
ENTRYPOINT ["/scripts/start_commands.sh"]
