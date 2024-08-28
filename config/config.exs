# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

sequin_config_schema = "sequin_config"
sequin_stream_schema = "sequin_streams"

config :esbuild, :version, "0.17.11"

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :sequin, Oban,
  prefix: sequin_config_schema,
  queues: [default: 10],
  repo: Sequin.Repo

# Configures the mailer
#
# By default it uses the "Local" adapter which stores the emails
# locally. You can see the emails in your browser, at "/dev/mailbox".
#
# For production it's recommended to configure a different adapter
# at the `config/runtime.exs`.
config :sequin, Sequin.Mailer, adapter: Swoosh.Adapters.Local

config :sequin, Sequin.Repo,
  config_schema_prefix: sequin_config_schema,
  stream_schema_prefix: sequin_stream_schema,
  migration_default_prefix: sequin_config_schema,
  migration_primary_key: [
    name: :id,
    type: :binary_id,
    autogenerate: false,
    read_after_writes: true,
    default: {:fragment, "uuid_generate_v4()"}
  ],
  log: false

# Configures the endpoint
config :sequin, SequinWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: SequinWeb.ErrorHTML, json: SequinWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Sequin.PubSub,
  live_view: [signing_salt: "Sm59ovfq"]

config :sequin,
  ecto_repos: [Sequin.Repo],
  env: Mix.env(),
  generators: [timestamp_type: :utc_datetime],
  self_hosted: true

# Configure tailwind (the version is required)
config :tailwind,
  version: "3.4.0",
  sequin: [
    args: ~w(
      --config=tailwind.config.js
      --input=css/app.css
      --output=../priv/static/assets/app.css
    ),

    # Import environment specific config. This must remain at the bottom
    # of this file so it overrides the configuration defined above.
    cd: Path.expand("../assets", __DIR__)
  ]

import_config "#{config_env()}.exs"
