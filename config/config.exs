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

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :phoenix, :json_library, Jason

config :redix, start_opts: {"redis://localhost:6379", [name: :redix]}

config :redix_cluster,
  cluster_nodes: [
    %{host: "localhost", port: 7001},
    %{host: "localhost", port: 7002},
    %{host: "localhost", port: 7003},
    %{host: "localhost", port: 7004},
    %{host: "localhost", port: 7005},
    %{host: "localhost", port: 7006}
  ]

# Configures Elixir's Logger

# Use Jason for JSON parsing in Phoenix
config :sentry,
  enable_source_code_context: true,
  environment_name: Mix.env(),
  root_source_code_paths: [File.cwd!()],
  client: Sequin.Sentry.FinchClient

config :sequin, Oban,
  prefix: sequin_config_schema,
  queues: [
    default: 10,
    health_checks: 20
  ],
  repo: Sequin.Repo,
  plugins: [
    {Oban.Plugins.Cron,
     crontab: [
       {"0 */6 * * *", Sequin.Databases.EnqueueDatabaseUpdateWorker},
       {"0 * * * *", Sequin.Logs.RotateLogsWorker},
       {"*/10 * * * *", Sequin.HealthRuntime.HttpEndpointHealthWorker},
       {"* * * * *", Sequin.CheckSystemHealthWorker}
     ]}
  ]

config :sequin, Sequin.Mailer, adapter: Swoosh.Adapters.Local

# Configures the mailer
#
# By default it uses the "Local" adapter which stores the emails
# locally. You can see the emails in your browser, at "/dev/mailbox".
#
# For production it's recommended to configure a different adapter
# at the `config/runtime.exs`.
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
  migration_lock: :pg_advisory_lock,
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

config :sequin, SequinWeb.UserSessionController,
  github: [
    redirect_uri: "http://localhost:4000/auth/github/callback"
  ]

config :sequin,
  ecto_repos: [Sequin.Repo],
  env: Mix.env(),
  generators: [timestamp_type: :utc_datetime],
  self_hosted: true,
  retool_workflow_key: "dummy_retool_workflow_key",
  portal_hostname: "localhost",
  datadog_req_opts: [],
  datadog: [configured: false],
  api_base_url: "http://localhost:4000"

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
