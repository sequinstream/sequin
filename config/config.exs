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

# Used by broadway_sqs
config :ex_aws, http_client: ExAws.Request.Req

config :logger, :console,
  format: {Sequin.ConsoleLogger, :format},
  metadata: :all

config :phoenix, :json_library, Jason

config :sentry,
  enable_source_code_context: true,
  environment_name: Mix.env(),
  root_source_code_paths: [File.cwd!()],
  client: Sequin.Sentry.FinchClient,
  integrations: [
    oban: [
      capture_errors: true,
      cron: [enabled: true]
    ]
  ]

config :sequin, Oban,
  prefix: sequin_config_schema,
  queues: [
    default: 32,
    lifecycle: 32,
    kickoff: 8,
    health_checks: 32,
    user_submitted: 8
  ],
  repo: Sequin.Repo,
  plugins: [
    # Prune jobs that are older than 14 days
    {Oban.Plugins.Pruner, max_age: 14 * 24 * 60 * 60, limit: 5_000, interval: 20_000},
    {Oban.Plugins.Cron,
     crontab: [
       # Runs every 6 hours (at minute 0)
       {"0 */6 * * *", Sequin.Databases.EnqueueDatabaseUpdateWorker},
       # Runs at the start of every hour
       {"0 * * * *", Sequin.Logs.RotateLogsWorker},
       # Runs every 10 minutes
       {"*/10 * * * *", Sequin.Health.KickoffCheckHttpEndpointHealthWorker},
       # Runs every minute
       {"* * * * *", Sequin.CheckSystemHealthWorker},
       # Runs every 2 minutes
       {"*/2 * * * *", Sequin.Health.SnapshotHealthWorker},
       # Runs every 1 minute
       {"* * * * *", Sequin.Health.KickoffCheckPostgresReplicationSlotWorker},
       # Runs every 5 minutes
       {"*/5 * * * *", Sequin.Health.KickoffCheckSinkConfigurationWorker},
       # Runs every 5 minutes
       {"*/5 * * * *", Sequin.Runtime.MessageConsistencyCheckWorker}
     ]}
  ]

config :sequin, Sequin.Mailer, adapter: Swoosh.Adapters.Local

config :sequin, Sequin.Redis,
  url: "redis://localhost:6379",
  reconnect_sleep: to_timeout(second: 5),
  pool_size: 10

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

config :sequin, Sequin.Runtime.SlotProducer,
  batch_flush_interval: [
    max_messages: 500,
    max_bytes: 1024 * 1024 * 10,
    max_age: 50
  ]

config :sequin, Sequin.Runtime.SlotProducer.ReorderBuffer,
  min_demand: 500,
  max_demand: 1000,
  retry_flush_batch_interval: 250

# Configures the endpoint
config :sequin, SequinWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: SequinWeb.ErrorHTML, json: SequinWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Sequin.PubSub,
  live_view: [signing_salt: "Sm59ovfq", long_poll_fallback_ms: 3000]

config :sequin, SequinWeb.MetricsEndpoint, adapter: Bandit.PhoenixAdapter

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
  api_base_url: "http://localhost:4000",
  message_handler_module: Sequin.Runtime.SlotMessageHandler

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
