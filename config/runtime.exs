import Config

alias LoggerJSON.Formatters.Datadog
alias Sequin.ConfigParser
alias Sequin.Logger.Redactor

require Logger

self_hosted = Application.compile_env(:sequin, :self_hosted)

env_vars = System.get_env()

enabled_feature_values = ~w(true 1 enabled ENABLED)

get_env = fn key ->
  if self_hosted do
    System.get_env(key)
  else
    System.fetch_env!(key)
  end
end

metrics_port = String.to_integer(System.get_env("SEQUIN_METRICS_PORT") || "8376")
metrics_host = System.get_env("METRICS_HOST") || "localhost"

metrics_auth =
  case {System.get_env("SEQUIN_METRICS_USER"), System.get_env("SEQUIN_METRICS_PASSWORD")} do
    {nil, nil} ->
      nil

    {u, p} ->
      [
        username: u || "",
        password: p || "",
        realm: "Sequin Metrics"
      ]
  end

config :sequin, Sequin.Benchmark.Stats, track_messages: System.get_env("BENCHMARK_TRACK_MESSAGES") == "true"
config :sequin, :metrics_basic_auth, metrics_auth

if System.get_env("PORT") do
  Logger.warning("PORT environment variable is deprecated. Please use SERVER_PORT instead.")
end

if System.get_env("PHX_HOST") do
  Logger.warning("PHX_HOST environment variable is deprecated. Please use SERVER_HOST instead.")
end

if config_env() == :test do
  config :logger, level: ConfigParser.log_level(env_vars, :warning)
else
  config :logger, level: ConfigParser.log_level(env_vars, :info)
end

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

# Configure SQS integration for HTTP Push sinks
sqs_config =
  if System.get_env("HTTP_PUSH_VIA_SQS_QUEUE_URL") do
    base_config = %{
      main_queue_url: System.fetch_env!("HTTP_PUSH_VIA_SQS_QUEUE_URL"),
      dlq_url: System.fetch_env!("HTTP_PUSH_VIA_SQS_DLQ_URL"),
      region: System.fetch_env!("HTTP_PUSH_VIA_SQS_REGION")
    }

    # Support both explicit credentials and task role
    if System.get_env("HTTP_PUSH_VIA_SQS_USE_TASK_ROLE") == "true" do
      Map.put(base_config, :use_task_role, true)
    else
      Map.merge(base_config, %{
        access_key_id: System.fetch_env!("HTTP_PUSH_VIA_SQS_ACCESS_KEY_ID"),
        secret_access_key: System.fetch_env!("HTTP_PUSH_VIA_SQS_SECRET_ACCESS_KEY")
      })
    end
  end

# Enable via_sqs_for_new_sinks? flag for HttpPushSink
config :sequin, Sequin.Consumers.HttpPushSink,
  via_sqs_for_new_sinks?: System.get_env("HTTP_PUSH_VIA_SQS_NEW_SINKS") in ~w(true 1)

# Configure the SQS pipeline with credentials
config :sequin, Sequin.Runtime.HttpPushSqsPipeline,
  sqs: sqs_config,
  discards_disabled?: System.get_env("HTTP_PUSH_VIA_SQS_DISCARDS_DISABLED") in ~w(true 1)

config :sequin, Sequin.Runtime.SlotProcessorServer,
  max_accumulated_bytes: ConfigParser.replication_flush_max_accumulated_bytes(env_vars),
  max_accumulated_messages: ConfigParser.replication_flush_max_accumulated_messages(env_vars),
  # ## Using releases
  #
  # If you use `mix release`, you need to explicitly enable the server
  # by passing the PHX_SERVER=true when you start it:
  #
  #     PHX_SERVER=true bin/sequin start
  #
  # Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
  # script that automatically sets the env var above.
  max_accumulated_messages_time_ms: ConfigParser.replication_flush_max_accumulated_time_ms(env_vars)

if System.get_env("PHX_SERVER") do
  config :sequin, SequinWeb.Endpoint, server: true
  config :sequin, SequinWeb.MetricsEndpoint, server: true
end

if config_env() == :prod and self_hosted do
  account_self_signup =
    if System.get_env("FEATURE_ACCOUNT_SELF_SIGNUP", "enabled") in enabled_feature_values, do: :enabled, else: :disabled

  provision_default_user =
    if System.get_env("FEATURE_PROVISION_DEFAULT_USER", "enabled") in enabled_feature_values,
      do: :enabled,
      else: :disabled

  backfill_max_pending_messages = ConfigParser.backfill_max_pending_messages(env_vars)

  database_url =
    case System.get_env("PG_URL") do
      nil ->
        hostname = System.get_env("PG_HOSTNAME")
        database = System.get_env("PG_DATABASE")
        port = System.get_env("PG_PORT")
        username = System.get_env("PG_USERNAME")
        password = System.get_env("PG_PASSWORD")

        if Enum.all?([hostname, database, port, username, password], &(not is_nil(&1))) do
          "postgres://#{username}:#{password}@#{hostname}:#{port}/#{database}"
        else
          raise """
          Missing PostgreSQL connection information.
          Please provide either PG_URL or all of the following environment variables:
          PG_HOSTNAME, PG_DATABASE, PG_PORT, PG_USERNAME, PG_PASSWORD
          """
        end

      url ->
        url
    end

  secret_key_base = ConfigParser.secret_key_base(env_vars)

  repo_ssl =
    case System.get_env("PG_SSL") do
      "true" -> [verify: :verify_none]
      "1" -> [verify: :verify_none]
      "verify-none" -> [verify: :verify_none]
      _ -> false
    end

  check_origin =
    case System.get_env("SERVER_CHECK_ORIGIN", "false") do
      "true" -> true
      "1" -> true
      "false" -> false
      "0" -> false
      other -> raise("Invalid SERVER_CHECK_ORIGIN: #{other}, must be true or false or 1 or 0")
    end

  if System.get_env("SEQUIN_LOG_FORMAT") in ~w(DATADOG_JSON datadog_json) do
    config :logger,
      default_handler: [
        formatter: {Datadog, metadata: :all, redactors: [{Redactor, []}]}
      ]
  else
    # Fallback to ConsoleLogger, set in prod.exs
    :ok
  end

  config :sequin, Sequin.Posthog,
    req_opts: [base_url: "https://us.i.posthog.com"],
    api_key: "phc_i9k28nZwjjJG9DzUK0gDGASxXtGNusdI1zdaz9cuA7h",
    frontend_api_key: "phc_i9k28nZwjjJG9DzUK0gDGASxXtGNusdI1zdaz9cuA7h",
    is_disabled: System.get_env("SEQUIN_TELEMETRY_DISABLED") in ~w(true 1)

  config :sequin, Sequin.Repo,
    ssl: repo_ssl,
    pool_size: String.to_integer(System.get_env("PG_POOL_SIZE", "10")),
    url: database_url,
    socket_options: ConfigParser.ecto_socket_opts(env_vars)

  config :sequin, SequinWeb.Endpoint,
    # `url` is used for configuring links in the console. So it corresponds to the *external*
    # host and port of the application
    url: [host: ConfigParser.server_host(env_vars), port: 443, scheme: "https"],
    check_origin: check_origin,
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: ConfigParser.server_port(env_vars)
    ],
    secret_key_base: secret_key_base,
    live_view: [
      long_poll_fallback_ms: String.to_integer(System.get_env("LONG_POLL_FALLBACK_MS", "3000"))
    ]

  config :sequin, SequinWeb.MetricsEndpoint,
    url: [host: metrics_host, port: metrics_port, scheme: "https"],
    check_origin: check_origin,
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: metrics_port
    ],
    secret_key_base: secret_key_base,
    live_view: [
      long_poll_fallback_ms: String.to_integer(System.get_env("LONG_POLL_FALLBACK_MS", "3000"))
    ]

  config :sequin, :features,
    account_self_signup: account_self_signup,
    provision_default_user: provision_default_user,
    function_transforms: :enabled

  config :sequin, :koala,
    public_key: "pk_ec2e6140b3d56f5eb1735350eb20e92b8002",
    is_disabled: System.get_env("SEQUIN_TELEMETRY_DISABLED") in ~w(true 1)

  config :sequin,
    api_base_url: "http://#{ConfigParser.server_host(env_vars)}:#{ConfigParser.server_port(env_vars)}",
    release_version: System.get_env("RELEASE_VERSION"),
    backfill_max_pending_messages: backfill_max_pending_messages,
    max_memory_bytes: ConfigParser.max_memory_bytes(env_vars)
end

if config_env() == :prod and not self_hosted do
  database_url = System.fetch_env!("PG_URL")
  secret_key_base = ConfigParser.secret_key_base(env_vars)

  function_transforms =
    if System.get_env("FEATURE_FUNCTION_TRANSFORMS", "disabled") in enabled_feature_values, do: :enabled, else: :disabled

  config :logger,
    default_handler: [
      formatter: {Datadog, metadata: :all, redactors: [{Redactor, []}]}
    ]

  config :sequin, Sequin.Pagerduty, integration_key: System.fetch_env!("PAGERDUTY_INTEGRATION_KEY")

  config :sequin, Sequin.Posthog,
    req_opts: [base_url: "https://us.i.posthog.com"],
    api_key: "phc_TZn6p4BG38FxUXrH8IvmG39TEHvqdO2kXGoqrSwN8IY",
    frontend_api_key: "phc_TZn6p4BG38FxUXrH8IvmG39TEHvqdO2kXGoqrSwN8IY"

  config :sequin, Sequin.Repo,
    ssl: AwsRdsCAStore.ssl_opts(database_url),
    pool_size: String.to_integer(System.get_env("PG_POOL_SIZE", "100")),
    socket_options: ConfigParser.ecto_socket_opts(env_vars),
    url: database_url,
    datadog_req_opts: [
      headers: [
        {"DD-API-KEY", System.fetch_env!("DATADOG_API_KEY")},
        {"DD-APPLICATION-KEY", System.fetch_env!("DATADOG_APP_KEY")}
      ]
    ]

  config :sequin, SequinWeb.Endpoint,
    # `url` is used for configuring links in the console. So it corresponds to the *external*
    # host and port of the application
    url: [host: ConfigParser.server_host(env_vars), port: 443, scheme: "https"],
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: ConfigParser.server_port(env_vars)
    ],
    secret_key_base: secret_key_base

  config :sequin, SequinWeb.MetricsEndpoint,
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: 8376
    ]

  config :sequin, :features,
    account_self_signup: :disabled,
    function_transforms: function_transforms

  config :sequin, :koala, public_key: "pk_ec2e6140b3d56f5eb1735350eb20e92b8002"

  config :sequin,
    api_base_url: "https://#{System.fetch_env!("API_HOST")}",
    # Arbitrarily high memory limit in prod of 100GB
    max_memory_bytes: 100 * 1024 * 1024 * 1024
end

# Set the default workers per sink setting from environment variable if available
default_workers_per_sink = ConfigParser.default_workers_per_sink(env_vars)

http_pool_size =
  if size = System.get_env("HTTP_POOL_SIZE") do
    String.to_integer(size)
  end

config :sequin, Sequin.Finch,
  pool_size: http_pool_size,
  pool_count: String.to_integer(System.get_env("HTTP_POOL_COUNT", "1"))

config :sequin, Sequin.Runtime.SinkPipeline, default_workers_per_sink: default_workers_per_sink

if config_env() == :prod do
  vault_key = ConfigParser.vault_key(env_vars)

  datadog_api_key = get_env.("DATADOG_API_KEY")
  datadog_app_key = get_env.("DATADOG_APP_KEY")

  config :sequin, Sequin.Mailer, adapter: Sequin.Swoosh.Adapters.Loops, api_key: System.get_env("LOOPS_API_KEY")
  config :sequin, Sequin.Redis, ConfigParser.redis_config(env_vars)

  config :sequin, Sequin.Vault,
    ciphers: [
      # In AES.GCM, it is important to specify 12-byte IV length for
      # interoperability with other encryption software. See this GitHub issue
      # for more details: https://github.com/danielberkompas/cloak/issues/93
      #
      # In Cloak 2.0, this will be the default iv length for AES.GCM.
      default: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1", key: Base.decode64!(vault_key), iv_length: 12}
    ]

  config :sequin, SequinWeb.Router,
    admin_user: System.get_env("ADMIN_USER"),
    admin_password: System.get_env("ADMIN_PASSWORD")

  config :sequin, SequinWeb.UserSessionController,
    github: [
      redirect_uri: System.get_env("GITHUB_CLIENT_REDIRECT_URI", "https://console.sequinstream.com/auth/github/callback"),
      client_id: get_env.("GITHUB_CLIENT_ID"),
      client_secret: get_env.("GITHUB_CLIENT_SECRET")
    ]

  config :sequin, :incident_io_api_key, System.get_env("INCIDENT_IO_API_KEY")
  config :sequin, :retool_workflow_key, System.get_env("RETOOL_WORKFLOW_KEY")

  config :sequin,
    datadog: [
      configured: is_binary(datadog_api_key) and is_binary(datadog_app_key),
      api_key: datadog_api_key,
      app_key: datadog_app_key,
      default_query: "service:sequin"
    ]
end
