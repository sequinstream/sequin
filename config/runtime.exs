import Config

alias LoggerJSON.Formatters.Datadog
alias Sequin.ConfigParser

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

maybe_parse_int_env = fn key ->
  try do
    if System.get_env(key) do
      String.to_integer(System.get_env(key))
    end
  rescue
    e in ArgumentError ->
      IO.puts("Environment variable #{key} must be a valid integer: #{inspect(e)}")
      reraise(e, __STACKTRACE__)
  end
end

server_port = String.to_integer(System.get_env("SERVER_PORT") || System.get_env("PORT") || "7376")
metrics_port = String.to_integer(System.get_env("SEQUIN_METRICS_PORT") || "8376")
server_host = System.get_env("SERVER_HOST") || System.get_env("PHX_HOST") || "localhost"
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

# ## Using releases
#
# If you use `mix release`, you need to explicitly enable the server
# by passing the PHX_SERVER=true when you start it:
#
#     PHX_SERVER=true bin/sequin start
#
# Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
# script that automatically sets the env var above.
if System.get_env("PHX_SERVER") do
  config :sequin, SequinWeb.Endpoint, server: true
  config :sequin, SequinWeb.MetricsEndpoint, server: true
end

# Deprecate ECTO_IPV6 in favor of PG_IPV6
ecto_socket_opts = if (System.get_env("ECTO_IPV6") || System.get_env("PG_IPV6")) in ~w(true 1), do: [:inet6], else: []

if config_env() == :prod do
  config :logger, default_handler: [formatter: {Datadog, metadata: :all}]
end

if config_env() == :prod and self_hosted do
  account_self_signup =
    if System.get_env("FEATURE_ACCOUNT_SELF_SIGNUP", "enabled") in enabled_feature_values, do: :enabled, else: :disabled

  provision_default_user =
    if System.get_env("FEATURE_PROVISION_DEFAULT_USER", "enabled") in enabled_feature_values,
      do: :enabled,
      else: :disabled

  backfill_max_pending_messages = maybe_parse_int_env.("BACKFILL_MAX_PENDING_MESSAGES")

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

  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

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

  case System.get_env("SEQUIN_LOG_FORMAT") do
    "DATADOG_JSON" ->
      config :logger, default_handler: [formatter: {Datadog, metadata: :all}]

    _ ->
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
    socket_options: ecto_socket_opts

  config :sequin, SequinWeb.Endpoint,
    # `url` is used for configuring links in the console. So it corresponds to the *external*
    # host and port of the application
    # TODO: Default to 443
    url: [host: server_host, port: server_port, scheme: "https"],
    check_origin: check_origin,
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: server_port
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
    api_base_url: "http://#{server_host}:#{server_port}",
    config_file_path: System.get_env("CONFIG_FILE_PATH"),
    config_file_yaml: System.get_env("CONFIG_FILE_YAML"),
    release_version: System.get_env("RELEASE_VERSION"),
    backfill_max_pending_messages: backfill_max_pending_messages,
    max_memory_bytes: ConfigParser.max_memory_bytes(env_vars),
    max_storage_bytes: ConfigParser.max_storage_bytes(env_vars)
end

if config_env() == :prod and not self_hosted do
  database_url = System.fetch_env!("PG_URL")

  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  function_transforms =
    if System.get_env("FEATURE_FUNCTION_TRANSFORMS", "disabled") in enabled_feature_values, do: :enabled, else: :disabled

  config :sequin, Sequin.Pagerduty, integration_key: System.fetch_env!("PAGERDUTY_INTEGRATION_KEY")

  config :sequin, Sequin.Posthog,
    req_opts: [base_url: "https://us.i.posthog.com"],
    api_key: "phc_TZn6p4BG38FxUXrH8IvmG39TEHvqdO2kXGoqrSwN8IY",
    frontend_api_key: "phc_TZn6p4BG38FxUXrH8IvmG39TEHvqdO2kXGoqrSwN8IY"

  config :sequin, Sequin.Repo,
    ssl: AwsRdsCAStore.ssl_opts(database_url),
    pool_size: String.to_integer(System.get_env("PG_POOL_SIZE", "100")),
    socket_options: ecto_socket_opts,
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
    url: [host: server_host, port: 443, scheme: "https"],
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: server_port
    ],
    secret_key_base: secret_key_base

  config :sequin, SequinWeb.MetricsEndpoint,
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: 8376
    ]

  config :sequin, :features,
    account_self_signup: :enabled,
    function_transforms: function_transforms

  config :sequin, :koala, public_key: "pk_ec2e6140b3d56f5eb1735350eb20e92b8002"

  config :sequin,
    api_base_url: "https://#{System.fetch_env!("API_HOST")}",
    # Arbitrarily high memory limit in prod of 100GB
    max_memory_bytes: 100 * 1024 * 1024 * 1024
end

# Set the default workers per sink setting from environment variable if available
default_workers_per_sink = ConfigParser.default_workers_per_sink(env_vars)
config :sequin, Sequin.Runtime.SinkPipeline, default_workers_per_sink: default_workers_per_sink

if config_env() == :prod do
  vault_key = System.get_env("VAULT_KEY") || raise("VAULT_KEY is not set")

  datadog_api_key = get_env.("DATADOG_API_KEY")
  datadog_app_key = get_env.("DATADOG_APP_KEY")

  config :libcluster,
    topologies: [
      sequin: [
        strategy: Sequin.Libcluster.PostgresStrategy,
        config: [
          channel_name: "sequin_cluster"
        ]
      ]
    ]

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

  config :sequin, :retool_workflow_key, System.get_env("RETOOL_WORKFLOW_KEY")

  config :sequin,
    datadog: [
      configured: is_binary(datadog_api_key) and is_binary(datadog_app_key),
      api_key: datadog_api_key,
      app_key: datadog_app_key,
      default_query: "service:sequin"
    ]

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :sequin, SequinWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :sequin, SequinWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.

  # ## Configuring the mailer
  #
  # In production you need to configure the mailer to use a different adapter.
  # Also, you may need to configure the Swoosh API client of your choice if you
  # are not using SMTP. Here is an example of the configuration:
  #
  #     config :sequin, Sequin.Mailer,
  #       adapter: Swoosh.Adapters.Mailgun,
  #       api_key: System.get_env("MAILGUN_API_KEY"),
  #       domain: System.get_env("MAILGUN_DOMAIN")
  #
  # For this example you need include a HTTP client required by Swoosh API client.
  # Swoosh supports Hackney and Finch out of the box:
  #
  #     config :swoosh, :api_client, Swoosh.ApiClient.Hackney
  #
  # See https://hexdocs.pm/swoosh/Swoosh.html#module-installation for details.
end
