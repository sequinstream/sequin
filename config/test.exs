import Config

alias Sequin.Postgres.PostgrexTypes
alias Sequin.Runtime.HttpPushSqsPipeline
alias Sequin.Test.UnboxedRepo

# Only in tests, remove the complexity from the password hashing algorithm
config :argon2_elixir, t_cost: 1, m_cost: 8

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true

config :sequin, HttpPushSqsPipeline, req_opts: [plug: {Req.Test, HttpPushSqsPipeline}]
config :sequin, Oban, testing: :manual, prefix: "sequin_config"

config :sequin, Sequin,
  datetime_mod: Sequin.TestSupport.DateTimeMock,
  uuid_mod: Sequin.TestSupport.UUIDMock,
  enum_mod: Sequin.TestSupport.EnumMock,
  process_mod: Sequin.TestSupport.ProcessMock,
  application_mod: Sequin.TestSupport.ApplicationMock

config :sequin, Sequin.Mailer, adapter: Swoosh.Adapters.Test

config :sequin, Sequin.Pagerduty,
  integration_key: "test_integration_key",
  req_opts: [plug: {Req.Test, Sequin.Pagerduty}]

config :sequin, Sequin.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "sequin_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 20,
  port: 5432,
  queue_target: 100,
  queue_interval: 1000,
  ssl: false,
  types: PostgrexTypes

config :sequin, Sequin.Runtime.SlotProducer, batch_flush_interval: 10

config :sequin, Sequin.Runtime.SlotProducer.ReorderBuffer,
  min_demand: 1,
  max_demand: 3,
  retry_flush_batch_interval: 5

config :sequin, Sequin.Sinks.AzureEventHub.Client, req_opts: [plug: {Req.Test, Sequin.Sinks.Azure.EventHub}]

config :sequin, Sequin.Vault,
  ciphers: [
    default: {
      Cloak.Ciphers.AES.GCM,
      tag: "AES.GCM.V1", key: Base.decode64!("ZTu2hzCwKYbeNea+8+Y2p0CBXpWusF6agpsZlQsVVi0="), iv_length: 12
    }
  ]

config :sequin, SequinWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "erV7XavfZn9wH6ZOort1IThbuZRv6q3FrG5JjgQfCqe9dyVLkKmi1yxZ9X2DaUy4",
  server: false

config :sequin, UnboxedRepo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "sequin_test",
  pool_size: 20,
  port: 5432,
  queue_target: 100,
  queue_interval: 1000,
  ssl: false,
  priv: "test/support/unboxed_repo",
  types: PostgrexTypes

config :sequin, :config_module, Sequin.ConfigMock

config :sequin,
  ecto_repos: [Sequin.Repo, UnboxedRepo],
  portal_hostname: "portal.sequin.test",
  datadog_req_opts: [
    plug: {Req.Test, Sequin.Datadog}
  ],
  features: [
    account_self_signup: :enabled,
    function_transforms: :enabled,
    enrichment_functions: :enabled
  ],
  aws_sqs: [
    req_opts: [plug: {Req.Test, Sequin.Aws.HttpClient}]
  ],
  # TODO Enable compress_body once Req fixes this issue
  # https://github.com/wojtekmach/req/pull/496
  gcp_pubsub: [
    req_opts: [plug: {Req.Test, Sequin.Sinks.Gcp.PubSub}, compress_body: false]
  ],
  s2: [
    req_opts: [plug: {Req.Test, Sequin.Sinks.S2.Client}, compress_body: false]
  ],
  meilisearch: [
    req_opts: [plug: {Req.Test, Sequin.Sinks.Meilisearch.Client}, compress_body: false]
  ],
  redis_module: Sequin.Sinks.RedisMock,
  kafka_module: Sequin.Sinks.KafkaMock,
  nats_module: Sequin.Sinks.NatsMock,
  rabbitmq_module: Sequin.Sinks.RabbitMqMock,
  aws_module: Sequin.AwsMock,
  # Arbitrarily high memory limit for testing
  max_memory_bytes: 100 * 1024 * 1024 * 1024,
  slot_message_store: [flush_batch_size: 8],
  jepsen_http_host: System.get_env("JEPSEN_HTTP_HOST", "127.0.0.1"),
  jepsen_http_port: "JEPSEN_HTTP_PORT" |> System.get_env("4040") |> String.to_integer(),
  jepsen_transactions_count: "JEPSEN_TRANSACTIONS_COUNT" |> System.get_env("10") |> String.to_integer(),
  jepsen_transaction_queries_count: "JEPSEN_TRANSACTION_QUERIES_COUNT" |> System.get_env("10") |> String.to_integer()

# Disable swoosh api client as it is only required for production adapters.
config :swoosh, :api_client, false

# In AES.GCM, it is important to specify 12-byte IV length for
# interoperability with other encryption software. See this GitHub
# issue for more details:
# https://github.com/danielberkompas/cloak/issues/93
#
# In Cloak 2.0, this will be the default iv length for AES.GCM.

# We don't run a server during test. If one is required,
# you can enable the server option below.

# In test we don't send emails.

# Print only warnings and errors during test

# Initialize plugs at runtime for faster test compilation
# Enable helpful, but potentially expensive runtime checks
