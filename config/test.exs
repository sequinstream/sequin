import Config

alias Sequin.Test.UnboxedRepo

# Only in tests, remove the complexity from the password hashing algorithm
config :argon2_elixir, t_cost: 1, m_cost: 8

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true

config :sequin, Oban, testing: :manual, prefix: "sequin_config"
config :sequin, Sequin.Mailer, adapter: Swoosh.Adapters.Test

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
  ssl: false

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
  priv: "test/support/unboxed_repo"

config :sequin,
  ecto_repos: [Sequin.Repo, UnboxedRepo],
  portal_hostname: "portal.sequin.test",
  datadog_req_opts: [
    plug: {Req.Test, Sequin.Datadog}
  ],
  features: [account_self_signup: :enabled],
  aws_sqs: [
    req_opts: [plug: {Req.Test, Sequin.Aws.HttpClient}]
  ],
  google_pubsub: [
    req_opts: [plug: {Req.Test, Sequin.GCP.PubSub}]
  ],
  redis_module: Sequin.RedisMock,
  kafka_module: Sequin.KafkaMock

# In AES.GCM, it is important to specify 12-byte IV length for
# interoperability with other encryption software. See this GitHub
# issue for more details:
# https://github.com/danielberkompas/cloak/issues/93
#
# In Cloak 2.0, this will be the default iv length for AES.GCM.

# We don't run a server during test. If one is required,
# you can enable the server option below.

# In test we don't send emails.

# Disable swoosh api client as it is only required for production adapters.
config :swoosh, :api_client, false

# Print only warnings and errors during test

# Initialize plugs at runtime for faster test compilation
# Enable helpful, but potentially expensive runtime checks
