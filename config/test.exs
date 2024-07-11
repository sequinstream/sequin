import Config

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
  database: "sequin_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2,
  port: 5432,
  queue_target: 100,
  queue_interval: 1000,
  ssl: false

config :sequin, Sequin.Vault,
  ciphers: [
    default: {
      Cloak.Ciphers.AES.GCM,
      # In AES.GCM, it is important to specify 12-byte IV length for
      # interoperability with other encryption software. See this GitHub
      # issue for more details:
      # https://github.com/danielberkompas/cloak/issues/93
      #
      # In Cloak 2.0, this will be the default iv length for AES.GCM.
      tag: "AES.GCM.V1", key: Base.decode64!("ZTu2hzCwKYbeNea+8+Y2p0CBXpWusF6agpsZlQsVVi0="), iv_length: 12
    }
  ]

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :sequin, SequinWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "erV7XavfZn9wH6ZOort1IThbuZRv6q3FrG5JjgQfCqe9dyVLkKmi1yxZ9X2DaUy4",
  server: false

# In test we don't send emails.

# Disable swoosh api client as it is only required for production adapters.
config :swoosh, :api_client, false

# Print only warnings and errors during test

# Initialize plugs at runtime for faster test compilation
# Enable helpful, but potentially expensive runtime checks
