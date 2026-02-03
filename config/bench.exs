import Config

config :sequin, Oban, testing: :manual, prefix: "sequin_config"
config :sequin, Sequin.Benchmark.Stats, checksum_sample_rate: 0.1

config :sequin, Sequin.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "sequin_bench",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 50

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

config :sequin,
  env: :bench,
  ecto_repos: [Sequin.Repo]

config :swoosh, :api_client, false
