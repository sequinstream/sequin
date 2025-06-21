defmodule Sequin.MixProject do
  use Mix.Project

  def project do
    [
      app: :sequin,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Sequin.Application, []},
      extra_applications: [:logger, :runtime_tools] ++ extra_applications(Mix.env())
    ]
  end

  defp extra_applications(:dev), do: [:wx, :observer]
  defp extra_applications(_), do: []

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "test/support", "bench"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      # Phoenix and Web Framework
      {:phoenix, "~> 1.7.12"},
      {:phoenix_ecto, "~> 4.4"},
      {:phoenix_html, "~> 4.0"},
      {:phoenix_live_view, "~> 0.20.2"},
      {:phoenix_live_dashboard, "~> 0.8.3"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:live_svelte, "~> 0.13.3"},
      {:bandit, "~> 1.2"},
      {:tailwind, "~> 0.2", runtime: Mix.env() == :dev},
      {:esbuild, "~> 0.8", runtime: Mix.env() == :dev},

      # Database and Ecto
      {:ecto_sql, "~> 3.10"},
      {:postgrex, ">= 0.0.0"},
      {:polymorphic_embed, "~> 4.1.1"},
      {:typed_ecto_schema, "~> 0.4.1"},
      {:cloak_ecto, "~> 1.3.0"},
      {:pgvector, "~> 0.3.0"},
      {:yesql, "~> 1.0"},

      # Authentication and Security
      {:argon2_elixir, "~> 3.0"},
      {:assent, "~> 0.2.9"},
      {:jose, "~> 1.11"},

      # AWS and Cloud Services
      {:aws, "~> 1.0"},
      {:aws_rds_castore, "~> 1.2.0"},
      {:aws_signature, "~> 0.3.2"},

      # Monitoring and Observability
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      {:sentry, "~> 10.2"},
      {:recon, "~> 2.5.6"},
      {:observer_cli, "~> 1.7"},
      {:prometheus_ex, "~> 3.0"},
      {:prometheus_plugs, "~> 1.1"},
      {:dogstatsd, "~> 1.0", hex: :dogstatsde},

      # Logging and JSON
      {:logger_json, "~> 6.0"},
      {:jason, "~> 1.2"},

      # Internationalization
      {:gettext, "~> 0.20"},

      # HTTP and API Clients
      {:finch, "~> 0.13"},
      {:req, "~> 0.5"},
      {:swoosh, "~> 1.5"},

      # Messaging / PubSub / Queues
      {:gnat, "~> 1.9"},
      {:amqp, "~> 4.0"},
      {:brod, "~> 4.3"},

      # Caching and State Management
      {:con_cache, "~> 1.1"},
      {:syn, "~> 3.3"},
      {:gen_state_machine, "~> 3.0"},

      # Redis Clients
      {:eredis_cluster, github: "Nordix/eredis_cluster", override: true},
      {:eredis, github: "acco/eredis", override: true},

      # Clustering and Distribution
      {:libcluster, "~> 3.3"},
      {:dns_cluster, "~> 0.1.1"},

      # Data Processing and Types
      {:flow, "~> 1.2"},
      {:typed_struct, "~> 0.3.0"},
      {:yaml_elixir, "~> 2.11"},
      {:ymlr, "~> 5.0"},
      {:broadway, "~> 1.0"},
      {:broadway_dashboard, "~> 0.4.0"},
      {:broadway_sqs, github: "dashbitco/broadway_sqs", ref: "94ccc7e"},

      # Background Jobs
      {:oban, "~> 2.19"},

      # Metaprogramming and Enhancements
      {:decorator, "~> 1.4"},

      # Development and Testing
      {:styler, "~> 1.4.0", only: [:dev, :test], runtime: false},
      {:faker, "~> 0.18.0", only: [:dev, :test]},
      {:mix_test_interactive, "~> 2.0", only: :dev, runtime: false},
      {:mox, "~> 1.0", runtime: false},
      {:hammox, "~> 0.7", only: :test},
      {:benchee, "~> 1.0", only: :dev},
      {:rexbug, "~> 1.0"},
      {:floki, ">= 0.30.0", only: :test},
      {:uuid, "~> 1.1"},
      {:tidewave, "~> 0.1", only: :dev},
      # Need in :dev for formatter
      {:assert_eventually, "~> 1.0", only: [:dev, :test]},

      # MiniElixir extras
      {:eden, "~> 2.1.0"}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup", "cmd npm install --prefix assets"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": [
        "ecto.drop",
        "ecto.setup",
        &remove_consumer_messages_log/1
      ],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"],
      "assets.setup": ["cmd --cd assets npm install"],
      "assets.build": ["tailwind sequin", "esbuild sequin"],
      "assets.deploy": [
        "tailwind sequin --minify",
        "cmd --cd assets node build.js --deploy",
        "phx.digest"
      ]
    ]
  end

  defp remove_consumer_messages_log(_) do
    Sequin.Logs.trim_log_file()
  end
end
