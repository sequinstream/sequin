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
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:argon2_elixir, "~> 3.0"},
      {:assent, "~> 0.2.9"},
      {:aws, "~> 1.0"},
      {:aws_rds_castore, "~> 1.2.0"},
      {:phoenix, "~> 1.7.12"},
      {:phoenix_ecto, "~> 4.4"},
      {:polymorphic_embed, "~> 4.1.1"},
      {:ecto_sql, "~> 3.10"},
      {:postgrex, ">= 0.0.0"},
      {:phoenix_html, "~> 4.0"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:phoenix_live_view, "~> 0.20.2"},
      {:live_svelte, "~> 0.13.3"},
      {:floki, ">= 0.30.0", only: :test},
      {:phoenix_live_dashboard, "~> 0.8.3"},
      {:tailwind, "~> 0.2", runtime: Mix.env() == :dev},
      {:styler, "~> 1.0.0-rc.1", only: [:dev, :test], runtime: false},
      {:logger_json, "~> 6.0"},
      {:swoosh, "~> 1.5"},
      {:finch, "~> 0.13"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      {:gettext, "~> 0.20"},
      {:jason, "~> 1.2"},
      {:dns_cluster, "~> 0.1.1"},
      {:bandit, "~> 1.2"},
      {:typed_ecto_schema, "~> 0.4.1"},
      {:typed_struct, "~> 0.3.0"},
      {:yesql, "~> 1.0"},
      {:faker, "~> 0.18.0", only: [:dev, :test]},
      {:uuid, "~> 1.1"},
      {:mix_test_interactive, "~> 2.0", only: :dev, runtime: false},
      {:uxid, "~> 0.2"},
      {:dogstatsd, "~> 1.0", hex: :dogstatsde},
      {:flow, "~> 1.2"},
      {:con_cache, "~> 1.1"},
      {:cloak_ecto, "~> 1.3.0"},
      {:gen_state_machine, "~> 3.0"},
      {:mox, "~> 1.0", runtime: false},
      {:oban, "~> 2.17"},
      {:hammox, "~> 0.7", only: :test},
      {:broadway, "~> 1.0"},
      {:req, "~> 0.5"},
      {:redix, "~> 1.0"},
      {:gnat, "~> 1.9"},
      {:sentry, "~> 10.0"},
      {:observer_cli, "~> 1.7"},
      {:esbuild, "~> 0.8", runtime: Mix.env() == :dev},
      {:yaml_elixir, "~> 2.11"},
      {:ymlr, "~> 5.0"},
      {:brod, "~> 4.3"},
      {:jose, "~> 1.11"},
      {:syn, "~> 3.3"},
      {:libcluster, "~> 3.3"},
      {:amqp, "~> 4.0"},
      {:recon, "~> 2.5.6"},
      {:aws_signature, "~> 0.3.2"}
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
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
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
