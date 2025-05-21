defmodule Sequin.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  use Application

  alias Sequin.Accounts
  alias Sequin.CheckSystemHealthWorker
  alias Sequin.Functions.TestMessages
  alias Sequin.Health.KickoffCheckPostgresReplicationSlotWorker
  alias Sequin.Health.KickoffCheckSinkConfigurationWorker
  alias Sequin.MutexedSupervisor

  require Logger

  @impl true
  def start(_type, _args) do
    env = Application.get_env(:sequin, :env)
    children = children(env)

    Sequin.Prometheus.setup()
    Sequin.PrometheusExporter.setup()

    :ets.new(Sequin.Runtime.SlotProcessorServer.ets_table(), [:set, :public, :named_table])
    # Add this line to create the new ETS table for health debouncing
    :ets.new(Sequin.Health.debounce_ets_table(), [:set, :public, :named_table])

    :ets.new(Sequin.Consumers.posthog_ets_table(), [:set, :public, :named_table])

    :syn.add_node_to_scopes([:account, :replication, :consumers])

    TestMessages.create_ets_table()
    Accounts.initialize_account_features_cache()

    Sequin.Sentry.init()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Sequin.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp children(:test) do
    base_children()
  end

  defp children(_) do
    base_children() ++
      [
        SequinWeb.Telemetry,
        MutexedSupervisor.child_spec(
          Sequin.Runtime.MutexedSupervisor,
          [
            Sequin.Runtime.Supervisor
          ]
        ),
        # Sequin.Tracer.Starter,
        Sequin.Telemetry.PosthogReporter
      ]
  end

  defp base_children do
    topologies = Application.get_env(:libcluster, :topologies)
    Sequin.Redis.connect_cluster()

    [
      Sequin.Repo,
      Sequin.Vault,
      Sequin.PubSub.child_spec(),
      # Start the Finch HTTP client for sending emails
      Sequin.Finch.child_spec(),
      Sequin.TaskSupervisor.child_spec(),
      Sequin.Cache.child_spec(),
      {Oban, Application.fetch_env!(:sequin, Oban)},
      {Registry, keys: :duplicate, name: TestMessages.registry()},
      Sequin.Functions.MiniElixir,
      Sequin.Databases.ConnectionCache,
      Sequin.Sinks.Redis.ConnectionCache,
      Sequin.Sinks.Kafka.ConnectionCache,
      Sequin.Sinks.Nats.ConnectionCache,
      Sequin.Sinks.RabbitMq.ConnectionCache,
      SequinWeb.Presence,
      Sequin.SystemMetricsServer,
      # Sequin.Tracer.DynamicSupervisor,
      {Cluster.Supervisor, [topologies]},
      {Task, fn -> enqueue_workers() end},
      # Start to serve requests, typically the last entry
      SequinWeb.Endpoint,
      SequinWeb.MetricsEndpoint
    ]
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    SequinWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp enqueue_workers do
    CheckSystemHealthWorker.enqueue()
    # Run these right away, in case this is the first time the app is started in a while
    KickoffCheckPostgresReplicationSlotWorker.enqueue()
    KickoffCheckSinkConfigurationWorker.enqueue()
  end
end
