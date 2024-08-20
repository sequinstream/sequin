defmodule Sequin.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  use Application

  alias Sequin.Databases.ConnectionCache

  @impl true
  def start(_type, _args) do
    env = Application.get_env(:sequin, :env)
    children = children(env)

    :ets.new(Sequin.Extensions.Replication.ets_table(), [:set, :public, :named_table])

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Sequin.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp children(:test) do
    base_children()
  end

  defp children(_) do
    base_children() ++ [Sequin.ReplicationRuntime.Supervisor, Sequin.ConsumersRuntime.Supervisor]
  end

  defp base_children do
    [
      Sequin.Registry,
      SequinWeb.Telemetry,
      Sequin.Repo,
      Sequin.Vault,
      {DNSCluster, query: Application.get_env(:sequin, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Sequin.PubSub},
      # Start the Finch HTTP client for sending emails
      {Finch, name: Sequin.Finch},
      {Task.Supervisor, name: Sequin.TaskSupervisor},
      {ConCache, name: Sequin.Cache, ttl_check_interval: :timer.seconds(1), global_ttl: :infinity},
      {Oban, Application.fetch_env!(:sequin, Oban)},
      ConnectionCache,
      SequinWeb.Presence,
      # Start to serve requests, typically the last entry
      SequinWeb.Endpoint
    ]
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    SequinWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
