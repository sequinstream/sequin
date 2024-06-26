defmodule Sequin.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      SequinWeb.Telemetry,
      Sequin.Repo,
      {DNSCluster, query: Application.get_env(:sequin, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Sequin.PubSub},
      # Start the Finch HTTP client for sending emails
      {Finch, name: Sequin.Finch},
      # Start a worker by calling: Sequin.Worker.start_link(arg)
      # {Sequin.Worker, arg},
      # Start to serve requests, typically the last entry
      SequinWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Sequin.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    SequinWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
