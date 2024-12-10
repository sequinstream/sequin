defmodule Sequin.DatabasesRuntime.Supervisor do
  @moduledoc """
  Supervisor for managing database-related runtime processes.
  """
  use Supervisor

  alias Sequin.DatabasesRuntime.TableProducerServer
  alias Sequin.DatabasesRuntime.TableProducerSupervisor
  alias Sequin.Repo

  defp table_producer_supervisor, do: {:via, :syn, {:replication, TableProducerSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:replication, __MODULE__}})
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(opts) do
    Supervisor.init(children(opts), strategy: :one_for_one)
  end

  def start_table_producer(supervisor \\ table_producer_supervisor(), consumer, opts \\ []) do
    consumer = Repo.preload(consumer, :sequence, replication_slot: :postgres_database)

    default_opts = [
      consumer: consumer,
      table_oid: consumer.sequence.table_oid
    ]

    opts = Keyword.merge(default_opts, opts)

    Sequin.DynamicSupervisor.start_child(supervisor, {TableProducerServer, opts})
  end

  def stop_table_producer(supervisor \\ table_producer_supervisor(), consumer) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableProducerServer.via_tuple(consumer))
    :ok
  end

  def restart_table_producer(supervisor \\ table_producer_supervisor(), consumer, opts \\ []) do
    stop_table_producer(supervisor, consumer)
    start_table_producer(supervisor, consumer, opts)
  end

  defp children(opts) do
    table_producer_supervisor = Keyword.get(opts, :table_producer_supervisor, table_producer_supervisor())

    [
      Sequin.DatabasesRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: table_producer_supervisor)
    ]
  end
end
