defmodule Sequin.DatabasesRuntime.Supervisor do
  @moduledoc """
  Supervisor for managing database-related runtime processes.
  """
  use Supervisor

  alias Sequin.DatabasesRuntime.BackfillProducerSupervisor
  alias Sequin.DatabasesRuntime.BackfillServer
  alias Sequin.Repo

  defp backfill_producer_supervisor, do: {:via, :syn, {:replication, BackfillProducerSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:replication, __MODULE__}})
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(opts) do
    Supervisor.init(children(opts), strategy: :one_for_one)
  end

  def start_backfill_producer(supervisor \\ backfill_producer_supervisor(), consumer, opts \\ []) do
    consumer = Repo.preload(consumer, :sequence, replication_slot: :postgres_database)

    default_opts = [
      consumer: consumer,
      table_oid: consumer.sequence.table_oid
    ]

    opts = Keyword.merge(default_opts, opts)

    Sequin.DynamicSupervisor.start_child(supervisor, {BackfillServer, opts})
  end

  def stop_backfill_producer(supervisor \\ backfill_producer_supervisor(), consumer) do
    Sequin.DynamicSupervisor.stop_child(supervisor, BackfillServer.via_tuple(consumer))
    :ok
  end

  def restart_backfill_producer(supervisor \\ backfill_producer_supervisor(), consumer, opts \\ []) do
    stop_backfill_producer(supervisor, consumer)
    start_backfill_producer(supervisor, consumer, opts)
  end

  defp children(opts) do
    backfill_producer_supervisor = Keyword.get(opts, :backfill_producer_supervisor, backfill_producer_supervisor())

    [
      Sequin.DatabasesRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: backfill_producer_supervisor)
    ]
  end
end
