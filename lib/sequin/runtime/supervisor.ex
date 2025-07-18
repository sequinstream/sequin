defmodule Sequin.Runtime.Supervisor do
  @moduledoc """
  Supervisor for managing database-related runtime processes.
  """
  use Supervisor

  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.SlotSupervisor
  alias Sequin.Runtime.SlotSupervisorSupervisor
  alias Sequin.Runtime.TableReaderServer
  alias Sequin.Runtime.TableReaderServerSupervisor
  alias Sequin.Runtime.WalEventSupervisor
  alias Sequin.Runtime.WalPipelineServer

  require Logger

  defp table_reader_supervisor, do: {:via, :syn, {:replication, TableReaderServerSupervisor}}
  defp slot_supervisor, do: {:via, :syn, {:replication, SlotSupervisorSupervisor}}
  defp wal_event_supervisor, do: {:via, :syn, {:replication, WalEventSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:replication, __MODULE__}})
    Supervisor.start_link(__MODULE__, nil, name: name)
  end

  @impl Supervisor
  def init(_opts) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  @spec start_for_sink_consumer(%SinkConsumer{}, Keyword.t()) :: :ok | {:error, term()}
  def start_for_sink_consumer(%SinkConsumer{} = consumer, opts \\ []) do
    sink_consumer = Repo.preload(consumer, :replication_slot)

    with {:ok, _} <- start_replication(slot_supervisor(), sink_consumer.replication_slot, opts) do
      SlotSupervisor.start_stores_and_pipeline!(consumer, opts)
    end
  end

  def restart_for_sink_consumer(%SinkConsumer{} = consumer) do
    SlotSupervisor.restart_stores_and_pipeline(consumer)
  end

  def stop_for_sink_consumer(%SinkConsumer{} = consumer) do
    stop_for_sink_consumer(consumer.replication_slot_id, consumer.id)
  end

  def stop_for_sink_consumer(replication_slot_id, id) do
    SlotSupervisor.stop_stores_and_pipeline(replication_slot_id, id)
  end

  def maybe_start_table_readers(supervisor \\ table_reader_supervisor(), %SinkConsumer{} = consumer, opts \\ []) do
    consumer = Repo.preload(consumer, :active_backfills)

    Enum.each(consumer.active_backfills, fn backfill -> start_table_reader(supervisor, backfill, opts) end)
  end

  def start_table_reader(supervisor \\ table_reader_supervisor(), %Backfill{} = backfill, opts \\ []) do
    if backfill.state == :active do
      default_opts = [
        backfill_id: backfill.id,
        table_oid: backfill.table_oid,
        max_timeout_ms: backfill.max_timeout_ms
      ]

      opts = Keyword.merge(default_opts, opts)

      Sequin.DynamicSupervisor.start_child(supervisor, {TableReaderServer, opts})
    end
  end

  def stop_table_reader(supervisor \\ table_reader_supervisor(), backfill_or_id)

  def stop_table_reader(supervisor, %Backfill{id: backfill_id}) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableReaderServer.via_tuple(backfill_id))
    :ok
  end

  def stop_table_reader(supervisor, backfill_id) when is_binary(backfill_id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableReaderServer.via_tuple(backfill_id))
    :ok
  end

  def restart_table_reader(supervisor \\ table_reader_supervisor(), backfill_or_id, opts \\ []) do
    stop_table_reader(supervisor, backfill_or_id)
    start_table_reader(supervisor, backfill_or_id, opts)
  end

  def start_replication(supervisor \\ slot_supervisor(), pg_replication, opts \\ [])

  def start_replication(_supervisor, %PostgresReplicationSlot{status: :disabled} = pg_replication, _opts) do
    Logger.info("PostgresReplicationSlot #{pg_replication.id} is disabled, skipping start")
    {:error, :disabled}
  end

  def start_replication(supervisor, %PostgresReplicationSlot{} = pg_replication, opts) do
    opts = Keyword.put(opts, :pg_replication_id, pg_replication.id)
    test_pid = Keyword.get(opts, :test_pid)

    opts =
      Keyword.update(opts, :slot_message_store_opts, [test_pid: test_pid], fn opts ->
        Keyword.put(opts, :test_pid, test_pid)
      end)

    case Sequin.DynamicSupervisor.maybe_start_child(supervisor, {SlotSupervisor, opts}) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, error} ->
        Logger.error("[Runtime.Supervisor] Failed to start SlotSupervisor: #{inspect(error)}", error: error)
        {:error, error}
    end
  end

  @spec refresh_message_handler_ctx(id :: String.t()) :: :ok | {:error, term()}
  def refresh_message_handler_ctx(id) do
    case Sequin.Replication.get_pg_replication(id) do
      {:ok, pg_replication} ->
        # Remove races by locking - better way?
        :global.trans(
          {__MODULE__, id},
          fn ->
            new_ctx = MessageHandler.context(pg_replication)

            case SlotProcessorServer.update_message_handler_ctx(id, new_ctx) do
              :ok -> :ok
              {:error, :not_running} -> :ok
            end
          end,
          [node() | Node.list()],
          # This is retries, not timeout
          20
        )

      error ->
        error
    end
  end

  def stop_replication(supervisor \\ slot_supervisor(), pg_replication_or_id)

  def stop_replication(supervisor, %PostgresReplicationSlot{id: id}) do
    stop_replication(supervisor, id)
  end

  def stop_replication(supervisor, id) do
    Logger.info("[Runtime.Supervisor] Stopping replication #{id}")
    SlotSupervisor.stop_slot_processor(id)
    Sequin.DynamicSupervisor.stop_child(supervisor, SlotSupervisor.via_tuple(id))
  end

  def restart_replication(supervisor \\ slot_supervisor(), %PostgresReplicationSlot{} = pg_replication) do
    stop_replication(supervisor, pg_replication)
    start_replication(supervisor, pg_replication)
  end

  def start_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.filter(fn wp -> wp.status == :active end)
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, pipelines} ->
      start_wal_pipeline_server(supervisor, replication_slot_id, destination_oid, destination_database_id, pipelines)
    end)
  end

  def start_wal_pipeline_server(
        supervisor \\ wal_event_supervisor(),
        replication_slot_id,
        destination_oid,
        destination_database_id,
        pipelines
      ) do
    opts = [
      replication_slot_id: replication_slot_id,
      destination_oid: destination_oid,
      destination_database_id: destination_database_id,
      wal_pipeline_ids: Enum.map(pipelines, & &1.id)
    ]

    Sequin.DynamicSupervisor.start_child(
      supervisor,
      {WalPipelineServer, opts}
    )
  end

  def stop_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, _} ->
      stop_wal_pipeline_server(supervisor, replication_slot_id, destination_oid, destination_database_id)
    end)
  end

  def stop_wal_pipeline_server(
        supervisor \\ wal_event_supervisor(),
        replication_slot_id,
        destination_oid,
        destination_database_id
      ) do
    Logger.info("Stopping WalPipelineServer",
      replication_slot_id: replication_slot_id,
      destination_oid: destination_oid,
      destination_database_id: destination_database_id
    )

    Sequin.DynamicSupervisor.stop_child(
      supervisor,
      WalPipelineServer.via_tuple({replication_slot_id, destination_oid, destination_database_id})
    )
  end

  def restart_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    stop_wal_pipeline_servers(supervisor, pg_replication)
    start_wal_pipeline_servers(supervisor, pg_replication)
  end

  defp children do
    [
      Sequin.Runtime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: table_reader_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: slot_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: wal_event_supervisor())
    ]
  end
end
