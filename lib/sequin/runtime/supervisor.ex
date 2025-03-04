defmodule Sequin.Runtime.Supervisor do
  @moduledoc """
  Supervisor for managing database-related runtime processes.
  """
  use Supervisor

  alias Sequin.Consumers
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.AzureEventHubPipeline
  alias Sequin.Runtime.GcpPubsubPipeline
  alias Sequin.Runtime.HttpPushPipeline
  alias Sequin.Runtime.KafkaPipeline
  alias Sequin.Runtime.NatsPipeline
  alias Sequin.Runtime.RabbitMqPipeline
  alias Sequin.Runtime.RedisPipeline
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.MessageHandler
  alias Sequin.Runtime.SlotSupervisor
  alias Sequin.Runtime.SlotSupervisorSupervisor
  alias Sequin.Runtime.SqsPipeline
  alias Sequin.Runtime.TableReaderServer
  alias Sequin.Runtime.TableReaderServerSupervisor
  alias Sequin.Runtime.WalEventSupervisor
  alias Sequin.Runtime.WalPipelineServer

  require Logger

  @sinks_to_pipelines %{
    http_push: HttpPushPipeline,
    sqs: SqsPipeline,
    redis: RedisPipeline,
    kafka: KafkaPipeline,
    gcp_pubsub: GcpPubsubPipeline,
    nats: NatsPipeline,
    rabbitmq: RabbitMqPipeline,
    azure_event_hub: AzureEventHubPipeline
  }

  def sinks_to_pipelines, do: @sinks_to_pipelines

  defp table_reader_supervisor, do: {:via, :syn, {:replication, TableReaderServerSupervisor}}
  defp slot_supervisor, do: {:via, :syn, {:replication, SlotSupervisorSupervisor}}
  defp wal_event_supervisor, do: {:via, :syn, {:replication, WalEventSupervisor}}
  defp consumer_supervisor, do: {:via, :syn, {:consumers, RuntimeSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:replication, __MODULE__}})
    Supervisor.start_link(__MODULE__, nil, name: name)
  end

  @impl Supervisor
  def init(_opts) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_table_reader(supervisor \\ table_reader_supervisor(), %SinkConsumer{} = consumer, opts \\ []) do
    consumer = Repo.preload(consumer, [:active_backfill, :sequence])

    if is_nil(consumer.active_backfill) do
      Logger.warning("Consumer #{consumer.id} has no active backfill, skipping start")
    else
      default_opts = [
        backfill_id: consumer.active_backfill.id,
        table_oid: consumer.sequence.table_oid
      ]

      opts = Keyword.merge(default_opts, opts)

      Sequin.DynamicSupervisor.start_child(supervisor, {TableReaderServer, opts})
    end
  end

  def stop_table_reader(supervisor \\ table_reader_supervisor(), consumer)

  def stop_table_reader(_supervisor, %SinkConsumer{active_backfill: nil}) do
    :ok
  end

  def stop_table_reader(supervisor, %SinkConsumer{active_backfill: %Backfill{id: backfill_id}}) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableReaderServer.via_tuple(backfill_id))
    :ok
  end

  def stop_table_reader(supervisor, %SinkConsumer{} = consumer) do
    consumer
    |> Repo.preload(:active_backfill)
    |> stop_table_reader(supervisor)
  end

  def stop_table_reader(supervisor, backfill_id) when is_binary(backfill_id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableReaderServer.via_tuple(backfill_id))
    :ok
  end

  def restart_table_reader(supervisor \\ table_reader_supervisor(), %SinkConsumer{} = consumer, opts \\ []) do
    stop_table_reader(supervisor, consumer)
    start_table_reader(supervisor, consumer, opts)
  end

  def start_replication(supervisor \\ slot_supervisor(), pg_replication_or_id, opts \\ [])

  def start_replication(_supervisor, %PostgresReplicationSlot{status: :disabled} = pg_replication, _opts) do
    Logger.info("PostgresReplicationSlot #{pg_replication.id} is disabled, skipping start")
    {:error, :disabled}
  end

  def start_replication(supervisor, %PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication = Repo.preload(pg_replication, [:postgres_database, sink_consumers: [:sequence]])
    opts = Keyword.put(opts, :pg_replication, pg_replication)
    test_pid = Keyword.get(opts, :test_pid)

    opts =
      Keyword.update(opts, :slot_message_store_opts, [test_pid: test_pid], fn opts ->
        Keyword.put(opts, :test_pid, test_pid)
      end)

    case Sequin.DynamicSupervisor.start_child(supervisor, {SlotSupervisor, opts}) do
      {:ok, pid} ->
        Logger.info("[Runtime.Supervisor] Started SlotSupervisor", replication_id: pg_replication.id)
        SlotSupervisor.start_children(pg_replication, opts)
        {:ok, pid}

      {:ok, pid, _term} ->
        Logger.info("[Runtime.Supervisor] Started SlotSupervisor", replication_id: pg_replication.id)
        SlotSupervisor.start_children(pg_replication, opts)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        SlotSupervisor.start_children(pg_replication, opts)
        {:ok, pid}

      {:error, error} ->
        Logger.error("[Runtime.Supervisor] Failed to start SlotSupervisor", error: error)
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

            case SlotProcessor.update_message_handler_ctx(id, new_ctx) do
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
    Sequin.DynamicSupervisor.stop_child(supervisor, SlotSupervisor.via_tuple(id))
  end

  def restart_replication(supervisor \\ slot_supervisor(), pg_replication_or_id) do
    stop_replication(supervisor, pg_replication_or_id)
    start_replication(supervisor, pg_replication_or_id)
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

  def start_for_sink_consumer(supervisor \\ consumer_supervisor(), consumer_or_id, opts \\ [])

  def start_for_sink_consumer(supervisor, %SinkConsumer{} = consumer, opts) do
    consumer = Repo.preload(consumer, [:sequence, :postgres_database], force: true)

    SlotSupervisor.start_message_store!(consumer)

    if consumer.type == :sequin_stream do
      :ok
    else
      default_opts = [consumer: consumer]
      consumer_features = Consumers.consumer_features(consumer)

      {features, opts} = Keyword.pop(opts, :features, [])
      features = Keyword.merge(consumer_features, features)

      opts =
        default_opts
        |> Keyword.merge(opts)
        |> Keyword.put(:features, features)

      Sequin.DynamicSupervisor.start_child(supervisor, {pipeline(consumer), opts})
    end
  end

  def start_for_sink_consumer(supervisor, id, opts) do
    case Consumers.get_consumer(id) do
      {:ok, consumer} -> start_for_sink_consumer(supervisor, consumer, opts)
      error -> error
    end
  end

  def stop_for_sink_consumer(supervisor \\ consumer_supervisor(), consumer_or_id)

  def stop_for_sink_consumer(supervisor, %SinkConsumer{id: id}) do
    Enum.each(
      Map.values(@sinks_to_pipelines),
      &Sequin.DynamicSupervisor.stop_child(supervisor, &1.via_tuple(id))
    )

    :ok
  end

  def stop_for_sink_consumer(supervisor, id) do
    Enum.each(
      Map.values(@sinks_to_pipelines),
      &Sequin.DynamicSupervisor.stop_child(supervisor, &1.via_tuple(id))
    )

    :ok
  end

  def restart_for_sink_consumer(supervisor \\ consumer_supervisor(), consumer_or_id) do
    stop_for_sink_consumer(supervisor, consumer_or_id)
    start_for_sink_consumer(supervisor, consumer_or_id)
  end

  defp pipeline(%SinkConsumer{} = consumer) do
    Map.fetch!(@sinks_to_pipelines, consumer.type)
  end

  defp children do
    [
      Sequin.Runtime.Starter,
      Sequin.Runtime.ConsumerStarter,
      Sequin.DynamicSupervisor.child_spec(name: table_reader_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: slot_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: wal_event_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: consumer_supervisor())
    ]
  end
end
