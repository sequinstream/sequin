defmodule Sequin.Runtime.ConsumerLifecycleEventWorker do
  @moduledoc """
  Worker that runs after a lifecycle event (create/update/delete) in the consumer context.
  """

  use Oban.Worker,
    queue: :default,
    max_attempts: 3

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Health.CheckHttpEndpointHealthWorker
  alias Sequin.Health.CheckSinkConfigurationWorker
  alias Sequin.Repo
  alias Sequin.Runtime.InitBackfillStatsWorker
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.Supervisor, as: RuntimeSupervisor
  alias Sequin.Transforms.MiniElixir

  require Logger

  @events ~w(create update delete)a
  @entities ~w(sink_consumer http_endpoint backfill transform)a

  @spec enqueue(event :: atom(), entity_type :: atom(), entity_id :: String.t(), data :: map() | nil) ::
          {:ok, Oban.Job.t()} | {:error, Oban.Job.changeset() | term()}
  def enqueue(event, entity_type, entity_id, data \\ nil) when event in @events and entity_type in @entities do
    Oban.insert(new(%{event: event, entity_type: entity_type, entity_id: entity_id, data: data}))
  end

  @impl Oban.Worker
  def perform(%Oban.Job{
        args: %{"event" => event, "entity_type" => entity_type, "entity_id" => entity_id, "data" => data}
      }) do
    case entity_type do
      "sink_consumer" ->
        handle_consumer_event(event, entity_id, data)

      "http_endpoint" ->
        handle_http_endpoint_event(event, entity_id)

      "backfill" ->
        handle_backfill_event(event, entity_id)

      "transform" ->
        handle_transform_event(event, entity_id)
    end
  end

  defp handle_consumer_event(event, id, data) do
    Logger.metadata(consumer_id: id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for consumer")

    :syn.publish(:consumers, :consumers_changed, :consumers_changed)

    case event do
      "create" ->
        with {:ok, consumer} <- Consumers.get_consumer(id) do
          consumer = Repo.preload(consumer, :postgres_database)
          # Lazy, just do this on every sink create. Eventually, we'll retire Sequence
          Databases.update_sequences_from_db(consumer.postgres_database)
          CheckSinkConfigurationWorker.enqueue(consumer.id, unique: false)
          RuntimeSupervisor.start_for_sink_consumer(consumer)
          :ok = RuntimeSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
          RuntimeSupervisor.maybe_start_table_reader(consumer)
          :ok
        end

      "update" ->
        with {:ok, consumer} <- Consumers.get_consumer(id) do
          consumer = Repo.preload(consumer, :replication_slot)
          CheckSinkConfigurationWorker.enqueue(consumer.id, unique: false)

          # Restart the entire supervision tree for replication, including slot processor, smss, etc.
          # This is safest- later we can be a bit more intelligent about when to restart (ie. when name changes we don't have to restart)
          {:ok, _} = RuntimeSupervisor.restart_replication(consumer.replication_slot)
        end

      "delete" ->
        replication_slot_id = Map.fetch!(data, "replication_slot_id")

        :ok = RuntimeSupervisor.refresh_message_handler_ctx(replication_slot_id)
        :ok = SlotProcessorServer.demonitor_message_store(replication_slot_id, id)
        :ok = RuntimeSupervisor.stop_for_sink_consumer(replication_slot_id, id)
        :ok = MessageLedgers.drop_for_consumer(id)
    end
  end

  defp handle_http_endpoint_event(event, id) do
    Logger.metadata(http_endpoint_id: id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for http_endpoint")

    case event do
      "create" ->
        CheckHttpEndpointHealthWorker.enqueue(id)

      "update" ->
        CheckHttpEndpointHealthWorker.enqueue(id)

        id
        |> Consumers.list_sink_consumers_for_http_endpoint()
        |> Enum.each(&RuntimeSupervisor.restart_for_sink_consumer(&1))

      "delete" ->
        :ok
    end
  end

  defp handle_backfill_event(event, id) do
    Logger.metadata(backfill_id: id)
    Logger.info("[LifecycleEventWorker] Loading backfill and consumer for backfill #{id}")

    case event do
      "create" ->
        with {:ok, backfill} <- Consumers.get_backfill(id),
             {:ok, consumer} <- Consumers.get_consumer(backfill.sink_consumer_id) do
          Logger.metadata(consumer_id: consumer.id)
          Logger.info("[LifecycleEventWorker] Handling event `#{event}` for backfill")
          InitBackfillStatsWorker.enqueue(backfill.id)
          RuntimeSupervisor.start_table_reader(consumer)
        end

      "update" ->
        with {:ok, backfill} <- Consumers.get_backfill(id),
             {:ok, consumer} <- Consumers.get_consumer(backfill.sink_consumer_id) do
          Logger.metadata(consumer_id: consumer.id)
          Logger.info("[LifecycleEventWorker] Handling event `#{event}` for backfill")

          case backfill.state do
            :active ->
              RuntimeSupervisor.restart_table_reader(consumer)

            s when s in [:cancelled, :completed] ->
              # Clear out the backfill_fetch_batch event - if backfill was erroring, we want to
              # clear out that state/event.
              Health.delete_event(consumer.id, "backfill_fetch_batch")
              Logger.info("Stopping TableReaderServer for backfill #{backfill.id}", backfill_status: s)
              RuntimeSupervisor.stop_table_reader(backfill.id)
          end

          :ok
        end
    end
  end

  defp handle_transform_event(event, id) do
    Logger.metadata(transform_id: id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for transform")

    case event do
      "create" ->
        with {:ok, transform} <- Consumers.get_transform(id) do
          case MiniElixir.create(id, transform.transform.code) do
            {:ok, _} ->
              :ok

            {:error, error} ->
              Logger.error("[LifecycleEventWorker] Failed to create transform", error: error)
              {:error, error}
          end
        end

      "update" ->
        with {:ok, transform} <- Consumers.get_transform(id) do
          consumers = Consumers.list_consumers_for_transform(transform.account_id, transform.id, [:replication_slot])
          replication_slots = consumers |> Enum.map(& &1.replication_slot) |> Enum.uniq_by(& &1.id)
          Enum.each(replication_slots, &RuntimeSupervisor.stop_replication/1)

          case MiniElixir.create(id, transform.transform.code) do
            {:ok, _} ->
              Enum.each(replication_slots, &RuntimeSupervisor.restart_replication/1)
              :ok

            {:error, error} ->
              Logger.error("[LifecycleEventWorker] Failed to create transform", error: error)
              {:error, error}
          end
        end
    end
  end
end
