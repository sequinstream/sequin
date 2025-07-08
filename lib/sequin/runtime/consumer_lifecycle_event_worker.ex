defmodule Sequin.Runtime.ConsumerLifecycleEventWorker do
  @moduledoc """
  Worker that runs after a lifecycle event (create/update/delete) in the consumer context.
  """

  use Oban.Worker,
    queue: :lifecycle,
    max_attempts: 3

  alias Sequin.Consumers
  alias Sequin.Consumers.Function
  alias Sequin.Functions.MiniElixir
  alias Sequin.Health
  alias Sequin.Health.CheckHttpEndpointHealthWorker
  alias Sequin.Health.CheckSinkConfigurationWorker
  alias Sequin.Repo
  alias Sequin.Runtime.InitBackfillStatsWorker
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.Supervisor, as: RuntimeSupervisor

  require Logger

  @events ~w(create update delete)a
  @entities ~w(sink_consumer http_endpoint backfill function)a

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

      "function" ->
        handle_function_event(event, entity_id)
    end
  end

  defp handle_consumer_event(event, id, data) do
    Logger.metadata(consumer_id: id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for consumer")

    :syn.publish(:consumers, :consumers_changed, :consumers_changed)

    case event do
      "create" ->
        with {:ok, consumer} <- Consumers.get_consumer(id) do
          CheckSinkConfigurationWorker.enqueue(consumer.id)
          RuntimeSupervisor.start_for_sink_consumer(consumer)
          :ok = RuntimeSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
          RuntimeSupervisor.maybe_start_table_readers(consumer)
          :ok
        end

      "update" ->
        with {:ok, consumer} <- Consumers.get_consumer(id) do
          consumer = Repo.preload(consumer, :replication_slot)
          CheckSinkConfigurationWorker.enqueue(consumer.id)

          if consumer.status != :disabled do
            # Restart the entire supervision tree for replication, including slot processor, smss, etc.
            # This is safest- later we can be a bit more intelligent about when to restart (ie. when name changes we don't have to restart)
            {:ok, _} = RuntimeSupervisor.restart_replication(consumer.replication_slot)
          end

          :ok
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
    Consumers.invalidate_cached_http_endpoint(id)

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
              RuntimeSupervisor.restart_table_reader(backfill)

            s when s in [:cancelled, :completed, :failed] ->
              # Clear out the backfill_fetch_batch event - if backfill was erroring, we want to
              # clear out that state/event.
              Health.delete_event(consumer.id, "backfill_fetch_batch")
              Logger.info("Stopping TableReaderServer for backfill #{backfill.id}", backfill_status: s)
              RuntimeSupervisor.stop_table_reader(backfill.id)

              Sequin.Runtime.TableReaderServer.remove_backfill_id_from_table_oid_to_backfill_id_ets_table(
                backfill.table_oid,
                backfill.id
              )
          end

          :ok
        end
    end
  end

  defp handle_function_event(event, id) when is_binary(id) do
    Logger.metadata(function_id: id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for function")

    with {:ok, function} <- Consumers.get_function(id) do
      handle_function_event(event, function)
    end
  end

  defp handle_function_event(event, %Function{} = function) do
    case event do
      "create" ->
        case maybe_recompile_function(function) do
          :ok ->
            :ok

          {:error, error} ->
            Logger.error("[LifecycleEventWorker] Failed to create function", error: error)
            {:error, error}
        end

      "update" ->
        consumers = Consumers.list_consumers_for_function(function.account_id, function.id, [:replication_slot])
        replication_slots = consumers |> Enum.map(& &1.replication_slot) |> Enum.uniq_by(& &1.id)
        Enum.each(replication_slots, &RuntimeSupervisor.stop_replication/1)

        case maybe_recompile_function(function) do
          :ok ->
            Enum.each(replication_slots, &RuntimeSupervisor.restart_replication/1)
            :ok

          {:error, error} ->
            Logger.error("[LifecycleEventWorker] Failed to create function", error: error)
            {:error, error}
        end
    end
  end

  # Path functions don't require compilation
  defp maybe_recompile_function(%Function{type: "path"}), do: :ok

  defp maybe_recompile_function(%Function{} = function) do
    case MiniElixir.create(function.id, function.function.code) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end
end
