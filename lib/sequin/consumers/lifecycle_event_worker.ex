defmodule Sequin.Consumers.LifecycleEventWorker do
  @moduledoc """
  Worker that runs after a lifecycle event (create/update/delete) in the consumer context.
  """

  use Oban.Worker,
    queue: :default,
    max_attempts: 3

  alias Sequin.Consumers
  alias Sequin.ConsumersRuntime.Supervisor, as: ConsumersSupervisor
  alias Sequin.DatabasesRuntime.SlotSupervisor
  alias Sequin.DatabasesRuntime.Supervisor, as: DatabasesRuntimeSupervisor
  alias Sequin.Health.CheckHttpEndpointHealthWorker

  require Logger

  @events ~w(create update delete)a
  @entities ~w(sink_consumer http_endpoint backfill)a

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
    end
  end

  defp handle_consumer_event(event, id, data) do
    case event do
      "create" ->
        with {:ok, consumer} <- Consumers.get_consumer(id),
             :ok <- DatabasesRuntimeSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id) do
          SlotSupervisor.start_message_store!(consumer)
          ConsumersSupervisor.start_for_sink_consumer(consumer)
          :ok
        end

      "update" ->
        with {:ok, consumer} <- Consumers.get_consumer(id),
             :ok <- DatabasesRuntimeSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id) do
          case consumer.status do
            :active ->
              SlotSupervisor.start_message_store!(consumer)
              ConsumersSupervisor.restart_for_sink_consumer(consumer)
              :ok

            :disabled ->
              SlotSupervisor.stop_message_store(consumer.replication_slot_id, consumer.id)
              ConsumersSupervisor.stop_for_sink_consumer(consumer)
              :ok
          end
        end

      "delete" ->
        replication_slot_id = Map.fetch!(data, "replication_slot_id")

        with :ok <- DatabasesRuntimeSupervisor.refresh_message_handler_ctx(replication_slot_id),
             :ok <- SlotSupervisor.stop_message_store(replication_slot_id, id) do
          ConsumersSupervisor.stop_for_sink_consumer(id)
        end
    end
  end

  defp handle_http_endpoint_event(event, id) do
    case event do
      "create" ->
        CheckHttpEndpointHealthWorker.enqueue(id)

      "update" ->
        CheckHttpEndpointHealthWorker.enqueue(id)

        id
        |> Consumers.list_sink_consumers_for_http_endpoint()
        |> Enum.each(&ConsumersSupervisor.restart_for_sink_consumer(&1))

      "delete" ->
        :ok
    end
  end

  defp handle_backfill_event(event, id) do
    case event do
      "create" ->
        with {:ok, backfill} <- Consumers.get_backfill(id),
             {:ok, consumer} <- Consumers.get_consumer(backfill.sink_consumer_id) do
          DatabasesRuntimeSupervisor.start_table_reader(consumer)
        end

      "update" ->
        with {:ok, backfill} <- Consumers.get_backfill(id),
             {:ok, consumer} <- Consumers.get_consumer(backfill.sink_consumer_id) do
          case backfill.state do
            :active ->
              DatabasesRuntimeSupervisor.restart_table_reader(consumer)

            s when s in [:cancelled, :completed] ->
              Logger.info("Stopping TableReaderServer for backfill #{backfill.id}", backfill_status: s)
              DatabasesRuntimeSupervisor.stop_table_reader(backfill.id)
          end

          :ok
        end
    end
  end
end
