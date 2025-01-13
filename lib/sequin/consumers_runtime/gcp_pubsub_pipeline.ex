defmodule Sequin.ConsumersRuntime.GcpPubsubPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Sinks.Gcp.PubSub

  require Logger

  def start_link(opts) do
    %SinkConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload([:sequence, :postgres_database])

    producer = Keyword.get(opts, :producer, Sequin.ConsumersRuntime.ConsumerProducer)
    test_pid = Keyword.get(opts, :test_pid)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 100
        ]
      ],
      context: %{
        consumer: consumer,
        pubsub_client: GcpPubsubSink.pubsub_client(consumer.sink),
        test_pid: test_pid
      }
    )
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:consumers, {__MODULE__, consumer_id}}}
  end

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  # `data` is either a [ConsumerRecord] or a [ConsumerEvent]
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: consumer,
        pubsub_client: pubsub_client,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    pubsub_messages = Enum.map(messages, &build_pubsub_message(consumer, &1))

    case PubSub.publish_messages(pubsub_client, consumer.sink.topic_id, pubsub_messages) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            consumer.id,
            msg.replication_message_trace_id,
            "Published message to Pub/Sub successfully"
          )
        end)

        message

      {:error, error} ->
        Logger.warning("Failed to publish message to Pub/Sub: #{inspect(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            consumer.id,
            msg.replication_message_trace_id,
            "Failed to publish message to Pub/Sub: #{inspect(error)}"
          )
        end)

        Broadway.Message.failed(message, error)
    end
  end

  defp build_pubsub_message(consumer, %Sequin.Consumers.ConsumerRecord{} = record) do
    %{
      data: %{
        record: record.data.record,
        metadata: %{
          table_schema: record.data.metadata.table_schema,
          table_name: record.data.metadata.table_name,
          consumer: record.data.metadata.consumer,
          commit_lsn: record.commit_lsn,
          record_pks: record.record_pks
        }
      },
      attributes: %{
        "trace_id" => record.replication_message_trace_id,
        "type" => "record"
      },
      ordering_key: ordering_key(consumer, record.data)
    }
  end

  defp build_pubsub_message(consumer, %Sequin.Consumers.ConsumerEvent{} = event) do
    %{
      data: %{
        record: event.data.record,
        changes: event.data.changes,
        action: to_string(event.data.action),
        metadata: %{
          table_schema: event.data.metadata.table_schema,
          table_name: event.data.metadata.table_name,
          consumer: event.data.metadata.consumer,
          commit_timestamp: event.data.metadata.commit_timestamp,
          commit_lsn: event.commit_lsn,
          record_pks: event.record_pks
        }
      },
      attributes: %{
        "trace_id" => event.replication_message_trace_id,
        "type" => "event"
      },
      ordering_key: ordering_key(consumer, event.data)
    }
  end

  defp ordering_key(consumer, data) do
    consumer
    |> Sequin.Consumers.group_column_values(data)
    |> Enum.join(":")
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Sinks.Gcp.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
