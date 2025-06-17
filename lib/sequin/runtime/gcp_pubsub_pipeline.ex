defmodule Sequin.Runtime.GcpPubsubPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Gcp.PubSub
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: %SinkConsumer{sink: sink}} = context
    Map.put(context, :pubsub_client, GcpPubsubSink.pubsub_client(sink))
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    [
      default: [
        concurrency: 400,
        batch_size: 10,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{test_pid: test_pid} = context
    setup_allowances(test_pid)

    record_or_event = message.data

    ordering_key = record_or_event.group_id
    message = Broadway.Message.put_batch_key(message, ordering_key)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, pubsub_client: pubsub_client, test_pid: test_pid} = context
    setup_allowances(test_pid)

    pubsub_messages =
      Enum.map(messages, fn %{data: data} ->
        build_pubsub_message(consumer, data)
      end)

    case PubSub.publish_messages(pubsub_client, consumer.sink.topic_id, pubsub_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_pubsub_message(consumer, %Sequin.Consumers.ConsumerRecord{} = record) do
    %{
      data: Message.to_external(consumer, record),
      attributes: %{
        "trace_id" => record.replication_message_trace_id,
        "type" => "record",
        "table_name" => record.data.metadata.table_name
      },
      ordering_key: record.group_id
    }
  end

  defp build_pubsub_message(consumer, %Sequin.Consumers.ConsumerEvent{} = event) do
    %{
      data: Message.to_external(consumer, event),
      attributes: %{
        "trace_id" => event.replication_message_trace_id,
        "type" => "event",
        "table_name" => event.data.metadata.table_name,
        "action" => to_string(event.data.action)
      },
      ordering_key: event.group_id
    }
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Sinks.Gcp.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
