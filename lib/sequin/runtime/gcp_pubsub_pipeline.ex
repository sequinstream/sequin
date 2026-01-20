defmodule Sequin.Runtime.GcpPubsubPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Gcp.PubSub
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: %SinkConsumer{sink: sink}} = context
    Map.put(context, :pubsub_client, GcpPubsubSink.pubsub_client(sink))
  end

  @max_bytes Sequin.Size.mb(10)
  # GCP Pub/Sub ordering key has a maximum of 1024 bytes
  @max_ordering_key_bytes 1024

  @impl SinkPipeline
  def batchers_config(consumer) do
    # If message grouping is enabled, we can only send one message at a time
    # due to GCP Pub/Sub's ordering_key requirement.
    batch_size = if consumer.message_grouping, do: 1, else: SinkPipeline.batcher(consumer.batch_size, @max_bytes * 0.9)
    max_demand = if consumer.message_grouping, do: 1, else: consumer.batch_size

    [
      default: [
        concurrency: 400,
        max_demand: max_demand,
        batch_size: batch_size,
        batch_timeout: 1
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.GcpPubsub{topic_id: topic_id} = Routing.route_message(context.consumer, message.data)

    group_id = message.data.group_id
    ordering_key = {topic_id, group_id}

    encoded_data = build_pubsub_message(context.consumer, message.data)
    encoded_data_size_bytes = pubsub_message_byte_size(encoded_data)

    consumer_message = %{
      message.data
      | encoded_data: encoded_data,
        encoded_data_size_bytes: encoded_data_size_bytes
    }

    message =
      message
      |> Broadway.Message.put_data(consumer_message)
      |> Broadway.Message.put_batch_key(ordering_key)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{pubsub_client: pubsub_client, test_pid: test_pid} = context
    {topic_id, _group_id} = batch_info.batch_key

    setup_allowances(test_pid)

    pubsub_messages = Enum.map(messages, fn %{data: data} -> data.encoded_data end)

    case PubSub.publish_messages(pubsub_client, topic_id, pubsub_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_pubsub_message(consumer, %Sequin.Consumers.ConsumerEvent{} = event) do
    msg = %{
      "data" => Base.encode64(Jason.encode!(Message.to_external(consumer, event))),
      "attributes" => %{
        "trace_id" => event.replication_message_trace_id,
        "type" => "event",
        "table_name" => event.data.metadata.table_name,
        "action" => to_string(event.data.action)
      }
    }

    ordering_key = Sequin.String.truncate_with_hash(event.group_id, @max_ordering_key_bytes)
    Sequin.Map.put_if_present(msg, "orderingKey", ordering_key)
  end

  defp pubsub_message_byte_size(%{"data" => data, "attributes" => attributes}) do
    data_size = byte_size(data)
    attributes_size = :erlang.external_size(attributes)
    data_size + attributes_size
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(PubSub, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
