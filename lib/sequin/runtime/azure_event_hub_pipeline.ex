defmodule Sequin.Runtime.AzureEventHubPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Azure.EventHub
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: %SinkConsumer{sink: sink}} = context
    Map.put(context, :event_hub_client, AzureEventHubSink.event_hub_client(sink))
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
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, event_hub_client: event_hub_client, test_pid: test_pid} = context
    setup_allowances(test_pid)

    event_hub_messages =
      Enum.map(messages, fn %{data: data} ->
        build_event_hub_message(consumer, data)
      end)

    case EventHub.publish_messages(event_hub_client, event_hub_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_event_hub_message(%SinkConsumer{} = consumer, %Sequin.Consumers.ConsumerRecord{} = record) do
    %{
      data: Message.to_external(consumer, record),
      attributes: %{
        "trace_id" => record.replication_message_trace_id,
        "type" => "record"
      }
    }
  end

  defp build_event_hub_message(consumer, %Sequin.Consumers.ConsumerEvent{} = event) do
    %{
      data: Message.to_external(consumer, event),
      attributes: %{
        "trace_id" => event.replication_message_trace_id,
        "type" => "event"
      }
    }
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Sinks.Azure.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
