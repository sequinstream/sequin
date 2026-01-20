defmodule Sequin.Runtime.AzureEventHubPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Broadway.Message
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.Routing.Consumers.AzureEventHub
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Azure.EventHub
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, opts) do
    %{consumer: %SinkConsumer{sink: sink}} = context
    req_opts = Keyword.get(opts, :req_opts, [])
    Map.put(context, :event_hub_client, AzureEventHubSink.event_hub_client(sink, req_opts))
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    [
      default: [
        concurrency: 400,
        batch_size: 10,
        batch_timeout: 5
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %AzureEventHub{event_hub_name: event_hub_name} = Routing.route_message(consumer, message)

    {:ok, Broadway.Message.put_batch_key(message, event_hub_name), context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{consumer: consumer, event_hub_client: event_hub_client, test_pid: test_pid} = context
    setup_allowances(test_pid)

    event_hub_name = batch_info.batch_key

    event_hub_messages =
      Enum.map(messages, fn %Broadway.Message{data: data} ->
        build_event_hub_message(consumer, data)
      end)

    case EventHub.publish_messages(event_hub_client, event_hub_name, event_hub_messages) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
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
