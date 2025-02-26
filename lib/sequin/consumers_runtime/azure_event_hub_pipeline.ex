defmodule Sequin.ConsumersRuntime.AzureEventHubPipeline do
  @moduledoc false
  @behaviour Sequin.ConsumersRuntime.SinkPipeline

  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.SinkPipeline
  alias Sequin.Sinks.Azure.EventHub

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: %SinkConsumer{sink: sink}} = context
    Map.put(context, :event_hub_client, AzureEventHubSink.event_hub_client(sink))
  end

  @impl SinkPipeline
  def processors_config(%SinkConsumer{max_waiting: max_waiting}) do
    [
      default: [
        concurrency: max_waiting,
        max_demand: 100
      ]
    ]
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{batch_size: batch_size}) do
    [
      default: [
        concurrency: 1,
        batch_size: batch_size,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, _context) do
    # Just pass the message through - all work happens in handle_batch
    message
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, event_hub_client: event_hub_client, test_pid: test_pid} = context
    setup_allowances(test_pid)

    event_hub_messages =
      Enum.flat_map(messages, fn message ->
        Enum.map(message.data, &build_event_hub_message(consumer, &1))
      end)

    case EventHub.publish_messages(event_hub_client, event_hub_messages) do
      :ok -> SinkPipeline.on_success(context, messages)
      {:error, error} -> SinkPipeline.on_failure(context, error, messages)
    end
  end

  defp build_event_hub_message(_consumer, %Sequin.Consumers.ConsumerRecord{} = record) do
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
      }
    }
  end

  defp build_event_hub_message(_consumer, %Sequin.Consumers.ConsumerEvent{} = event) do
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
      }
    }
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Sinks.Azure.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
