defmodule Sequin.Metrics.SinkConsumerMetrics do
  @moduledoc """
  Module for reporting sink consumer metrics to Datadog.
  """

  alias Sequin.Consumers
  alias Sequin.Metrics
  alias Sequin.Statsd

  @doc """
  Reports sink consumer throughput metrics to Datadog.

  This function:
  1. Gets all active sink consumer IDs
  2. For each consumer, gets the messages and bytes throughput
  3. Reports these metrics to Datadog with appropriate tags
  """
  def report_sink_consumer_throughput do
    consumer_ids = Consumers.list_active_sink_consumer_ids()

    Enum.each(consumer_ids, fn consumer_id ->
      report_consumer_throughput(consumer_id)
    end)
  end

  defp report_consumer_throughput(consumer_id) do
    # Get throughput metrics
    with {:ok, messages_throughput} <- Metrics.get_consumer_messages_processed_throughput(consumer_id),
         {:ok, bytes_throughput} <- Metrics.get_consumer_messages_processed_bytes(consumer_id) do
      # Report to Datadog
      Statsd.gauge("sequin.consumers.throughput_messages", messages_throughput,
        tags: %{
          consumer_id: consumer_id
        }
      )

      Statsd.gauge("sequin.consumers.throughput_bytes", bytes_throughput,
        tags: %{
          consumer_id: consumer_id
        }
      )
    end
  end
end
