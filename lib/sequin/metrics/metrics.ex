defmodule Sequin.Metrics do
  @moduledoc """
  A context for collecting and storing metrics.

  Specifically these are used to power metrics in the Console.
  """

  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Metrics.Store
  alias Sequin.Replication.PostgresReplicationSlot

  # Consumer Messages Processed
  def incr_consumer_messages_processed_count(%{id: id}, count \\ 1) do
    Store.incr_count("consumer_messages_processed:#{id}", count)
  end

  def get_consumer_messages_processed_count(%{id: id}) do
    Store.get_count("consumer_messages_processed:#{id}")
  end

  def reset_consumer_messages_processed_count(%{id: id}) do
    Store.reset_count("consumer_messages_processed:#{id}")
  end

  def incr_consumer_messages_processed_throughput(consumer, count \\ 1)

  def incr_consumer_messages_processed_throughput(_consumer, 0), do: :ok

  def incr_consumer_messages_processed_throughput(%{id: id}, count) do
    Store.incr_throughput("consumer_messages_processed_throughput:#{id}", count)
  end

  def incr_consumer_messages_processed_bytes(_consumer, 0), do: :ok

  def incr_consumer_messages_processed_bytes(%{id: id}, bytes) do
    Store.incr_throughput("consumer_messages_processed_bytes:#{id}", bytes)
  end

  def get_consumer_messages_processed_throughput_timeseries(%{id: id}, window_count \\ 60) do
    Store.get_throughput_timeseries("consumer_messages_processed_throughput:#{id}", window_count)
  end

  def get_consumer_messages_processed_throughput_timeseries_smoothed(%{id: id}, window_count \\ 60, smoothing_window \\ 5) do
    with {:ok, timeseries} <-
           Store.get_throughput_timeseries(
             "consumer_messages_processed_throughput:#{id}",
             window_count + smoothing_window
           ) do
      {:ok, smooth_timeseries(timeseries, window_count, smoothing_window)}
    end
  end

  def get_consumer_messages_processed_bytes_timeseries(%{id: id}, window_count \\ 60) do
    Store.get_throughput_timeseries("consumer_messages_processed_bytes:#{id}", window_count)
  end

  def get_consumer_messages_processed_throughput(consumer_id) when is_binary(consumer_id) do
    Store.get_throughput("consumer_messages_processed_throughput:#{consumer_id}")
  end

  def get_consumer_messages_processed_bytes_timeseries_smoothed(%{id: id}, window_count \\ 60, smoothing_window \\ 5) do
    with {:ok, timeseries} <-
           Store.get_throughput_timeseries(
             "consumer_messages_processed_bytes:#{id}",
             window_count + smoothing_window
           ) do
      {:ok, smooth_timeseries(timeseries, window_count, smoothing_window)}
    end
  end

  def get_consumer_messages_processed_bytes(consumer_id) when is_binary(consumer_id) do
    Store.get_throughput("consumer_messages_processed_bytes:#{consumer_id}")
  end

  # Database Average Latency
  def measure_database_avg_latency(%PostgresDatabase{id: id}, latency) do
    Store.measure_latency("database_avg_latency:#{id}", latency)
  end

  def get_database_avg_latency(%PostgresDatabase{id: id}) do
    Store.get_latency("database_avg_latency:#{id}")
  end

  # HTTP Endpoint Throughput
  def incr_http_endpoint_throughput(%HttpEndpoint{id: id}) do
    Store.incr_throughput("http_endpoint_throughput:#{id}")
  end

  def get_http_endpoint_throughput(%HttpEndpoint{id: id}) do
    Store.get_throughput("http_endpoint_throughput:#{id}")
  end

  def measure_postgres_replication_slot_lag(%PostgresReplicationSlot{id: id}, lag_bytes) do
    Store.measure_gauge("postgres_replication_slot_lag:#{id}", lag_bytes)
  end

  def get_postgres_replication_slot_lag(%PostgresReplicationSlot{id: id}) do
    Store.get_gauge("postgres_replication_slot_lag:#{id}")
  end

  defp smooth_timeseries(timeseries, window_count, smoothing_window) do
    {smoothed_timeseries, _} =
      Enum.reduce(timeseries, {[], []}, fn throughput, {smoothed_acc, rolling_acc} ->
        rolling_acc = Enum.take([throughput | rolling_acc], smoothing_window)
        smoothed = Enum.sum(rolling_acc) / length(rolling_acc)
        {[smoothed | smoothed_acc], rolling_acc}
      end)

    smoothed_timeseries
    |> Enum.take(window_count)
    |> Enum.reverse()
  end
end
