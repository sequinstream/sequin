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

  def get_consumer_messages_processed_bytes_timeseries(%{id: id}, window_count \\ 60) do
    Store.get_throughput_timeseries("consumer_messages_processed_bytes:#{id}", window_count)
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

  # HTTP Endpoint Average Latency
  def measure_http_endpoint_avg_latency(%HttpEndpoint{id: id}, latency) do
    Store.measure_latency("http_endpoint_avg_latency:#{id}", latency)
  end

  def get_http_endpoint_avg_latency(%HttpEndpoint{id: id}) do
    Store.get_latency("http_endpoint_avg_latency:#{id}")
  end

  def measure_postgres_replication_slot_lag(%PostgresReplicationSlot{id: id}, lag_bytes) do
    Store.measure_gauge("postgres_replication_slot_lag:#{id}", lag_bytes)
  end

  def get_postgres_replication_slot_lag(%PostgresReplicationSlot{id: id}) do
    Store.get_gauge("postgres_replication_slot_lag:#{id}")
  end
end
