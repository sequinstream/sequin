defmodule Sequin.Metrics do
  @moduledoc """
  A context for collecting and storing metrics.

  Specifically these are used to power metrics in the Console.
  """

  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Metrics.Store

  # Consumer Messages Processed
  def incr_consumer_messages_processed_count(%{id: id}, count \\ 1) do
    Store.incr_count("consumer_messages_processed:#{id}", count)
  end

  def get_consumer_messages_processed_count(%{id: id}) do
    Store.get_count("consumer_messages_processed:#{id}")
  end

  def incr_consumer_messages_processed_throughput(consumer, count \\ 1)

  def incr_consumer_messages_processed_throughput(_consumer, 0), do: :ok

  def incr_consumer_messages_processed_throughput(%{id: id}, count) do
    Store.incr_throughput("consumer_messages_processed_throughput:#{id}", count)
  end

  def get_consumer_messages_processed_throughput(%{id: id}) do
    Store.get_throughput("consumer_messages_processed_throughput:#{id}")
  end

  def get_consumer_messages_processed_throughput_timeseries(%{id: id}) do
    Store.get_throughput_timeseries("consumer_messages_processed_throughput:#{id}")
  end

  # Database Average Latency
  def incr_database_avg_latency(%PostgresDatabase{id: id}, latency) do
    Store.incr_avg("database_avg_latency:#{id}", latency)
  end

  def get_database_avg_latency(%PostgresDatabase{id: id}) do
    Store.get_avg("database_avg_latency:#{id}")
  end

  # HTTP Endpoint Throughput
  def incr_http_endpoint_throughput(%HttpEndpoint{id: id}) do
    Store.incr_throughput("http_endpoint_throughput:#{id}")
  end

  def get_http_endpoint_throughput(%HttpEndpoint{id: id}) do
    Store.get_throughput("http_endpoint_throughput:#{id}")
  end

  # HTTP Endpoint Average Latency
  def incr_http_endpoint_avg_latency(%HttpEndpoint{id: id}, latency) do
    Store.incr_avg("http_endpoint_avg_latency:#{id}", latency)
  end

  def get_http_endpoint_avg_latency(%HttpEndpoint{id: id}) do
    Store.get_avg("http_endpoint_avg_latency:#{id}")
  end
end
