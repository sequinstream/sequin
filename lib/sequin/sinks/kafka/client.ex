defmodule Sequin.Sinks.Kafka.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.Kafka

  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.NetworkUtils
  alias Sequin.Sinks.Kafka
  alias Sequin.Sinks.Kafka.ConnectionCache

  require Logger

  @publish_retry_count 5
  @impl Kafka
  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, topic, partition, messages) when is_list(messages) do
    do_publish(consumer, topic, partition, messages, 0)
  end

  defp do_publish(consumer, topic, partition, messages, retry_count, original_error \\ nil)

  defp do_publish(consumer, topic, partition, messages, retry_count, original_error)
       when retry_count <= @publish_retry_count do
    with {:ok, connection} <- ConnectionCache.connection(consumer.sink),
         :ok <-
           :brod.produce_sync(
             connection,
             topic,
             partition,
             "unused_key",
             messages
           ) do
      :ok
    else
      {:error, reason} ->
        message = reason |> to_sequin_error() |> Exception.message()
        Logger.warning("Failed to publish to Kafka: #{message}, retrying...")

        backoff_ms = Sequin.Time.exponential_backoff(200, retry_count)
        :timer.sleep(backoff_ms)
        do_publish(consumer, topic, partition, messages, retry_count + 1, original_error || reason)
    end
  rescue
    error ->
      message = error |> to_sequin_error() |> Exception.message()
      Logger.warning("Failed to publish to Kafka: #{message}, retrying...")

      backoff_ms = Sequin.Time.exponential_backoff(200, retry_count)
      :timer.sleep(backoff_ms)
      do_publish(consumer, topic, partition, messages, retry_count + 1, original_error || error)
  end

  defp do_publish(_consumer, _topic, _partition, _messages, _retry_count, error) do
    {:error, to_sequin_error(error)}
  end

  @impl Kafka
  @doc """
  Test the connection to the Kafka cluster.
  """
  def test_connection(%KafkaSink{} = sink) do
    with :ok <- test_hosts_reachability(sink),
         {:ok, _} <- get_metadata(sink) do
      :ok
    else
      {:error, reason} ->
        {:error, to_sequin_error(reason)}
    end
  rescue
    error ->
      {:error, Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(error)}")}
  catch
    :exit, reason ->
      {:error, Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(reason)}")}

    {:failed_to_connect, [{{_host, _port}, {{:sasl_auth_error, message}, _}}]} ->
      {:error, Sequin.Error.validation(summary: "SASL authentication error: #{message}")}

    {:failed_to_connect, error} ->
      Logger.warning("Failed to connect to kafka on `hosts`: #{inspect(error)}")
      {:error, Sequin.Error.validation(summary: "Failed to connect to kafka on `hosts`")}

    error ->
      {:error, Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(error)}")}
  end

  @impl Kafka
  def get_metadata(%KafkaSink{} = sink, topic \\ nil) do
    hosts = KafkaSink.hosts(sink)
    topics = if topic, do: [topic], else: :all
    config = KafkaSink.to_brod_config(sink)

    :brod.get_metadata(hosts, topics, config)
  end

  @impl Kafka
  @doc """
  Get the partition count for a topic.

  If we receive :leader_not_available, it indicates that the topic does not exist in the cluster.

  This metadata call will cause Kafka to auto-create the topic if Kafka is configured to do so. But
  this is asynchronous, so we need to retry if the topic is not found.
  """
  def get_partition_count(%KafkaSink{} = sink, topic, opts \\ [retry_count: 0, max_retries: 3]) do
    with {:ok, metadata} <- get_metadata(sink, topic) do
      case Enum.find(metadata.topics, fn t -> t.name == topic end) do
        %{error_code: :no_error, partitions: partitions} ->
          {:ok, length(partitions)}

        %{error_code: :leader_not_available} ->
          if opts[:retry_count] < opts[:max_retries] do
            # Backoff
            100
            |> Sequin.Time.exponential_backoff(opts[:retry_count])
            |> Process.sleep()

            # Retry
            opts = Keyword.update!(opts, :retry_count, &(&1 + 1))
            get_partition_count(sink, topic, opts)
          else
            {:error, Sequin.Error.service(service: :kafka, message: "Topic '#{topic}' does not exist")}
          end

        _ ->
          {:error, Sequin.Error.service(service: :kafka, message: "Topic '#{topic}' does not exist")}
      end
    end
  end

  defp to_sequin_error(error, extra \\ nil) do
    case error do
      :unknown_topic_or_partition ->
        Sequin.Error.service(service: :kafka, message: "Topic or partition not found: #{inspect(extra)}")

      :leader_not_available ->
        Sequin.Error.service(service: :kafka, message: "Leader not available")

      error when is_exception(error) ->
        Sequin.Error.service(service: :kafka, message: "Kafka error: #{Exception.message(error)}")

      error ->
        Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(error)}")
    end
  end

  # TODO: Add ipv6 support
  defp test_hosts_reachability(%KafkaSink{} = sink) do
    results =
      sink
      |> KafkaSink.hosts()
      |> Enum.map(fn {hostname, port} ->
        NetworkUtils.test_tcp_reachability(hostname, port, false, to_timeout(second: 5))
      end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, Sequin.Error.validation(summary: "Unable to reach Kafka hosts")}
    end
  end
end
