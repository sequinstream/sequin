defmodule Sequin.Sinks.Kafka.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.Kafka

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.NetworkUtils
  alias Sequin.Sinks.Kafka
  alias Sequin.Sinks.Kafka.ConnectionCache

  require Logger

  @impl Kafka
  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, message)
      when is_struct(message, ConsumerRecord) or is_struct(message, ConsumerEvent) do
    message_key = Kafka.message_key(consumer, message)

    with {:ok, connection} <- ConnectionCache.connection(consumer.sink),
         :ok <-
           :brod.produce_sync(
             connection,
             consumer.sink.topic,
             :hash,
             message_key,
             Jason.encode!(message.data)
           ) do
      :ok
    else
      {:error, reason} -> {:error, to_sequin_error(reason)}
    end
  end

  @impl Kafka
  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, partition, messages) when is_list(messages) do
    with {:ok, connection} <- ConnectionCache.connection(consumer.sink),
         :ok <-
           :brod.produce_sync(
             connection,
             consumer.sink.topic,
             partition,
             "unused_key",
             Enum.map(messages, fn message ->
               %{key: Kafka.message_key(consumer, message), value: Jason.encode!(message.data)}
             end)
           ) do
      :ok
    else
      {:error, reason} -> {:error, to_sequin_error(reason)}
    end
  end

  @impl Kafka
  def test_connection(%KafkaSink{} = sink) do
    with :ok <- test_hosts_reachability(sink),
         {:ok, metadata} <- get_metadata(sink) do
      validate_topic_exists(metadata, sink.topic)
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
  def get_metadata(%KafkaSink{} = sink) do
    hosts = KafkaSink.hosts(sink)
    topics = [sink.topic]
    config = KafkaSink.to_brod_config(sink)

    :brod.get_metadata(hosts, topics, config)
  end

  @impl Kafka
  def get_partition_count(%KafkaSink{} = sink) do
    with {:ok, metadata} <- get_metadata(sink) do
      case Enum.find(metadata.topics, fn t -> t.name == sink.topic end) do
        %{error_code: :no_error, partitions: partitions} -> {:ok, length(partitions)}
        _ -> {:error, Sequin.Error.service(service: :kafka, message: "Topic '#{sink.topic}' does not exist")}
      end
    end
  end

  defp to_sequin_error(error) do
    case error do
      :leader_not_available ->
        Sequin.Error.service(service: :kafka, message: "Leader not available")

      error when is_exception(error) ->
        Sequin.Error.service(service: :kafka, message: "Kafka error: #{Exception.message(error)}")

      error ->
        Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(error)}")
    end
  end

  defp validate_topic_exists(%{topics: topics}, topic_name) do
    case Enum.find(topics, fn t -> t.name == topic_name end) do
      %{error_code: :no_error} -> :ok
      _ -> {:error, Sequin.Error.service(service: :kafka, message: "Topic '#{topic_name}' does not exist")}
    end
  end

  # TODO: Add ipv6 support
  defp test_hosts_reachability(%KafkaSink{} = sink) do
    results =
      sink
      |> KafkaSink.hosts()
      |> Enum.map(fn {hostname, port} ->
        NetworkUtils.test_tcp_reachability(hostname, port, false, :timer.seconds(5))
      end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, Sequin.Error.validation(summary: "Unable to reach Kafka hosts")}
    end
  end
end
