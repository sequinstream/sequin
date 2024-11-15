defmodule Sequin.Kafka.Client do
  @moduledoc false
  @behaviour Sequin.Kafka

  alias Sequin.Consumers.KafkaDestination
  alias Sequin.Kafka.ConnectionCache
  alias Sequin.NetworkUtils

  @impl Sequin.Kafka
  def publish(%KafkaDestination{} = destination, message) do
    with {:ok, connection} <- ConnectionCache.connection(destination) do
      case :brod.produce_sync(
             connection,
             destination.topic,
             0,
             "",
             Jason.encode!(message)
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, to_sequin_error(reason)}
      end
    end
  end

  @impl Sequin.Kafka
  def test_connection(%KafkaDestination{} = destination) do
    with :ok <- test_hosts_reachability(destination),
         {:ok, _conn} <- ConnectionCache.connection(destination),
         {:ok, metadata} <- get_metadata(destination) do
      validate_topic_exists(metadata, destination.topic)
    else
      {:error, reason} ->
        {:error, to_sequin_error(reason)}
    end
  end

  @impl Sequin.Kafka
  def get_metadata(%KafkaDestination{} = destination) do
    :brod.get_metadata(
      KafkaDestination.hosts(destination),
      [destination.topic],
      KafkaDestination.to_brod_config(destination)
    )
  end

  defp to_sequin_error(error) do
    case error do
      :leader_not_available ->
        Sequin.Error.service(service: :kafka, message: "Leader not available")

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

  defp test_hosts_reachability(destination) do
    results =
      destination
      |> KafkaDestination.hosts()
      |> Enum.map(fn {hostname, port} ->
        NetworkUtils.test_tcp_reachability(hostname, port, destination.tls, :timer.seconds(10))
      end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, Sequin.Error.validation(summary: "Unable to reach Kafka hosts")}
    end
  end
end
