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
         {:ok, metadata} <- get_metadata(destination) do
      validate_topic_exists(metadata, destination.topic)
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

    {:failed_to_connect, _} ->
      {:error, Sequin.Error.validation(summary: "Failed to connect to kafka on `hosts`")}

    error ->
      {:error, Sequin.Error.service(service: :kafka, message: "Kafka error: #{inspect(error)}")}
  end

  @impl Sequin.Kafka
  def get_metadata(%KafkaDestination{} = destination) do
    destination
    |> KafkaDestination.hosts()
    |> :brod.get_metadata(
      [destination.topic],
      KafkaDestination.to_brod_config(destination)
    )
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

  defp test_hosts_reachability(%KafkaDestination{tls: true}) do
    {:error, Sequin.Error.validation(summary: "Talk to the Sequin team to enable TLS for Kafka destinations")}
  end

  defp test_hosts_reachability(%KafkaDestination{} = destination) do
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
