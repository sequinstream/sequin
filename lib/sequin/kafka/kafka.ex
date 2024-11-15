defmodule Sequin.Kafka do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.KafkaDestination
  alias Sequin.Error

  @callback publish(KafkaDestination.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(KafkaDestination.t()) :: :ok | {:error, Error.t()}
  @callback get_metadata(KafkaDestination.t()) :: {:ok, any()} | {:error, Error.t()}

  @spec publish(KafkaDestination.t(), ConsumerRecordData.t() | ConsumerEventData.t()) :: :ok | {:error, Error.t()}
  def publish(%KafkaDestination{} = destination, message) do
    impl().publish(destination, message)
  end

  @spec test_connection(KafkaDestination.t()) :: :ok | {:error, Error.t()}
  def test_connection(%KafkaDestination{} = destination) do
    impl().test_connection(destination)
  end

  @spec get_metadata(KafkaDestination.t()) :: {:ok, any()} | {:error, Error.t()}
  def get_metadata(%KafkaDestination{} = destination) do
    impl().get_metadata(destination)
  end

  defp impl do
    Application.get_env(:sequin, :kafka_module, Sequin.Kafka.Client)
  end
end
