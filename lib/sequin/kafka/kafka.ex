defmodule Sequin.Kafka do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.KafkaDestination
  alias Sequin.Error

  @callback publish(KafkaDestination.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(KafkaDestination.t()) :: :ok | {:error, Error.t()}

  @spec publish(KafkaDestination.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
          :ok | {:error, Error.t()}
  def publish(%KafkaDestination{} = destination, messages) do
    impl().send_messages(destination, messages)
  end

  @spec test_connection(KafkaDestination.t()) :: :ok | {:error, Error.t()}
  def test_connection(%KafkaDestination{} = destination) do
    impl().test_connection(destination)
  end

  defp impl do
    Application.get_env(:sequin, :kafka_module, Sequin.Kafka.Client)
  end
end
