defmodule Sequin.Kafka do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Error

  @callback publish(KafkaSink.t(), ConsumerRecord.t() | ConsumerEvent.t()) :: :ok | {:error, Error.t()}
  @callback test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  @callback get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}

  @spec publish(KafkaSink.t(), ConsumerRecord.t() | ConsumerEvent.t()) :: :ok | {:error, Error.t()}
  def publish(%KafkaSink{} = sink, %ConsumerRecord{} = record) do
    impl().publish(sink, record)
  end

  def publish(%KafkaSink{} = sink, %ConsumerEvent{} = event) do
    impl().publish(sink, event)
  end

  @spec test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%KafkaSink{} = sink) do
    impl().test_connection(sink)
  end

  @spec get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  def get_metadata(%KafkaSink{} = sink) do
    impl().get_metadata(sink)
  end

  defp impl do
    Application.get_env(:sequin, :kafka_module, Sequin.Kafka.Client)
  end
end
