defmodule Sequin.Sinks.Kafka do
  @moduledoc false
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  @callback publish(SinkConsumer.t(), ConsumerRecord.t() | ConsumerEvent.t()) :: :ok | {:error, Error.t()}
  @callback publish(SinkConsumer.t(), integer(), [ConsumerRecord.t() | ConsumerEvent.t()]) :: :ok | {:error, Error.t()}
  @callback test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  @callback get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  @callback get_partition_count(KafkaSink.t()) :: {:ok, integer()} | {:error, Error.t()}

  @spec publish(SinkConsumer.t(), ConsumerRecord.t() | ConsumerEvent.t()) ::
          :ok | {:error, Error.t()}
  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, %ConsumerRecord{} = record) do
    impl().publish(consumer, record)
  end

  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, %ConsumerEvent{} = event) do
    impl().publish(consumer, event)
  end

  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, partition, messages) when is_list(messages) do
    # impl().publish(consumer, partition, messages)
    :ok
  end

  @spec test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%KafkaSink{} = sink) do
    impl().test_connection(sink)
  end

  @spec get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  def get_metadata(%KafkaSink{} = sink) do
    impl().get_metadata(sink)
  end

  @spec get_partition_count(KafkaSink.t()) :: {:ok, integer()} | {:error, Error.t()}
  def get_partition_count(%KafkaSink{} = sink) do
    impl().get_partition_count(sink)
  end

  @spec message_key(SinkConsumer.t(), ConsumerRecord.t() | ConsumerEvent.t()) :: String.t()
  def message_key(%SinkConsumer{sink: %KafkaSink{}} = consumer, %ConsumerRecord{} = record) do
    consumer
    |> Consumers.group_column_values(record.data)
    |> Enum.join(":")
  end

  def message_key(%SinkConsumer{sink: %KafkaSink{}} = consumer, %ConsumerEvent{} = event) do
    consumer
    |> Consumers.group_column_values(event.data)
    |> Enum.join(":")
  end

  defp impl do
    Application.get_env(:sequin, :kafka_module, Sequin.Sinks.Kafka.Client)
  end
end
