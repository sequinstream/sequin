defmodule Sequin.Sinks.Kafka do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  @module Application.compile_env(:sequin, :kafka_module, Sequin.Sinks.Kafka.Client)

  @callback publish(SinkConsumer.t(), String.t(), ConsumerRecord.t() | ConsumerEvent.t()) ::
              :ok | {:error, Error.t()}
  @callback publish(SinkConsumer.t(), String.t(), integer(), [ConsumerRecord.t() | ConsumerEvent.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  @callback get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  @callback get_partition_count(KafkaSink.t(), String.t()) :: {:ok, integer()} | {:error, Error.t()}

  @spec publish(SinkConsumer.t(), String.t(), ConsumerRecord.t() | ConsumerEvent.t()) ::
          :ok | {:error, Error.t()}
  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, topic, %ConsumerRecord{} = record) do
    @module.publish(consumer, topic, record)
  end

  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, topic, %ConsumerEvent{} = event) do
    @module.publish(consumer, topic, event)
  end

  def publish(%SinkConsumer{sink: %KafkaSink{}} = consumer, topic, partition, messages) when is_list(messages) do
    @module.publish(consumer, topic, partition, messages)
  end

  @spec test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%KafkaSink{} = sink) do
    @module.test_connection(sink)
  end

  @spec get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  def get_metadata(%KafkaSink{} = sink) do
    @module.get_metadata(sink)
  end

  @spec get_partition_count(KafkaSink.t(), String.t()) :: {:ok, integer()} | {:error, Error.t()}
  def get_partition_count(%KafkaSink{} = sink, topic) do
    @module.get_partition_count(sink, topic)
  end

  @spec message_key(SinkConsumer.t(), ConsumerRecord.t() | ConsumerEvent.t()) :: String.t()
  def message_key(%SinkConsumer{sink: %KafkaSink{}}, %ConsumerRecord{} = record) do
    record.group_id
  end

  def message_key(%SinkConsumer{sink: %KafkaSink{}}, %ConsumerEvent{} = event) do
    event.group_id
  end
end
