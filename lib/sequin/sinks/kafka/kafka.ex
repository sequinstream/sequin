defmodule Sequin.Sinks.Kafka do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  defmodule Message do
    @moduledoc "A message to be published to Kafka"
    use TypedStruct

    typedstruct do
      field :key, :string
      field :value, :string
    end
  end

  @module Application.compile_env(:sequin, :kafka_module, Sequin.Sinks.Kafka.Client)

  @callback publish(SinkConsumer.t(), String.t(), integer(), [Message.t()]) :: :ok | {:error, Error.t()}
  @callback test_connection(KafkaSink.t()) :: :ok | {:error, Error.t()}
  @callback get_metadata(KafkaSink.t()) :: {:ok, any()} | {:error, Error.t()}
  @callback get_partition_count(KafkaSink.t(), String.t()) :: {:ok, integer()} | {:error, Error.t()}

  @spec publish(SinkConsumer.t(), String.t(), integer(), [Message.t()]) :: :ok | {:error, Error.t()}
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

  @spec message_key(SinkConsumer.t(), ConsumerEvent.t()) :: String.t()

  def message_key(%SinkConsumer{sink: %KafkaSink{}}, %ConsumerEvent{} = event) do
    event.group_id || ""
  end
end
