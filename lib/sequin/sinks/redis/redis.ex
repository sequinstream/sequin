defmodule Sequin.Sinks.Redis do
  @moduledoc false
  import Sequin.Consumers.Guards, only: [is_redis_sink: 1]

  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  @callback send_messages(SinkConsumer.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}

  @callback set_messages(RedisStringSink.t(), [RoutedMessage.t()]) :: :ok | {:error, Error.t()}
  @callback message_count(RedisStreamSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback client_info(RedisStreamSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  @callback test_connection(RedisStreamSink.t()) :: :ok | {:error, Error.t()}

  @spec send_messages(SinkConsumer.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
          :ok | {:error, Error.t()}
  def send_messages(%SinkConsumer{} = consumer, messages) do
    impl().send_messages(consumer, messages)
  end

  @spec set_messages(RedisStringSink.t(), [%{key: String.t(), value: String.t()}]) ::
          :ok | {:error, Error.t()}
  def set_messages(%RedisStringSink{} = sink, messages) do
    impl().set_messages(sink, messages)
  end

  @spec message_count(RedisStreamSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def message_count(%RedisStreamSink{} = sink) do
    impl().message_count(sink)
  end

  @spec client_info(RedisStreamSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def client_info(redis_sink) when is_redis_sink(redis_sink) do
    impl().client_info(redis_sink)
  end

  @spec test_connection(RedisStreamSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(redis_sink) when is_redis_sink(redis_sink) do
    impl().test_connection(redis_sink)
  end

  defp impl do
    Application.get_env(:sequin, :redis_module, Sequin.Sinks.Redis.Client)
  end
end
