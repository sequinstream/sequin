defmodule Sequin.Sinks.Redis do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  @callback send_messages(SinkConsumer.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback message_count(RedisStreamSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback client_info(RedisStreamSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  @callback test_connection(RedisStreamSink.t()) :: :ok | {:error, Error.t()}

  @spec send_messages(SinkConsumer.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
          :ok | {:error, Error.t()}
  def send_messages(%SinkConsumer{} = consumer, messages) do
    impl().send_messages(consumer, messages)
  end

  @spec message_count(RedisStreamSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def message_count(%RedisStreamSink{} = sink) do
    impl().message_count(sink)
  end

  @spec client_info(RedisStreamSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def client_info(%RedisStreamSink{} = sink) do
    impl().client_info(sink)
  end

  @spec test_connection(RedisStreamSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%RedisStreamSink{} = sink) do
    impl().test_connection(sink)
  end

  defp impl do
    Application.get_env(:sequin, :redis_module, Sequin.Sinks.Redis.Client)
  end
end
