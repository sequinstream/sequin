defmodule Sequin.Redis do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisSink
  alias Sequin.Error

  @callback send_messages(RedisSink.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback message_count(RedisSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback client_info(RedisSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  @callback test_connection(RedisSink.t()) :: :ok | {:error, Error.t()}

  @spec send_messages(RedisSink.t(), [any()]) :: :ok | {:error, Error.t()}
  def send_messages(%RedisSink{} = sink, messages) do
    impl().send_messages(sink, messages)
  end

  @spec message_count(RedisSink.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def message_count(%RedisSink{} = sink) do
    impl().message_count(sink)
  end

  @spec client_info(RedisSink.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def client_info(%RedisSink{} = sink) do
    impl().client_info(sink)
  end

  @spec test_connection(RedisSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%RedisSink{} = sink) do
    impl().test_connection(sink)
  end

  defp impl do
    Application.get_env(:sequin, :redis_module, Sequin.Redis.Client)
  end
end
