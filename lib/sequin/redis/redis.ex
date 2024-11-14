defmodule Sequin.Redis do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisDestination
  alias Sequin.Error

  @callback send_messages(RedisDestination.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback message_count(RedisDestination.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback client_info(RedisDestination.t()) :: {:ok, String.t()} | {:error, Error.t()}
  @callback test_connection(RedisDestination.t()) :: :ok | {:error, Error.t()}

  @spec send_messages(RedisDestination.t(), [any()]) :: :ok | {:error, Error.t()}
  def send_messages(%RedisDestination{} = destination, messages) do
    impl().send_messages(destination, messages)
  end

  @spec message_count(RedisDestination.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def message_count(%RedisDestination{} = destination) do
    impl().message_count(destination)
  end

  @spec client_info(RedisDestination.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def client_info(%RedisDestination{} = destination) do
    impl().client_info(destination)
  end

  @spec test_connection(RedisDestination.t()) :: :ok | {:error, Error.t()}
  def test_connection(%RedisDestination{} = destination) do
    impl().test_connection(destination)
  end

  defp impl do
    Application.get_env(:sequin, :redis_module, Sequin.Redis.Client)
  end
end
