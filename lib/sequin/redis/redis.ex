defmodule Sequin.Redis do
  @moduledoc false
  alias Sequin.Consumers.RedisDestination
  alias Sequin.Error

  @callback send_messages(%RedisDestination{}, [any()]) :: :ok | {:error, Error.t()}

  @spec send_messages(%RedisDestination{}, [any()]) :: :ok | {:error, Error.t()}
  def send_messages(%RedisDestination{} = destination, messages) do
    impl().send_messages(destination, messages)
  end

  defp impl do
    Application.get_env(:sequin, :redis_module, Sequin.Redis.Client)
  end
end
