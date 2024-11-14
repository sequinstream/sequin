defmodule Sequin.Redis.Client do
  @moduledoc false
  @behaviour Sequin.Redis

  alias Sequin.Consumers.RedisDestination
  alias Sequin.Error

  @spec send_messages(%RedisDestination{}, [any()]) :: :ok | {:error, Error.t()}
  def send_messages(%RedisDestination{} = _destination, _messages) do
    :ok
  end
end
