defmodule Sequin.Mutex do
  @moduledoc false
  require Logger

  @type acquire_return :: :ok | {:error, :mutex_taken} | :error
  @spec acquire_or_touch(key :: String.t(), token :: String.t(), expiry :: non_neg_integer()) ::
          acquire_return()

  @doc """
  Call acquire_or_touch/3 with a key and a unique token. If you're able to lock that Redis key,
  returns :ok. If the key is already locked (the key already exists with another token),
  returns {:error, :mutex_taken}
  """
  def acquire_or_touch(key, token, expiry) do
    case Redix.command(:redix, ["EVAL", acquire_or_touch_script(), 1, key, token, expiry]) do
      {:ok, ^token} ->
        :ok

      {:ok, _} ->
        {:error, :mutex_taken}

      err ->
        Logger.error("Redis error while trying to acquire mutex: #{inspect(err)}", error: err)
        :error
    end
  end

  @doc """
  Call release/2 with a key and a unique token. If you own the Redis key, releases the mutex and
  returns :ok. Otherwise, does not touch the mutex and instead returns {:error, :mutex_taken}.
  """
  def release(key, token) do
    case Redix.command(:redix, ["EVAL", release_script(), 1, key, token]) do
      {:ok, 1} ->
        :ok

      {:ok, 0} ->
        {:error, :mutex_taken}

      err ->
        Logger.error("Redis error while trying to release mutex: #{inspect(err)}", error: err)
        :error
    end
  end

  defp acquire_or_touch_script do
    """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      redis.call("PEXPIRE", KEYS[1], ARGV[2])
      return ARGV[1]
    elseif redis.call("EXISTS", KEYS[1]) == 0 then
      redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
      return ARGV[1]
    else
      return redis.call("GET", KEYS[1])
    end
    """
  end

  defp release_script do
    """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
    """
  end
end
