defmodule Sequin.Metrics.Store do
  @moduledoc false

  alias Sequin.Error

  # Count functions
  def incr_count(key, amount \\ 1) do
    :redix
    |> Redix.command(["INCRBY", "metrics:count:#{key}", amount])
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_count(key) do
    :redix
    |> Redix.command(["GET", "metrics:count:#{key}"])
    |> handle_response()
    |> case do
      {:ok, nil} -> {:ok, 0}
      {:ok, value} -> {:ok, String.to_integer(value)}
      error -> error
    end
  end

  # Average functions
  def incr_avg(key, value) do
    :redix
    |> Redix.pipeline([
      ["HINCRBY", "metrics:avg:#{key}", "total", round(value)],
      ["HINCRBY", "metrics:avg:#{key}", "count", 1]
    ])
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_avg(key) do
    :redix
    |> Redix.command(["HMGET", "metrics:avg:#{key}", "total", "count"])
    |> handle_response()
    |> case do
      {:ok, [total, count]} when is_binary(total) and is_binary(count) ->
        {:ok, String.to_integer(total) / String.to_integer(count)}

      {:ok, _} ->
        {:ok, 0}

      error ->
        error
    end
  end

  # Throughput functions
  def incr_throughput(key) do
    now = :os.system_time(:nanosecond)
    one_hour_ago = now - :timer.seconds(3600) * 1_000_000_000

    :redix
    |> Redix.pipeline([
      ["ZADD", "metrics:throughput:#{key}", now, now],
      ["ZREMRANGEBYSCORE", "metrics:throughput:#{key}", "-inf", one_hour_ago],
      ["ZCARD", "metrics:throughput:#{key}"]
    ])
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_throughput(key) do
    now = :os.system_time(:nanosecond)

    :redix
    |> Redix.pipeline([
      ["ZRANGE", "metrics:throughput:#{key}", 0, 0, "WITHSCORES"],
      ["ZCARD", "metrics:throughput:#{key}"]
    ])
    |> handle_response()
    |> case do
      {:ok, [[], _]} ->
        {:ok, 0.0}

      {:ok, [[_oldest, oldest_score], count]} ->
        oldest = String.to_integer(oldest_score)
        time_diff = max(now - oldest, 1)

        # Nanoseconds to seconds
        time_diff_seconds = time_diff / 1_000_000_000

        # Require at least 60 seconds to avoid spikes
        time_diff_seconds = max(time_diff_seconds, 60)

        {:ok, count / time_diff_seconds}

      error ->
        error
    end
  end

  @spec handle_response(any()) :: {:ok, any()} | {:error, Error.t()}
  defp handle_response({:ok, response}), do: {:ok, response}
  defp handle_response({:error, error}), do: {:error, Error.service(entity: :redis, details: %{error: error})}
end
