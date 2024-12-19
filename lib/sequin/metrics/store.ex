defmodule Sequin.Metrics.Store do
  @moduledoc false

  alias Sequin.Error

  # Count functions
  def incr_count(key, amount \\ 1) do
    ["INCRBY", "metrics:count:#{key}", amount]
    |> RedixCluster.command()
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_count(key) do
    ["GET", "metrics:count:#{key}"]
    |> RedixCluster.command()
    |> handle_response()
    |> case do
      {:ok, nil} -> {:ok, 0}
      {:ok, value} -> {:ok, String.to_integer(value)}
      error -> error
    end
  end

  # Average functions
  def incr_avg(key, value) do
    [
      ["HINCRBY", "metrics:avg:#{key}", "total", round(value)],
      ["HINCRBY", "metrics:avg:#{key}", "count", 1]
    ]
    |> RedixCluster.pipeline()
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_avg(key) do
    ["HMGET", "metrics:avg:#{key}", "total", "count"]
    |> RedixCluster.command()
    |> handle_response()
    |> case do
      {:ok, [total, count]} when is_binary(total) and is_binary(count) ->
        {:ok, String.to_integer(total) / String.to_integer(count)}

      {:ok, _} ->
        {:ok, nil}

      error ->
        error
    end
  end

  # Throughput functions
  def incr_throughput(key) do
    now = :os.system_time(:nanosecond)

    # ms to nano
    one_hour_ago = now - :timer.seconds(3600) * 1_000_000

    [
      ["ZADD", "metrics:throughput:#{key}", now, now],
      ["ZREMRANGEBYSCORE", "metrics:throughput:#{key}", "-inf", one_hour_ago],
      ["ZREMRANGEBYRANK", "metrics:throughput:#{key}", "0", "-10001"],
      ["ZCARD", "metrics:throughput:#{key}"]
    ]
    |> RedixCluster.pipeline()
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_throughput(key) do
    now = :os.system_time(:nanosecond)

    # ms to nano
    one_hour_ago = now - :timer.seconds(3600) * 1_000_000

    [
      ["ZREVRANGEBYSCORE", "metrics:throughput:#{key}", "-inf", one_hour_ago],
      ["ZRANGE", "metrics:throughput:#{key}", 0, 0, "WITHSCORES"],
      ["ZCARD", "metrics:throughput:#{key}"]
    ]
    |> RedixCluster.pipeline()
    |> handle_response()
    |> case do
      {:ok, [_, [], _]} ->
        {:ok, 0.0}

      {:ok, [_, [_oldest, oldest_score], count]} ->
        {decimal_oldest, ""} = Decimal.parse(oldest_score)
        oldest = Decimal.to_integer(decimal_oldest)

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

  defp handle_response({:error, error}) when is_exception(error) do
    {:error, Error.service(service: :redis, message: Exception.message(error))}
  end

  defp handle_response({:error, error}) do
    {:error, Error.service(service: :redis, message: "Redis error: #{inspect(error)}")}
  end
end
