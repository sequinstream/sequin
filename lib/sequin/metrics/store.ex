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
        {:ok, nil}

      error ->
        error
    end
  end

  # Throughput functions
  def incr_throughput(key) do
    window = get_current_window()
    window_key = "metrics:throughput:#{key}:window:#{window}"

    :redix
    |> Redix.pipeline([
      ["INCR", window_key],
      # 10 minute TTL
      ["EXPIRE", window_key, 600]
    ])
    |> handle_response()
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_throughput(key) do
    current_window = get_current_window()
    window_keys = for i <- 0..9, do: "metrics:throughput:#{key}:window:#{current_window - i}"

    :redix
    |> Redix.command(["MGET"] ++ window_keys)
    |> handle_response()
    |> case do
      {:ok, counts} ->
        counts_with_weights = Enum.zip(counts, weighted_factors())
        weighted_sum = calculate_weighted_sum(counts_with_weights)
        # Convert to per-second rate
        {:ok, weighted_sum / 60.0}

      error ->
        error
    end
  end

  # Private helpers
  defp get_current_window do
    :second |> System.system_time() |> div(60)
  end

  defp weighted_factors do
    # More recent windows have higher weights
    [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
  end

  defp calculate_weighted_sum(counts_with_weights) do
    Enum.reduce(counts_with_weights, 0, fn
      {nil, _weight}, acc -> acc
      {count, weight}, acc -> acc + String.to_integer(count) * weight
    end)
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
