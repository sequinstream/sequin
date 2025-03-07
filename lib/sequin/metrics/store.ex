defmodule Sequin.Metrics.Store do
  @moduledoc false

  alias Sequin.Redis

  # Count functions
  def incr_count(key, amount \\ 1) do
    case Redis.command(["INCRBY", "metrics:count:#{key}", amount]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def get_count(key) do
    case Redis.command(["GET", "metrics:count:#{key}"]) do
      {:ok, nil} -> {:ok, 0}
      {:ok, value} -> {:ok, String.to_integer(value)}
      {:error, error} -> {:error, error}
    end
  end

  def reset_count(key) do
    case Redis.command(["DEL", "metrics:count:#{key}"]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  # Throughput functions
  # 70 seconds of throughput telemetry is stored; 5 seconds of read telemetry is read for "instant" throughput
  # We store more than 60 seconds so we can do smoothing and then take 60 seconds of smoothed data
  @timeseries_windows 70
  def incr_throughput(key, count \\ 1) do
    now = :os.system_time(:second)

    [
      ["INCRBY", "metrics:throughput:#{key}:#{now}", count],
      ["EXPIRE", "metrics:throughput:#{key}:#{now}", @timeseries_windows + 1]
    ]
    |> Redis.pipeline()
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @instant_throughput_window 5
  def get_throughput(key) do
    now = :os.system_time(:second)
    buckets = Enum.to_list((now - @instant_throughput_window + 1)..now)
    commands = Enum.map(buckets, &["GET", "metrics:throughput:#{key}:#{&1}"])

    case Redis.pipeline(commands) do
      {:ok, results} ->
        sum =
          results
          |> Stream.map(&String.to_integer(&1 || "0"))
          |> Enum.sum()

        {:ok, sum / @instant_throughput_window}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec get_throughput_timeseries(String.t(), non_neg_integer()) :: {:ok, [non_neg_integer()]} | {:error, any()}
  def get_throughput_timeseries(key, window_count \\ @timeseries_windows)

  def get_throughput_timeseries(_key, window_count) when window_count > @timeseries_windows do
    raise "Window count #{window_count} is greater than the maximum window size of #{@timeseries_windows}"
  end

  def get_throughput_timeseries(key, window_count) do
    now = :os.system_time(:second)
    most_recent_full_window = now - 1
    buckets = Enum.to_list((most_recent_full_window - window_count + 1)..most_recent_full_window)
    commands = Enum.map(buckets, &["GET", "metrics:throughput:#{key}:#{&1}"])

    case Redis.pipeline(commands) do
      {:ok, results} ->
        {:ok, Enum.map(results, &String.to_integer(&1 || "0"))}

      {:error, error} ->
        {:error, error}
    end
  end

  # Latency functions
  @latency_windows 5
  def measure_latency(key, value) do
    [
      ["RPUSH", "metrics:latency:#{key}", value],
      ["LTRIM", "metrics:latency:#{key}", -@latency_windows, -1]
    ]
    |> Redis.pipeline()
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def get_latency(key) do
    case Redis.command(["LRANGE", "metrics:latency:#{key}", "0", "-1"]) do
      {:ok, values} ->
        values =
          Enum.map(values, fn str ->
            case Float.parse(str) do
              {float, _} -> float
              :error -> String.to_integer(str) * 1.0
            end
          end)

        case values do
          [] -> {:ok, nil}
          vals -> {:ok, Enum.sum(vals) / length(vals)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  def measure_gauge(key, value) do
    case Redis.command(["SET", "metrics:gauge:#{key}", value]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def get_gauge(key) do
    case Redis.command(["GET", "metrics:gauge:#{key}"]) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} -> {:ok, String.to_integer(value)}
      {:error, error} -> {:error, error}
    end
  end
end
