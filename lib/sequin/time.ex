defmodule Sequin.Time do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Error.ValidationError

  @doc """
  Parse an ISO-8601 formatted timestamp string.

  Tries to parse directly as a `DateTime` first, but if that fails due to a
  missing TZ offset, then try parsing as a `NaiveDateTime` instead, then
  converting to a UTC `DateTime`.

  If you know for sure that your timestamp will have the trailing `Z` offset,
  then call `DateTime.from_iso8601/1` directly instead.
  """
  @spec parse_timestamp!(String.t()) :: DateTime.t()
  def parse_timestamp!(str) do
    case parse_timestamp(str) do
      {:ok, datetime} -> datetime
      {:error, error} -> raise error
    end
  end

  @spec parse_timestamp(String.t()) ::
          {:ok, DateTime.t()} | {:error, ValidationError.t()}
  def parse_timestamp(str) do
    case DateTime.from_iso8601(str) do
      {:ok, timestamp, _offset} ->
        {:ok, timestamp}

      {:error, :missing_offset} ->
        with {:ok, naive_datetime} <- NaiveDateTime.from_iso8601(str),
             {:ok, datetime} <- DateTime.from_naive(naive_datetime, "Etc/UTC") do
          {:ok, datetime}
        else
          {:error, reason} ->
            {:error,
             Error.validation(
               summary: "Str value is not a valid naive ISO-8601 timestamp",
               errors: %{str: ["#{inspect(reason, pretty: true)}"]}
             )}
        end

      {:error, reason} ->
        {:error,
         Error.validation(
           summary: "Str value is not a valid ISO-8601 timestamp",
           errors: %{str: ["#{inspect(reason, pretty: true)}"]}
         )}
    end
  end

  def parse_date!(str) do
    case Date.from_iso8601(str) do
      {:ok, date} -> date
      {:error, error} -> raise error
    end
  end

  def before_ms_ago?(timestamp, ms) do
    DateTime.before?(timestamp, DateTime.add(Sequin.utc_now(), -ms, :millisecond))
  end

  def before_sec_ago?(timestamp, seconds) do
    DateTime.before?(timestamp, DateTime.add(Sequin.utc_now(), -seconds, :second))
  end

  def before_min_ago?(timestamp, minutes) do
    DateTime.before?(timestamp, DateTime.add(Sequin.utc_now(), -minutes, :minute))
  end

  def after_ms_ago?(timestamp, ms) do
    DateTime.after?(timestamp, DateTime.add(Sequin.utc_now(), -ms, :millisecond))
  end

  def after_sec_ago?(timestamp, seconds) do
    DateTime.after?(timestamp, DateTime.add(Sequin.utc_now(), -seconds, :second))
  end

  def after_min_ago?(timestamp, minutes) do
    DateTime.after?(timestamp, DateTime.add(Sequin.utc_now(), -minutes, :minute))
  end

  @doc """
  Calculates exponential backoff time in milliseconds.

  ## Parameters
    - base: The base retry time in milliseconds (default: 1000)
    - count: The number of failed attempts (default: 0)
    - max: The maximum backoff time in milliseconds (default: 5 minutes)

  ## Examples
      iex> Sequin.Time.exponential_backoff(1000, 0)
      1000
      iex> Sequin.Time.exponential_backoff(1000, 3)
      8000
      iex> Sequin.Time.exponential_backoff(1000, 10, 60_000)
      60000
  """
  @spec exponential_backoff(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def exponential_backoff(base \\ to_timeout(second: 1), count \\ 0, max \\ to_timeout(minute: 3)) do
    max_count = trunc(:math.log2(max / base) + 1)

    if count >= max_count do
      max
    else
      backoff = if count <= 1, do: base, else: base * :math.pow(2, count - 1)
      # Random value between 0.85 and 1.15
      jitter = :rand.uniform() * 0.3 + 0.85
      trunc(backoff * jitter)
    end
  end

  @doc """
  Parse a duration string into milliseconds.
  Supports formats like "60s", "5m", "100ms", or "1000" (assumed milliseconds)

  ## Examples
      iex> Sequin.Time.parse_duration("60s")
      {:ok, 60000}
      iex> Sequin.Time.parse_duration("5m")
      {:ok, 300000}
      iex> Sequin.Time.parse_duration("100ms")
      {:ok, 100}
      iex> Sequin.Time.parse_duration("1000")
      {:ok, 1000}
      iex> Sequin.Time.parse_duration("invalid")
      {:error, %Sequin.Error.InvariantError{message: "Invalid duration format"}}
  """
  @spec parse_duration(String.t()) :: {:ok, non_neg_integer()} | {:error, Error.InvariantError.t()}
  def parse_duration(str) when is_binary(str) do
    case Regex.run(~r/^(\d+)(ms|s|m)?$/, str, capture: :all_but_first) do
      [number, unit] ->
        case Integer.parse(number) do
          {num, ""} -> {:ok, convert_to_ms(num, unit)}
          _ -> {:error, Error.invariant(message: "Invalid duration format")}
        end

      [number] ->
        case Integer.parse(number) do
          {num, ""} -> {:ok, num}
          _ -> {:error, Error.invariant(message: "Invalid duration format")}
        end

      _ ->
        {:error, Error.invariant(message: "Invalid duration format")}
    end
  end

  def parse_duration(num) when is_integer(num), do: {:ok, num}

  def parse_duration(_), do: {:error, Error.invariant(message: "Invalid duration format")}

  defp convert_to_ms(num, "ms"), do: num
  defp convert_to_ms(num, "s"), do: num * 1000
  defp convert_to_ms(num, "m"), do: num * 60 * 1000

  @doc """
  Calculate the difference in milliseconds between two timestamps.

  ## Parameters
    - start: The starting timestamp
    - finish: The ending timestamp (defaults to now)

  ## Examples
      iex> start = ~N[2023-01-01 00:00:00]
      iex> finish = ~N[2023-01-01 00:00:01]
      iex> Sequin.Time.ms_since(start, finish)
      1000
  """
  @spec ms_since(DateTime.t() | NaiveDateTime.t(), DateTime.t() | NaiveDateTime.t() | nil) :: integer()
  def ms_since(start, finish \\ nil)

  def ms_since(%DateTime{} = start, %DateTime{} = finish) do
    DateTime.diff(finish, start, :millisecond)
  end

  def ms_since(%DateTime{} = start, nil) do
    ms_since(start, DateTime.utc_now())
  end

  def ms_since(%NaiveDateTime{} = start, %NaiveDateTime{} = finish) do
    NaiveDateTime.diff(finish, start, :millisecond)
  end

  def ms_since(%NaiveDateTime{} = start, nil) do
    ms_since(start, NaiveDateTime.utc_now())
  end

  @doc """
  Convert microseconds since 2000-01-01 to milliseconds difference from now.

  This is primarily used for Postgres replication protocol timestamps.

  ## Parameters
    - microseconds_since_2000: Microseconds since 2000-01-01 00:00:00 UTC

  ## Returns
    Millisecond difference between the timestamp and now (positive if timestamp is in future)
  """
  @spec microseconds_since_2000_to_ms_since_now(integer()) :: integer()
  def microseconds_since_2000_to_ms_since_now(microseconds_since_2000) do
    # Define the epoch (2000-01-01 midnight in UTC)
    {:ok, epoch_2000} = NaiveDateTime.new(2000, 1, 1, 0, 0, 0)
    epoch_2000_in_microseconds = NaiveDateTime.diff(epoch_2000, ~N[1970-01-01 00:00:00], :microsecond)

    # Convert microseconds since 2000 to microseconds since Unix epoch
    unix_microseconds = epoch_2000_in_microseconds + microseconds_since_2000

    # Convert to DateTime
    timestamp = DateTime.from_unix!(unix_microseconds, :microsecond)

    # Calculate difference in milliseconds
    ms_since(timestamp, DateTime.utc_now())
  end

  @doc """
  Adds random jitter to a time value.

  ## Parameters
    - value: The base time value in milliseconds
    - factor: The jitter factor (default: 0.15, meaning ±15%)

  ## Examples
      iex> Sequin.Time.with_jitter(1000)
      # Returns a value between 850 and 1150

      iex> Sequin.Time.with_jitter(1000, 0.5)
      # Returns a value between 500 and 1500
  """
  @spec with_jitter(non_neg_integer(), float()) :: non_neg_integer()
  def with_jitter(value, factor \\ 0.15) when is_integer(value) and value >= 0 and is_float(factor) and factor >= 0 do
    # Calculate the jitter range (factor * 2 gives the full range)
    jitter_range = value * factor * 2

    # Generate a random value within the jitter range
    random_jitter = :rand.uniform() * jitter_range

    # Apply the jitter by adding the random value and subtracting half the range
    # This gives a value between (value - value*factor) and (value + value*factor)
    trunc(value + random_jitter - jitter_range / 2)
  end
end
