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
  def exponential_backoff(base \\ :timer.seconds(1), count \\ 0, max \\ :timer.minutes(3)) do
    backoff = if count <= 1, do: base, else: base * :math.pow(2, count - 1)
    trunc(min(backoff, max))
  end
end
