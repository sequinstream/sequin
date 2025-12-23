defmodule Sequin.Postgres.ValueCaster do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Postgres.ArrayLexer

  require Logger

  @spec cast(type :: String.t(), value :: any()) :: {:ok, any()} | {:error, Error.t()}
  def cast("bool", "t"), do: {:ok, true}
  def cast("bool", "f"), do: {:ok, false}

  def cast("_" <> _type, "{}"), do: {:ok, []}

  def cast("_" <> type, array_string) when is_binary(array_string) do
    with {:ok, uncasted_list} <- ArrayLexer.lex(array_string) do
      cast(type, uncasted_list)
    end
  end

  def cast(type, value) when type in ["json", "jsonb"] and is_binary(value) do
    case Jason.decode(value) do
      {:ok, json} ->
        {:ok, json}

      {:error, %Jason.DecodeError{} = error} ->
        Logger.error("Failed to decode JSON value: #{inspect(error, limit: 10_000)}")

        wrapped_error =
          Error.service(
            service: :postgres_replication_slot,
            message: "Failed to decode JSON value: #{inspect(error)}",
            details: %{error: Exception.message(error)},
            code: :invalid_json
          )

        {:error, wrapped_error}
    end
  end

  def cast("vector", nil), do: {:ok, nil}

  def cast("interval", value_str) when is_binary(value_str) do
    {:ok, parse_interval(value_str)}
  end

  def cast("vector", value_str) when is_binary(value_str) do
    list =
      value_str
      |> String.trim("[")
      |> String.trim("]")
      |> String.split(",")
      |> Enum.map(fn num ->
        {float, ""} = Float.parse(num)
        float
      end)

    {:ok, list}
  end

  def cast(type, value) when is_list(value) do
    res =
      Enum.reduce_while(value, {:ok, []}, fn elem, {:ok, acc} ->
        case cast(type, elem) do
          {:ok, casted_elem} -> {:cont, {:ok, [casted_elem | acc]}}
          error -> {:halt, error}
        end
      end)

    case res do
      {:ok, res} -> {:ok, Enum.reverse(res)}
      error -> error
    end
  end

  def cast(type, value) do
    case Ecto.Type.cast(string_to_ecto_type(type), value) do
      {:ok, casted_value} -> {:ok, casted_value}
      # Fallback to original value if casting fails
      :error -> {:ok, value}
    end
  end

  @postgres_to_ecto_type_mapping %{
    # Numeric Types
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "money" => :decimal,
    # Character Types
    "char" => :string,
    "varchar" => :string,
    "text" => :string,
    # Binary Data Types
    "bytea" => :binary,
    # Date/Time Types
    "timestamp" => :naive_datetime_usec,
    "timestamptz" => :utc_datetime_usec,
    "date" => :date,
    "time" => :time,
    "timetz" => :time,
    # Ecto doesn't have a direct interval type
    "interval" => :map,
    # Boolean Type
    "bool" => :boolean,
    # Geometric Types
    "point" => {:array, :float},
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    # Network Address Types
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    # Bit String Types
    "bit" => :string,
    "bit_varying" => :string,
    # Text Search Types
    "tsvector" => :string,
    "tsquery" => :string,
    # UUID Type
    "uuid" => Ecto.UUID,
    # XML Type
    "xml" => :string,
    # JSON Types
    "json" => :map,
    "jsonb" => :map,
    # Arrays
    "_text" => {:array, :string},
    # Composite Types
    "composite" => :map,
    # Range Types
    "range" => {:array, :any},
    # Domain Types
    "domain" => :any,
    # Object Identifier Types
    "oid" => :integer,
    # pg_lsn Type
    "pg_lsn" => :string,
    # Pseudotypes
    "any" => :any
  }

  defp string_to_ecto_type(type) do
    Map.get(@postgres_to_ecto_type_mapping, type, :string)
  end

  defp parse_interval(str) do
    # Split into date parts and time part
    {date_part, time_part} = split_date_and_time(str)

    # Parse date components (years, months, days)
    {months, days} = parse_date_components(date_part)

    # Parse time component (HH:MM:SS.microseconds)
    microseconds = parse_time_component(time_part)

    %{
      "months" => months,
      "days" => days,
      "microseconds" => microseconds
    }
  end

  defp split_date_and_time(str) do
    # Time component matches patterns like "04:05:06" or "-04:05:06" or "04:05:06.789"
    case Regex.run(~r/(-?\d{2}:\d{2}:\d{2}(?:\.\d+)?)$/, str) do
      [time_match | _] ->
        date_part = String.trim(String.replace(str, time_match, ""))
        {date_part, time_match}

      nil ->
        {str, nil}
    end
  end

  defp parse_date_components(date_part) when is_binary(date_part) do
    parts = String.split(date_part, ~r/\s+/, trim: true)
    parse_date_parts(parts, 0, 0)
  end

  defp parse_date_components(nil), do: {0, 0}

  defp parse_date_parts([], months, days), do: {months, days}

  defp parse_date_parts([value, unit | rest], months, days) do
    {num, _} = Integer.parse(value)

    case String.downcase(unit) do
      u when u in ["year", "years"] ->
        parse_date_parts(rest, months + num * 12, days)

      u when u in ["mon", "mons", "month", "months"] ->
        parse_date_parts(rest, months + num, days)

      u when u in ["day", "days"] ->
        parse_date_parts(rest, months, days + num)

      # Skip unknown units
      _ ->
        parse_date_parts(rest, months, days)
    end
  end

  defp parse_date_parts([_single], months, days), do: {months, days}

  defp parse_time_component(nil), do: 0

  defp parse_time_component(time_str) do
    # Handle negative time (e.g., "-04:05:06")
    {sign, time_str} =
      if String.starts_with?(time_str, "-") do
        {-1, String.trim_leading(time_str, "-")}
      else
        {1, time_str}
      end

    # Parse HH:MM:SS.microseconds
    case String.split(time_str, ":") do
      [hours_str, minutes_str, seconds_str] ->
        {hours, _} = Integer.parse(hours_str)
        {minutes, _} = Integer.parse(minutes_str)
        {seconds, microsecs} = parse_seconds(seconds_str)

        total_microsecs = (hours * 3600 + minutes * 60 + seconds) * 1_000_000 + microsecs
        sign * total_microsecs

      _ ->
        0
    end
  end

  defp parse_seconds(seconds_str) do
    case String.split(seconds_str, ".") do
      [whole, frac] ->
        {seconds, _} = Integer.parse(whole)
        # Pad fraction to 6 digits (microseconds)
        padded_frac = frac |> String.pad_trailing(6, "0") |> String.slice(0, 6)
        {microsecs, _} = Integer.parse(padded_frac)
        {seconds, microsecs}

      [whole] ->
        {seconds, _} = Integer.parse(whole)
        {seconds, 0}
    end
  end
end
