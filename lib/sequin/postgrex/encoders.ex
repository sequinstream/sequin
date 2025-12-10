defmodule Sequin.Postgrex.Encoders do
  @moduledoc false

  defimpl Jason.Encoder, for: Postgrex.Interval do
    @doc """
    Encodes a Postgrex.Interval as a PostgreSQL-style interval string.

    This matches the format that live CDC receives from the WAL, ensuring
    consistent output between backfill and streaming modes.

    Examples:
    - %Postgrex.Interval{months: 0, days: 0, secs: 3661, microsecs: 0} => "01:01:01"
    - %Postgrex.Interval{months: 1, days: 2, secs: 3661, microsecs: 0} => "1 mon 2 days 01:01:01"
    """
    def encode(%Postgrex.Interval{} = interval, opts) do
      Jason.Encode.string(to_pg_interval_string(interval), opts)
    end

    defp to_pg_interval_string(%Postgrex.Interval{months: months, days: days, secs: secs, microsecs: microsecs}) do
      parts = []

      # Handle years and months
      years = div(months, 12)
      remaining_months = rem(months, 12)

      parts = if years == 0, do: parts, else: parts ++ [format_unit(years, "year")]
      parts = if remaining_months == 0, do: parts, else: parts ++ [format_unit(remaining_months, "mon")]

      # Handle days
      parts = if days == 0, do: parts, else: parts ++ [format_unit(days, "day")]

      # Handle time portion (hours:minutes:seconds.microseconds)
      time_str = format_time(secs, microsecs)

      # Always include time if there are no other parts, or if time is non-zero
      parts =
        if parts == [] or secs != 0 or microsecs != 0 do
          parts ++ [time_str]
        else
          parts
        end

      Enum.join(parts, " ")
    end

    defp format_unit(value, unit) when value == 1 or value == -1, do: "#{value} #{unit}"
    defp format_unit(value, unit), do: "#{value} #{unit}s"

    defp format_time(secs, microsecs) do
      # Handle negative intervals
      {sign, abs_secs} = if secs < 0, do: {"-", abs(secs)}, else: {"", secs}

      hours = div(abs_secs, 3600)
      remaining = rem(abs_secs, 3600)
      minutes = div(remaining, 60)
      seconds = rem(remaining, 60)

      base = "#{sign}#{pad2(hours)}:#{pad2(minutes)}:#{pad2(seconds)}"

      if microsecs == 0 do
        base
        # Format microseconds, removing trailing zeros
      else
        micro_str = microsecs |> abs() |> Integer.to_string() |> String.pad_leading(6, "0")
        micro_str = String.trim_trailing(micro_str, "0")
        "#{base}.#{micro_str}"
      end
    end

    defp pad2(n), do: n |> Integer.to_string() |> String.pad_leading(2, "0")
  end

  defimpl Jason.Encoder, for: Postgrex.Lexeme do
    def encode(%Postgrex.Lexeme{} = lexeme, opts) do
      Jason.Encoder.encode(
        %{
          type: :lexeme,
          word: lexeme.word,
          positions: Enum.map(lexeme.positions, &Tuple.to_list/1)
        },
        opts
      )
    end
  end

  defimpl String.Chars, for: Postgrex.Range do
    def to_string(%Postgrex.Range{} = range) do
      lower_bracket = if range.lower_inclusive, do: "[", else: "("
      upper_bracket = if range.upper_inclusive, do: "]", else: ")"

      lower_str = format_bound(range.lower)
      upper_str = format_bound(range.upper)

      "#{lower_bracket}#{lower_str},#{upper_str}#{upper_bracket}"
    end

    defp format_bound(value) when is_nil(value), do: ""
    defp format_bound(%Date{} = date), do: Date.to_iso8601(date)
    defp format_bound(%NaiveDateTime{} = dt), do: NaiveDateTime.to_iso8601(dt)
    defp format_bound(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
    defp format_bound(value), do: Kernel.to_string(value)
  end

  defimpl Jason.Encoder, for: Postgrex.Range do
    def encode(range, opts) do
      Jason.Encode.string(to_string(range), opts)
    end
  end
end
