defmodule Sequin.Postgrex.Encoders do
  @moduledoc false

  defimpl Jason.Encoder, for: Postgrex.Interval do
    @moduledoc """
    Encodes a Postgrex.Interval as a time string in HH:MM:SS format.

    All interval components (months, days, seconds) are converted to a total
    time representation. This ensures consistent output between backfill and
    streaming modes.

    Examples:
    - %Postgrex.Interval{months: 0, days: 0, secs: 3661, microsecs: 0} => "01:01:01"
    - %Postgrex.Interval{months: 0, days: 1, secs: 3600, microsecs: 0} => "25:00:00"
    - %Postgrex.Interval{months: 1, days: 0, secs: 0, microsecs: 0} => "720:00:00"
    """

    # Conversion constants
    # Using 30 days per month as PostgreSQL's default for interval arithmetic
    @seconds_per_minute 60
    @seconds_per_hour 3600
    @seconds_per_day 86_400
    @seconds_per_month 30 * @seconds_per_day

    def encode(%Postgrex.Interval{} = interval, opts) do
      Jason.Encode.string(to_time_string(interval), opts)
    end

    defp to_time_string(%Postgrex.Interval{months: months, days: days, secs: secs, microsecs: microsecs}) do
      # Convert everything to total seconds
      total_secs = months * @seconds_per_month + days * @seconds_per_day + secs

      # Handle negative intervals
      {sign, abs_total_secs} = if total_secs < 0, do: {"-", abs(total_secs)}, else: {"", total_secs}

      hours = div(abs_total_secs, @seconds_per_hour)
      remaining = rem(abs_total_secs, @seconds_per_hour)
      minutes = div(remaining, @seconds_per_minute)
      seconds = rem(remaining, @seconds_per_minute)

      # Format hours with at least 2 digits, but allow more for large intervals
      hours_str = hours |> Integer.to_string() |> String.pad_leading(2, "0")
      base = "#{sign}#{hours_str}:#{pad2(minutes)}:#{pad2(seconds)}"

      if microsecs == 0 do
        base
      else
        # Format microseconds, removing trailing zeros
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
