defmodule Sequin.Postgrex.Encoders do
  @moduledoc false

  defimpl Jason.Encoder, for: Postgrex.Interval do
    def encode(%Postgrex.Interval{months: months, days: days, secs: secs, microsecs: microsecs}, opts) do
      Jason.Encoder.encode(
        %{
          months: months,
          days: days,
          secs: secs,
          microsecs: microsecs
        },
        opts
      )
    end
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
