defmodule Sequin.Postgres.ArrayLexer do
  @moduledoc """
  Lexer for Postgres array literals.

  * **Splits only** – every scalar comes back as a *string* (or `nil` for the
    literal `NULL`). No JSON decoding or type‑casting takes place here.
  * **Nesting preserved** – any inner arrays are returned as nested Elixir
    lists.
  * **Return contract** – always `{:ok, result}` on success or
    `{:error, reason}` on failure.

  ### Examples

      iex> Sequin.Postgres.ArrayLexer.lex("{}")
      {:ok, []}

      iex> Sequin.Postgres.ArrayLexer.lex("{1,2,3}")
      {:ok, ["1", "2", "3"]}

      iex> Sequin.Postgres.ArrayLexer.lex(~s({"a","b","c"}))
      {:ok, ["a", "b", "c"]}

      iex> Sequin.Postgres.ArrayLexer.lex("{{1,2},{3,4}}")
      {:ok, [["1", "2"], ["3", "4"]]}
  """
  alias Sequin.Error

  @type element :: String.t() | nil | [element]
  @type result :: {:ok, [element]} | {:error, term}

  @spec lex(String.t()) :: result
  def lex(binary) when is_binary(binary) do
    binary = String.trim(binary)

    case parse_array(binary) do
      {:ok, list, ""} -> {:ok, list}
      {:ok, _list, _rest} -> {:error, Error.invariant(message: "Invalid Postgres array (trailing garbage)")}
      {:error, _} = err -> err
    end
  end

  ## low‑level parsing -------------------------------------------------------

  # Entry point – expects the very first char to be "{"
  defp parse_array(<<"{", rest::binary>>) do
    parse_items(rest, [], nil)
  end

  defp parse_array(_), do: {:error, Error.invariant(message: "Invalid Postgres array (no opening bracket)")}

  # parse_items/3 walks until it finds the matching "}".
  # acc          – already‑parsed elements (reversed for speed)
  # current_buf  – nil  → no element started yet
  #                ""   → empty quoted element ("\"\"")
  #                str  → building (or finished) scalar element
  defp parse_items(<<"}", rest::binary>>, acc, buf) do
    {:ok, Enum.reverse(put_buf(buf, acc)), rest}
  end

  # Comma between elements
  defp parse_items(<<",", rest::binary>>, acc, buf) do
    parse_items(rest, put_buf(buf, acc), nil)
  end

  # Ignore whitespace outside of quotes
  @ws String.to_charlist(" \t\r\n")
  defp parse_items(<<c, rest::binary>>, acc, buf) when c in @ws do
    parse_items(rest, acc, buf)
  end

  # Start of a nested array
  defp parse_items(<<"{", _::binary>> = bin, acc, nil) do
    with {:ok, nested, rest} <- parse_array(bin) do
      parse_items(rest, [nested | acc], nil)
    end
  end

  # Quoted element
  # we just read { or a comma and the next non‑whitespace character is "
  defp parse_items(<<"\"", rest::binary>>, acc, nil) do
    with {:ok, value, rest2} <- parse_quoted(rest) do
      parse_items(rest2, acc, value)
    end
  end

  # Backslash escape (outside quotes) → keep the next char literally
  defp parse_items(<<"\\", c, rest::binary>>, acc, buf) do
    # buf will be nil if this is the first character
    parse_items(rest, acc, (buf || "") <> <<c>>)
  end

  # Any other character – part of an unquoted element
  defp parse_items(<<c, rest::binary>>, acc, buf) do
    # buf will be nil if this is the first character
    parse_items(rest, acc, (buf || "") <> <<c>>)
  end

  defp parse_items(<<>>, _acc, _buf),
    do: {:error, Error.invariant(message: "Invalid Postgres array (unterminated array)")}

  ## quoted string -----------------------------------------------------------

  defp parse_quoted(bin), do: collect_quoted(bin, "")

  defp collect_quoted(<<"\"", rest::binary>>, acc), do: {:ok, acc, rest}

  # Escaped char – drop the backslash, keep the char literally
  defp collect_quoted(<<"\\", c, rest::binary>>, acc), do: collect_quoted(rest, acc <> <<c>>)

  defp collect_quoted(<<c, rest::binary>>, acc), do: collect_quoted(rest, acc <> <<c>>)

  defp collect_quoted(<<>>, _), do: {:error, Error.invariant(message: "Invalid Postgres array (unterminated quoted)")}

  ## helpers -----------------------------------------------------------------

  # Adds the finished buffer (if any) to the accumulator.
  # no element captured
  defp put_buf(nil, acc), do: acc

  defp put_buf(buf, acc) when is_binary(buf) do
    elem =
      case buf do
        "NULL" -> nil
        raw -> raw
      end

    [elem | acc]
  end
end
