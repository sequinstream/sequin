defmodule Sequin.TOML.Encode do
  @moduledoc """
  Utilities for encoding Elixir values to TOML with proper indentation for nested entries.

  Keys with `nil` values are omitted since TOML does not support null values.
  """

  alias Sequin.TOML.Encoder

  @type opts :: %{indent: non_neg_integer, newline: String.t()}

  @doc false
  @spec encode(any(), opts()) :: {:ok, iodata()} | {:error, Exception.t()}
  def encode(value, opts \\ %{indent: 2, newline: "\n"}) do
    {:ok, value(value, opts)}
  rescue
    e in Protocol.UndefinedError -> {:error, e}
  end

  @doc """
  Encodes an Elixir value to TOML.
  """
  @spec value(any(), opts()) :: iodata()
  def value(value, opts \\ %{indent: 2, newline: "\n"}) do
    Encoder.encode(value, opts)
  end

  @spec atom(atom(), opts()) :: iodata()
  def atom(true, _opts), do: "true"
  def atom(false, _opts), do: "false"
  def atom(nil, _opts), do: ""
  def atom(atom, _opts), do: Atom.to_string(atom)

  @spec integer(integer()) :: iodata()
  def integer(integer) do
    Integer.to_string(integer)
  end

  @spec float(float()) :: iodata()
  def float(float) do
    :erlang.float_to_binary(float, [:compact, decimals: 15])
  end

  @spec string(String.t(), opts()) :: iodata()
  def string(string, _opts) do
    ["\"", escape_string(string), "\""]
  end

  @spec list(list(), opts()) :: iodata()
  def list(list, opts) do
    ["[", Enum.map_join(list, ", ", &value(&1, opts)), "]"]
  end

  @spec map(map(), opts()) :: iodata()
  def map(map, opts) do
    iodata = encode_map(map, opts)
    IO.iodata_to_binary(iodata)
  end

  defp encode_map(map, opts, path \\ [], indent_level \\ 0) do
    indent = String.duplicate("  ", indent_level)
    next_indent_level = indent_level + 1

    {key_values, tables} =
      Enum.reduce(map, {[], []}, fn {k, v}, {key_values, tables} ->
        cond do
          v == nil ->
            # Skip keys with nil values
            {key_values, tables}

          is_map(v) and not is_struct(v) ->
            # Recursively encode nested maps
            new_path = path ++ split_key(k)
            table = encode_map(v, opts, new_path, next_indent_level)
            {key_values, tables ++ [table]}

          is_struct(v) ->
            # Handle structs by creating a new table and encoding the struct
            new_path = path ++ split_key(k)
            table_header = construct_header(new_path, indent_level, opts)
            # Encode the struct using its own encoder
            struct_content = Encoder.encode(v, opts)
            # Indent the struct content
            indented_content = indent_lines(struct_content, next_indent_level, opts)
            full_table = [table_header, indented_content]
            {key_values, tables ++ [full_table]}

          true ->
            # Handle scalar values
            key = key(k)
            val = value(v, opts)

            {
              key_values ++ [[indent, key, " = ", val, opts[:newline]]],
              tables
            }
        end
      end)

    header = construct_header(path, indent_level, opts)

    # Only include the header if there are key-values or tables under it
    if path != [] and (key_values != [] or tables != []) do
      [header | key_values ++ tables]
    else
      key_values ++ tables
    end
  end

  defp construct_header(path, indent_level, opts) do
    # Ensure indent_level is not negative
    indent_level = max(indent_level, 0)
    header_indent = String.duplicate("  ", indent_level)

    if path == [] do
      []
    else
      keys = Enum.map(path, &header_key/1)
      [header_indent, "[", Enum.join(keys, "."), "]", opts[:newline]]
    end
  end

  defp indent_lines(content, indent_level, opts) do
    indent = String.duplicate("  ", indent_level)

    content
    |> IO.iodata_to_binary()
    |> String.split(opts[:newline])
    |> Enum.map_join(opts[:newline], fn
      "" -> ""
      line -> indent <> line
    end)
    |> Kernel.<>(opts[:newline])
  end

  defp header_key(key) do
    key = to_string(key)

    if String.match?(key, ~r/^[A-Za-z0-9_-]+$/) do
      key
    else
      "\"#{escape_string(key)}\""
    end
  end

  defp split_key(key) do
    key = to_string(key)
    String.split(key, ".")
  end

  @spec datetime(Date.t() | Time.t() | NaiveDateTime.t() | DateTime.t()) :: iodata()
  def datetime(value) do
    [value.__struct__.to_iso8601(value)]
  end

  @spec decimal(Decimal.t()) :: iodata()
  def decimal(value) do
    decimal = Decimal
    decimal.to_string(value, :normal)
  end

  defp key(string) when is_binary(string) do
    if String.match?(string, ~r/^[A-Za-z0-9_-]+$/) do
      string
    else
      ["\"", escape_string(string), "\""]
    end
  end

  defp key(atom) when is_atom(atom) do
    key(Atom.to_string(atom))
  end

  defp escape_string(string) do
    string
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
  end
end
