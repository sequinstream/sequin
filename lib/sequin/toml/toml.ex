defmodule Sequin.TOML do
  @moduledoc """
  This module provides functions to encode Elixir data structures into TOML format,
  following the TOML 1.1 specification.

  ## Features

  - Supports encoding of common Elixir data types.
  - Proper indentation for nested tables.
  - Customizable indentation and newline characters.
  - High-performance encoding with iodata support.

  ## Examples

      iex> Sequin.TOML.encode(%{title: "TOML Example"})
      {:ok, "title = \"TOML Example\""}

      iex> Sequin.TOML.encode!(%{servers: %{alpha: %{ip: "10.0.0.1"}}})
      "[servers]\n  [servers.alpha]\n  ip = \"10.0.0.1\"\n"

  """

  alias Sequin.TOML.Encode

  @type encode_opt :: {:indent, non_neg_integer} | {:newline, String.t()}

  @doc """
  Encodes a given Elixir data structure into a TOML-formatted string.

  ## Options

    * `:indent` - The number of spaces to use for indentation (default: `2`).
    * `:newline` - The newline string to use (default: `"\n"`).

  ## Examples

      iex> Sequin.TOML.encode(%{title: "TOML Example"})
      {:ok, "title = \"TOML Example\""}

      iex> Sequin.TOML.encode(%{servers: %{alpha: %{ip: "10.0.0.1"}}}, indent: 4)
      {:ok, "[servers]\n    [servers.alpha]\n    ip = \"10.0.0.1\"\n"}

  """
  @spec encode(term, [encode_opt]) :: {:ok, String.t()} | {:error, Exception.t()}
  def encode(input, opts \\ []) do
    case do_encode(input, format_encode_opts(opts)) do
      {:ok, iodata} -> {:ok, IO.iodata_to_binary(iodata)}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Encodes a given Elixir data structure into a TOML-formatted string.

  Similar to `encode/2`, but raises an exception on error.

  ## Examples

      iex> Sequin.TOML.encode!(%{title: "TOML Example"})
      "title = \"TOML Example\""

      iex> Sequin.TOML.encode!(%{servers: %{alpha: %{ip: "10.0.0.1"}}}, indent: 4)
      "[servers]\n    [servers.alpha]\n    ip = \"10.0.0.1\"\n"

  """
  @spec encode!(term, [encode_opt]) :: String.t() | no_return
  def encode!(input, opts \\ []) do
    case do_encode(input, format_encode_opts(opts)) do
      {:ok, iodata} -> IO.iodata_to_binary(iodata)
      {:error, error} -> raise error
    end
  end

  @doc """
  Encodes a given Elixir data structure into TOML and returns iodata.

  This function is efficient when writing directly to IO devices.

  ## Options

    * `:indent` - The number of spaces to use for indentation (default: `2`).
    * `:newline` - The newline string to use (default: `"\n"`).

  ## Examples

      iex> {:ok, iodata} = Sequin.TOML.encode_to_iodata(%{title: "TOML Example"})
      iex> IO.iodata_to_binary(iodata)
      "title = \"TOML Example\""

  """
  @spec encode_to_iodata(term, [encode_opt]) :: {:ok, iodata()} | {:error, Exception.t()}
  def encode_to_iodata(input, opts \\ []) do
    do_encode(input, format_encode_opts(opts))
  end

  @doc """
  Encodes a given Elixir data structure into TOML and returns iodata.

  Similar to `encode_to_iodata/2`, but raises an exception on error.

  ## Examples

      iex> iodata = Sequin.TOML.encode_to_iodata!(%{title: "TOML Example"})
      iex> IO.iodata_to_binary(iodata)
      "title = \"TOML Example\""

  """
  @spec encode_to_iodata!(term, [encode_opt]) :: iodata() | no_return
  def encode_to_iodata!(input, opts \\ []) do
    case do_encode(input, format_encode_opts(opts)) do
      {:ok, iodata} -> iodata
      {:error, error} -> raise error
    end
  end

  defp do_encode(input, opts) do
    Encode.encode(input, opts)
  end

  defp format_encode_opts(opts) do
    defaults = %{indent: 2, newline: "\n"}

    opts
    |> Map.new()
    |> Map.merge(defaults)
  end
end
