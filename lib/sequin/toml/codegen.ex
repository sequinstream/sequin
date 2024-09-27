defmodule Sequin.TOML.Codegen do
  @moduledoc false

  alias Sequin.TOML.Encode

  @doc """
  Builds IO data for key-value pairs for TOML encoding.
  """
  def build_kv_iodata(kv, _encode_args) do
    elements =
      kv
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Enum.map(&encode_pair/1)
      |> Enum.intersperse("\n")

    collapse_static(elements)
  end

  defp encode_pair({key, value}) do
    key = Encode.key(key)
    val = quote do: Encode.value(unquote(value))
    [key, " = ", val]
  end

  defp collapse_static([bin1, bin2 | rest]) when is_binary(bin1) and is_binary(bin2) do
    collapse_static([bin1 <> bin2 | rest])
  end

  defp collapse_static([other | rest]) do
    [other | collapse_static(rest)]
  end

  defp collapse_static([]) do
    []
  end
end
