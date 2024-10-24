defmodule Sequin.Ecto.IntegerKeyMap do
  @moduledoc false
  use Ecto.Type

  @type t :: map()

  def type, do: :map

  def cast(data) when is_map(data) do
    {:ok, Map.new(data, fn {k, v} -> {to_integer_key(k), v} end)}
  end

  def cast(_), do: :error

  def load(data) when is_map(data) do
    {:ok, Map.new(data, fn {k, v} -> {to_integer_key(k), v} end)}
  end

  def load(_), do: :error

  def dump(data) when is_map(data) do
    {:ok, Map.new(data, fn {k, v} -> {to_string(k), v} end)}
  end

  def dump(_), do: :error

  defp to_integer_key(key) when is_binary(key) do
    case Integer.parse(key) do
      {int_key, ""} -> int_key
      _ -> key
    end
  end

  defp to_integer_key(key), do: key
end
