defmodule Sequin.Ecto.IntegerKeyMap do
  @moduledoc false
  use Ecto.Type

  def type, do: :map

  def cast(data) when is_map(data), do: {:ok, data}
  def cast(_), do: :error

  def load(data) when is_map(data) do
    {:ok,
     Enum.reduce(data, %{}, fn {key, value}, acc ->
       {int_key, ""} = Integer.parse(key)
       Map.put(acc, int_key, value)
     end)}
  end

  def load(_), do: :error

  def dump(data) when is_map(data), do: {:ok, data}
  def dump(_), do: :error
end
