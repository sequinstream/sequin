defmodule Sequin.Map do
  @moduledoc """
  Map utilities. Functions defined in this file should feel like candidates for inclusion in an
  extended version of Elixir's Map module.
  """
  def rename_key(map, key, new_key) do
    {value, map} = Map.pop(map, key)
    Map.put(map, new_key, value)
  end

  def stringify_keys(map) do
    Map.new(map, fn {k, v} -> {to_string(k), v} end)
  end

  def deep_stringify_keys(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {to_string(k), deep_stringify_keys(v)} end)
  end

  def deep_stringify_keys(list) when is_list(list) do
    Enum.map(list, &deep_stringify_keys/1)
  end

  def deep_stringify_keys(value), do: value

  def atomize_keys(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) ->
        {String.to_existing_atom(k), v}

      {k, v} when is_atom(k) ->
        {k, v}
    end)
  end

  def put_if_present(map, key, value) do
    if is_nil(value) do
      map
    else
      Map.put(map, key, value)
    end
  end

  def reject_nil_values(map) do
    map
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Map.new()
  end

  @doc """
  In our factories, we generate structs. But sometimes we want to use those factories/structs as
  maps that we can pass into e.g. changesets. This function takes an Ecto schema struct and
  returns a map, stripped of any nil or Ecto.Association.NotLoaded values.
  """
  @spec from_ecto(Ecto.Schema.t() | map(), Keyword.t() | nil) :: map()
  def from_ecto(struct, opts \\ [])

  def from_ecto(struct, opts) when is_struct(struct) do
    struct
    |> Map.from_struct()
    |> from_ecto(opts)
  end

  # May be a struct that was recently converted into a map
  def from_ecto(map, opts) do
    keep_nils = Keyword.get(opts, :keep_nils, false)

    map
    |> Enum.reject(fn {_k, v} ->
      (not keep_nils and is_nil(v)) or is_struct(v, Ecto.Association.NotLoaded) or is_struct(v, Ecto.Schema.Metadata)
    end)
    |> Map.new()
  end

  @doc """
  Converts all keys in a map from snake_case to camelCase.
  """
  def camelize_keys(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {camelize_key(k), v} end)
  end

  defp camelize_key(key) when is_atom(key) do
    key
    |> Atom.to_string()
    |> camelize_key()
    |> String.to_atom()
  end

  defp camelize_key(key) when is_binary(key) do
    [first | rest] = String.split(key, "_")
    first <> Enum.map_join(rest, &String.capitalize/1)
  end

  @doc """
  Recursively converts a struct and all nested structs into maps.
  """
  @structs_to_preserve [Date, Time, DateTime, NaiveDateTime, Decimal]
  def from_struct_deep(%struct{} = value) when struct in @structs_to_preserve do
    value
  end

  def from_struct_deep(struct) when is_struct(struct) do
    struct
    |> Map.from_struct()
    |> from_struct_deep()
  end

  def from_struct_deep(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {k, from_struct_deep(v)} end)
  end

  def from_struct_deep(list) when is_list(list) do
    Enum.map(list, &from_struct_deep/1)
  end

  def from_struct_deep(value), do: value
end
