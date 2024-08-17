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
end
