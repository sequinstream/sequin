defmodule Sequin.Multiset do
  @moduledoc """
  A generic multiset implementation that maintains groups of unique string values.

  The multiset is implemented as a map where each key (group) maps to a MapSet of values.
  Values within each group are unique, but the same value can appear in different groups.

  This implementation is particularly useful for scenarios where you need to:
  - Track sets of values by a group identifier
  - Efficiently check group membership
  - Maintain unique values within groups
  - Remove empty groups automatically
  """

  @type key :: any()
  @type value :: any()
  @type t :: %{key() => MapSet.t(value())}

  @doc """
  Creates a new empty multiset.

  ## Examples
      iex> Sequin.Multiset.new()
      %{}
  """
  @spec new() :: t()
  def new, do: %{}

  @doc """
  Creates a new multiset from a list of {key, value} tuples.

  ## Examples
      iex> Sequin.Multiset.new([{"group1", "value1"}, {"group1", "value2"}, {"group2", "value1"}])
      %{"group1" => MapSet.new(["value1", "value2"]), "group2" => MapSet.new(["value1"])}
  """
  @spec new([{key(), value()}]) :: t()
  def new(entries) do
    Enum.reduce(entries, new(), fn {key, value}, acc ->
      put(acc, key, value)
    end)
  end

  @doc """
  Adds a value to the set associated with the given key.
  If the key doesn't exist, creates a new set with the value.

  ## Examples
      iex> multiset = Sequin.Multiset.new()
      iex> Sequin.Multiset.put(multiset, "group1", "value1")
      %{"group1" => MapSet.new(["value1"])}
  """
  @spec put(t(), key(), value()) :: t()
  def put(multiset, key, value) do
    Map.update(multiset, key, MapSet.new([value]), &MapSet.put(&1, value))
  end

  @doc """
  Removes a value from the set associated with the given key.
  If the resulting set is empty, removes the key entirely.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}])
      iex> Sequin.Multiset.delete(multiset, "group1", "value1")
      %{}
  """
  @spec delete(t(), key(), value()) :: t()
  def delete(multiset, key, value) do
    case Map.get(multiset, key) do
      nil ->
        multiset

      set ->
        next_set = MapSet.delete(set, value)

        if next_set == MapSet.new([]) do
          Map.delete(multiset, key)
        else
          Map.put(multiset, key, next_set)
        end
    end
  end

  @spec values(t()) :: [value()]
  def values(multiset) do
    multiset
    |> Map.values()
    |> Enum.map(fn set -> MapSet.to_list(set) end)
    |> List.flatten()
  end

  @spec values(t(), key()) :: [value()]
  def values(multiset, key) do
    case Map.get(multiset, key) do
      nil -> []
      set -> MapSet.to_list(set)
    end
  end

  @doc """
  Checks if a key exists in the multiset.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}])
      iex> Sequin.Multiset.member?(multiset, "group1")
      true
  """
  @spec member?(t(), key()) :: boolean()
  def member?(multiset, key) do
    Map.has_key?(multiset, key)
  end

  @doc """
  Checks if a value exists in the set associated with the given key.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}])
      iex> Sequin.Multiset.value_member?(multiset, "group1", "value1")
      true
  """
  @spec value_member?(t(), key(), value()) :: boolean()
  def value_member?(multiset, key, value) do
    case Map.get(multiset, key) do
      nil -> false
      set -> MapSet.member?(set, value)
    end
  end

  @doc """
  Gets all values associated with a key.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.Multiset.get(multiset, "group1")
      MapSet.new(["value1", "value2"])
  """
  @spec get(t(), key()) :: MapSet.t(value()) | nil
  def get(multiset, key) do
    Map.get(multiset, key)
  end

  @doc """
  Gets all values associated with a key. Raises if the key doesn't exist.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.Multiset.fetch!(multiset, "group1")
      MapSet.new(["value1", "value2"])

      iex> multiset = Sequin.Multiset.new()
      iex> Sequin.Multiset.fetch!(multiset, "nonexistent")
      ** (KeyError) key "nonexistent" not found in: %{}
  """
  @spec fetch!(t(), key()) :: MapSet.t(value())
  def fetch!(multiset, key) do
    Map.fetch!(multiset, key)
  end

  @spec count(t(), key()) :: non_neg_integer()
  def count(multiset, key) do
    case Map.get(multiset, key) do
      nil -> 0
      set -> MapSet.size(set)
    end
  end

  @doc """
  Returns all keys in the multiset.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}, {"group2", "value2"}])
      iex> Sequin.Multiset.keys(multiset)
      ["group1", "group2"]
  """
  @spec keys(t()) :: [key()]
  def keys(multiset) do
    Map.keys(multiset)
  end

  @doc """
  Removes a set of values from the set associated with the given key.
  If the resulting set is empty, removes the key entirely.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> values_to_remove = MapSet.new(["value1", "value2"])
      iex> Sequin.Multiset.difference(multiset, "group1", values_to_remove)
      %{}
  """
  @spec difference(t(), key(), MapSet.t(value())) :: t()
  def difference(multiset, key, values) when is_struct(values, MapSet) do
    case Map.get(multiset, key) do
      nil ->
        multiset

      set ->
        next_set = MapSet.difference(set, values)

        if MapSet.size(next_set) == 0 do
          Map.delete(multiset, key)
        else
          Map.put(multiset, key, next_set)
        end
    end
  end

  @doc """
  Adds a set of values to the set associated with the given key.
  If the key doesn't exist, creates a new set with the values.

  ## Examples
      iex> multiset = Sequin.Multiset.new([{"group1", "value1"}])
      iex> values_to_add = MapSet.new(["value2", "value3"])
      iex> Sequin.Multiset.union(multiset, "group1", values_to_add)
      %{"group1" => MapSet.new(["value1", "value2", "value3"])}
  """
  @spec union(t(), key(), MapSet.t(value())) :: t()
  def union(multiset, key, values) when is_struct(values, MapSet) do
    Map.update(multiset, key, values, &MapSet.union(&1, values))
  end
end
