defmodule Sequin.EtsMultiset do
  @moduledoc """
  A generic multiset implementation using ETS tables that maintains groups of unique values.

  This provides similar functionality to Sequin.Multiset but leverages
  ETS for potentially better performance with large datasets and concurrent access.

  Values within each group are unique, but the same value can appear in different groups.
  """
  @type key :: any()
  @type value :: any()
  @type t :: :ets.tid() | atom()
  @type access :: :protected | :private | :public

  @doc """
  Creates a new empty multiset as an ETS table.

  ## Options
    * `:access` - Access mode for the table. Can be `:protected`, `:private`, or `:public`. Defaults to `:protected`.

  ## Examples
      iex> table = Sequin.EtsMultiset.new()
      iex> is_reference(table)
      true
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    access = Keyword.get(opts, :access, :protected)
    :ets.new(:ets_multiset, [:set, access])
  end

  @doc """
  Creates a new empty multiset as an ETS table with a specified name.

  ## Options
    * `:access` - Access mode for the table. Can be `:protected`, `:private`, or `:public`. Defaults to `:protected`.

  ## Examples
      iex> table = Sequin.EtsMultiset.new_named(:my_table)
      iex> is_reference(table)
      true
  """
  @spec new_named(atom(), keyword()) :: t()
  def new_named(name, opts \\ []) when is_atom(name) do
    access = Keyword.get(opts, :access, :protected)
    :ets.new(name, [:named_table, :set, access])
  end

  @doc """
  Creates a new multiset from a list of {key, value} tuples.

  ## Examples
      iex> table = Sequin.EtsMultiset.new_from_list([{"group1", "value1"}, {"group1", "value2"}, {"group2", "value1"}])
      iex> Sequin.EtsMultiset.get(table, "group1") |> Enum.sort()
      ["value1", "value2"]
  """
  @spec new_from_list([{key(), value()}], atom() | nil) :: t()
  def new_from_list(entries, name \\ nil) do
    table = if is_nil(name), do: new(), else: new_named(name)
    put_many(table, entries)
    table
  end

  @doc """
  Adds a value to the set associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new()
      iex> Sequin.EtsMultiset.put(table, "group1", "value1")
      iex> Sequin.EtsMultiset.get(table, "group1")
      ["value1"]
  """
  @spec put(t(), key(), value()) :: t()
  def put(table, key, value) do
    :ets.insert(table, {{key, value}})
    table
  end

  @doc """
  Adds multiple key-value pairs to the multiset in a single batch operation.
  This is more efficient than calling put/3 multiple times.

  ## Examples
      iex> table = Sequin.EtsMultiset.new()
      iex> Sequin.EtsMultiset.put_many(table, [{"group1", "value1"}, {"group1", "value2"}, {"group2", "value3"}])
      iex> Sequin.EtsMultiset.get(table, "group1") |> Enum.sort()
      ["value1", "value2"]
  """
  @spec put_many(t(), [{key(), value()}]) :: t()
  def put_many(table, entries) when is_list(entries) do
    objects = Enum.map(entries, fn {key, value} -> {{key, value}} end)
    :ets.insert(table, objects)
    table
  end

  @doc """
  Removes a value from the set associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}])
      iex> Sequin.EtsMultiset.delete(table, "group1", "value1")
      iex> Sequin.EtsMultiset.get(table, "group1")
      []
  """
  @spec delete(t(), key(), value()) :: t()
  def delete(table, key, value) do
    :ets.delete_object(table, {{key, value}})
    table
  end

  @doc """
  Removes all values associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}, {"group2", "value3"}])
      iex> Sequin.EtsMultiset.delete_key(table, "group1")
      iex> Sequin.EtsMultiset.get(table, "group1")
      []
      iex> Sequin.EtsMultiset.get(table, "group2")
      ["value3"]
  """
  @spec delete_key(t(), key()) :: t()
  def delete_key(table, key) do
    :ets.select_delete(table, [{{{key, :_}}, [], [true]}])
    table
  end

  @doc """
  Removes multiple values from the set associated with the given key in a single operation.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.EtsMultiset.delete_many(table, "group1", ["value1", "value2"])
      iex> Sequin.EtsMultiset.get(table, "group1")
      []
  """
  @spec delete_many(t(), key(), [value()]) :: t()
  def delete_many(table, key, values) when is_list(values) do
    objects = Enum.map(values, fn value -> {{key, value}} end)
    Enum.each(objects, fn object -> :ets.delete_object(table, object) end)
    table
  end

  @doc """
  Returns all values across all keys in the multiset.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group2", "value2"}])
      iex> Sequin.EtsMultiset.values(table) |> Enum.sort()
      ["value1", "value2"]
  """
  @spec values(t()) :: [value()]
  def values(table) do
    :ets.select(table, [{{{:_, :"$1"}}, [], [:"$1"]}])
  end

  @doc """
  Checks if a key exists in the multiset.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}])
      iex> Sequin.EtsMultiset.member?(table, "group1")
      true
  """
  @spec member?(t(), key()) :: boolean()
  def member?(table, key) do
    case :ets.select(table, [{{{key, :_}}, [], [true]}], 1) do
      {[true], _} -> true
      _ -> false
    end
  end

  @doc """
  Checks if a value exists in the set associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}])
      iex> Sequin.EtsMultiset.value_member?(table, "group1", "value1")
      true
  """
  @spec value_member?(t(), key(), value()) :: boolean()
  def value_member?(table, key, value) do
    :ets.member(table, {key, value})
  end

  @doc """
  Gets all values associated with a key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.EtsMultiset.get(table, "group1") |> Enum.sort()
      ["value1", "value2"]
  """
  @spec get(t(), key()) :: [value()]
  def get(table, key) do
    :ets.select(table, [{{{key, :"$1"}}, [], [:"$1"]}])
  end

  @doc """
  Gets all values associated with a key as a MapSet.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.EtsMultiset.get_set(table, "group1")
      MapSet.new(["value1", "value2"])
  """
  @spec get_set(t(), key()) :: MapSet.t(value())
  def get_set(table, key) do
    table
    |> get(key)
    |> MapSet.new()
  end

  @doc """
  Gets all values associated with a key. Raises if the key doesn't exist.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.EtsMultiset.fetch!(table, "group1") |> Enum.sort()
      ["value1", "value2"]
  """
  @spec fetch!(t(), key()) :: [value()]
  def fetch!(table, key) do
    values = get(table, key)
    if values == [], do: raise(KeyError, "key #{inspect(key)} not found in ETS table")
    values
  end

  @doc """
  Returns the count of values for a given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> Sequin.EtsMultiset.count(table, "group1")
      2
  """
  @spec count(t(), key()) :: non_neg_integer()
  def count(table, key) do
    :ets.select_count(table, [{{{key, :_}}, [], [true]}])
  end

  @doc """
  Returns all keys in the multiset.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group2", "value2"}])
      iex> Sequin.EtsMultiset.keys(table) |> Enum.sort()
      ["group1", "group2"]
  """
  @spec keys(t()) :: [key()]
  def keys(table) do
    table
    |> :ets.select([{{{:"$1", :_}}, [], [:"$1"]}])
    |> Enum.uniq()
  end

  @doc """
  Removes a set of values from the set associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new([{"group1", "value1"}, {"group1", "value2"}])
      iex> values_to_remove = MapSet.new(["value1", "value2"])
      iex> Sequin.EtsMultiset.difference(table, "group1", values_to_remove)
      iex> Sequin.EtsMultiset.get(table, "group1")
      []
  """
  @spec difference(t(), key(), MapSet.t(value())) :: t()
  def difference(table, key, values) when is_struct(values, MapSet) do
    delete_many(table, key, MapSet.to_list(values))
    table
  end

  @doc """
  Adds a set of values to the set associated with the given key.

  ## Examples
      iex> table = Sequin.EtsMultiset.new()
      iex> values_to_add = MapSet.new(["value2", "value3"])
      iex> Sequin.EtsMultiset.union(table, "group1", values_to_add)
      iex> Sequin.EtsMultiset.get(table, "group1") |> Enum.sort()
      ["value2", "value3"]
  """
  @spec union(t(), key(), MapSet.t(value())) :: t()
  def union(table, key, values) when is_struct(values, MapSet) do
    entries = Enum.map(MapSet.to_list(values), fn value -> {key, value} end)
    put_many(table, entries)
    table
  end

  @doc """
  Deletes the ETS table when you're done with it.

  ## Examples
      iex> table = Sequin.EtsMultiset.new()
      iex> Sequin.EtsMultiset.destroy(table)
      true
  """
  @spec destroy(t()) :: true
  def destroy(table) do
    :ets.delete(table)
  end
end
