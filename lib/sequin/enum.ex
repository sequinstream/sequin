defmodule Sequin.Enum do
  @moduledoc """
  Enum utilities. Functions defined in this file should feel like candidates for inclusion in an
  extended version of Elixir's Enum module.
  """
  def find!(enumberable, find_fun) do
    case Enum.find(enumberable, find_fun) do
      nil -> raise ArgumentError, "Could not find element"
      el -> el
    end
  end

  def take!(enumberable, count) do
    case Enum.take(enumberable, count) do
      list when length(list) < count -> raise ArgumentError, "Not enough elements in list to take"
      list -> list
    end
  end

  def find_index!(enumberable, find_fun) do
    case Enum.find_index(enumberable, find_fun) do
      nil -> raise ArgumentError, "Could not find element"
      index -> index
    end
  end

  @doc """
  Takes up to `count` elements from the `enumerable` for which `fun` returns a truthy value.

  Returns a list of elements that matched the predicate, up to `count` elements.

  The function will stop collecting elements once either:
  - `count` elements have been collected
  - or the enumerable has been fully traversed

  ## Examples

      iex> Sequin.Enum.take_until(1..10, 3, &(rem(&1, 2) == 0))
      [2, 4, 6]

      iex> Sequin.Enum.take_until(1..10, 2, &(&1 > 5))
      [6, 7]

      iex> Sequin.Enum.take_until([1, 2, 3], 5, &(&1 > 0))
      [1, 2, 3]

  """
  @spec take_until(Enumerable.t(), pos_integer(), (any() -> boolean())) :: [any()]
  def take_until(enumerable, count, fun) when is_integer(count) and count > 0 do
    enumerable
    |> Enum.reduce_while({[], 0}, fn elem, {acc, count_taken} ->
      if fun.(elem) do
        new_acc = [elem | acc]
        new_count_taken = count_taken + 1

        if new_count_taken >= count do
          {:halt, {new_acc, new_count_taken}}
        else
          {:cont, {new_acc, new_count_taken}}
        end
      else
        {:cont, {acc, count_taken}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  @doc """
  Reduces the `enumerable` as long as the accumulating function returns `:ok` or `{:ok, _}`.
  x
  If all elements are processed successfully, the function returns `:ok`.
  ## Examples
      iex> Sequin.Enum.reduce_while_ok(1..3, fn x -> if x < 3, do: :ok, else: {:error, "too large"} end)
      {:error, "too large"}
      iex> Sequin.Enum.reduce_while_ok(1..3, fn x -> if x > 3, do: {:error, "too large"}, else: :ok end)
      :ok
      iex> Sequin.Enum.reduce_while_ok(1..3, fn x -> if x > 3, do: {:error, "too large"}, else: {:ok, x} end)
      :ok
  """
  @spec reduce_while_ok(Enumerable.t(), (any() -> :ok | {:ok, any()} | any())) :: :ok | any()
  def reduce_while_ok(enumerable, fun) do
    Enum.reduce_while(enumerable, :ok, fn elem, _acc ->
      case fun.(elem) do
        :ok -> {:cont, :ok}
        {:ok, _} -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Recursively traverses a data structure and applies a transformation function to each value.

  The function traverses maps (including structs), lists, and scalar values. For each scalar value,
  the provided transformation function is applied. Structs are temporarily converted to maps,
  transformed, and then converted back to their original struct type.

  ## Examples

      iex> transform_deeply(%{a: 1, b: 2}, fn x -> x * 2 end)
      %{a: 2, b: 4}

      iex> transform_deeply([1, [2, 3], %{a: 4}], fn x -> is_integer(x) && x * 2 || x end)
      [2, [4, 6], %{a: 8}]

      iex> transform_deeply(%DateTime{year: 2023, month: 4, day: 8}, fn
      ...>   %DateTime{} = dt -> DateTime.to_unix(dt)
      ...>   other -> other
      ...> end)
      # Returns the DateTime converted to unix timestamp

  """
  @spec transform_deeply(term(), (term() -> term())) :: term()
  def transform_deeply(value, transform_fn) when is_function(transform_fn, 1) do
    case value do
      %scalar_module{} = struct
      when scalar_module in [DateTime, NaiveDateTime, Date, Time, Calendar.ISO, Decimal, MapSet, Regex, Range, Port, URI] ->
        transform_fn.(struct)

      %struct_module{} = struct ->
        # For structs: convert to map → transform → convert back to same struct type
        struct
        |> Map.from_struct()
        |> transform_deeply(transform_fn)
        |> then(&struct(struct_module, &1))

      %{} = map ->
        # For maps: transform each value recursively
        Map.new(map, fn {k, v} -> {k, transform_deeply(v, transform_fn)} end)

      list when is_list(list) ->
        # For lists: transform each element recursively
        Enum.map(list, &transform_deeply(&1, transform_fn))

      # For scalar values: apply the transform function directly
      other ->
        transform_fn.(other)
    end
  end
end
