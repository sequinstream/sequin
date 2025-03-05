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
end
