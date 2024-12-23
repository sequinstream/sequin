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
end
