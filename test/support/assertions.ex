defmodule Sequin.Test.Assertions do
  @moduledoc """
  Partially vendored from https://github.com/devonestes/assertions
  """

  @type comparison :: (any, any -> boolean | no_return)

  defmodule Comparisons do
    @moduledoc false
    def compare_maps(left, right, comparison \\ &Kernel.==/2) when is_map(left) and is_map(right) do
      {left_diff, right_diff, equal?} =
        compare_lists(Map.to_list(left), Map.to_list(right), comparison)

      {Map.new(left_diff), Map.new(right_diff), equal?}
    end

    @doc false
    def compare_lists(left, right, comparison \\ &Kernel.==/2) when is_list(left) and is_list(right) do
      {left_diff, right_diff} =
        Enum.reduce(1..length(left), {left, right}, &compare(&1, &2, comparison))

      {left_diff, right_diff, left_diff == [] and right_diff == []}
    end

    def compare(_, {[left_elem | left_acc], right_acc}, comparison) do
      result =
        Enum.find_index(right_acc, fn right_elem ->
          try do
            comparison.(left_elem, right_elem)
          rescue
            _ in [ExUnit.AssertionError] -> false
          end
        end)

      case result do
        nil -> {left_acc ++ [left_elem], right_acc}
        index -> {left_acc, List.delete_at(right_acc, index)}
      end
    end

    def compare(_, acc, _), do: acc
  end

  @doc """
  Asserts that a function should raise an exception, but without forcing the user to specify which
  exception should be raised. This is essentially a less-strict version of `assert_raise/2`.

      iex> assert_raise(fn -> String.to_existing_atom("asleimflisesliseli") end)
      true
  """
  @spec assert_raise(fun()) :: true | no_return
  def assert_raise(func) do
    func.()
    ExUnit.Assertions.flunk("Expected exception but nothing was raised")
  rescue
    e in ExUnit.AssertionError ->
      raise e

    _ ->
      true
  end

  @doc """
  Asserts that two lists contain the same elements without asserting they are
  in the same order.

      iex> assert_lists_equal([1, 2, 3], [1, 3, 2])
      true

  """
  @spec assert_lists_equal(list, list) :: true | no_return
  def assert_lists_equal(left, right) do
    {left_diff, right_diff, equal?} = Comparisons.compare_lists(left, right)

    if equal? do
      true
    else
      raise ExUnit.AssertionError,
        args: [left, right],
        left: left_diff,
        right: right_diff,
        message: "Comparison of each element failed!"
    end
  end

  @doc """
  Asserts that two lists contain the same elements without asserting they are
  in the same order.

  The given comparison function determines if the two lists are considered
  equal.

      iex> assert_lists_equal(["dog"], ["cat"], &(is_binary(&1) and is_binary(&2)))
      true

  """
  @spec assert_lists_equal(list, list, [any]) :: true | no_return
  @spec assert_lists_equal(list, list, comparison) :: true | no_return
  def assert_lists_equal(left, right, comparison) do
    {left_diff, right_diff, equal?} = Comparisons.compare_lists(left, right, comparison)

    if equal? do
      true
    else
      raise ExUnit.AssertionError,
        args: [inspect(left), inspect(right), inspect(comparison)],
        left: left_diff,
        right: right_diff,
        message: "Comparison of each element failed!"
    end
  end

  @doc """
  Asserts that a `map` is in the given `list`.

  This is either done by passing a list of `keys`, and the values at those keys
  will be compared to determine if the map is in the list.

      iex> map = %{first: :first, second: :second}
      iex> list = [%{first: :first, second: :second, third: :third}]
      iex> keys = [:first, :second]
      iex> assert_map_in_list(map, list, keys)
      true

  Or this is done by passing a comparison function that determines if the map
  is in the list.

  If using a comparison function, the `map` is the first argument to that
  function, and the elements in the list are the second argument.

      iex> map = %{first: :first, second: :second}
      iex> list = [%{"first" => :first, "second" => :second, "third" => :third}]
      iex> comparison = &(&1.first == &2["first"] and &1.second == &2["second"])
      iex> assert_map_in_list(map, list, comparison)
      true

  """
  @spec assert_map_in_list(map, [map], [any]) :: true | no_return
  @spec assert_map_in_list(map, [map], comparison) :: true | no_return
  def assert_map_in_list(map, list, keys_or_comparison) do
    {in_list?, map, list, message} =
      if is_list(keys_or_comparison) do
        keys = keys_or_comparison
        map = Map.take(map, keys)
        list = Enum.map(list, &Map.take(&1, keys))
        keys = stringify_list(keys_or_comparison)
        message = "Map matching the values for keys `#{keys}` not found"
        {Enum.member?(list, map), map, list, message}
      else
        comparison = keys_or_comparison
        message = "Map not found in list using given comparison"

        {Enum.any?(list, &comparison.(map, &1)), map, list, message}
      end

    if in_list? do
      true
    else
      raise ExUnit.AssertionError,
        args: [inspect(map), inspect(list)],
        left: map,
        right: list,
        message: message
    end
  end

  @doc """
  Asserts that two structs are equal.

  Equality can be determined in two ways. First, by passing a list of keys. The
  values at these keys and the type of the structs will be used to determine if
  the structs are equal.

      iex> left = DateTime.utc_now()
      iex> right = DateTime.utc_now()
      iex> keys = [:year, :minute]
      iex> assert_structs_equal(left, right, keys)
      true

  The second is to pass a comparison function that returns a boolean that
  determines if the structs are equal. When using a comparison function, the
  first argument to the function is the `left` struct and the second argument
  is the `right` struct.

      iex> left = DateTime.utc_now()
      iex> right = DateTime.utc_now()
      iex> comparison = &(&1.year == &2.year and &1.minute == &2.minute)
      iex> assert_structs_equal(left, right, comparison)
      true

  """
  @spec assert_structs_equal(struct, struct, [atom]) :: true | no_return
  @spec assert_structs_equal(struct, struct, (any, any -> boolean)) :: true | no_return
  def assert_structs_equal(left, right, keys_or_comparison) do
    {left_diff, right_diff, equal?, message} =
      if is_list(keys_or_comparison) do
        keys = [:__struct__ | keys_or_comparison]
        left = Map.take(left, keys)
        right = Map.take(right, keys)
        message = "Values for #{stringify_list(keys_or_comparison)} not equal!"
        {left_diff, right_diff, equal?} = Comparisons.compare_maps(left, right)
        {left_diff, right_diff, equal?, message}
      else
        comparison = keys_or_comparison

        {left_diff, right_diff, equal?} =
          case comparison.(left, right) do
            {_, _, equal?} = result when is_boolean(equal?) -> result
            true_or_false when is_boolean(true_or_false) -> {left, right, true_or_false}
          end

        {left_diff, right_diff, equal?, "Comparison failed!"}
      end

    if equal? do
      true
    else
      raise ExUnit.AssertionError,
        args: [inspect(left), inspect(right)],
        left: left_diff,
        right: right_diff,
        message: message
    end
  end

  @doc """
  Asserts that two maps are equal.

  Equality can be determined in two ways. First, by passing a list of keys. The
  values at these keys will be used to determine if the maps are equal.

      iex> left = %{first: :first, second: :second, third: :third}
      iex> right = %{first: :first, second: :second, third: :fourth}
      iex> keys = [:first, :second]
      iex> assert_maps_equal(left, right, keys)
      true

  The second is to pass a comparison function that returns a boolean that
  determines if the maps are equal. When using a comparison function, the first
  argument to the function is the `left` map and the second argument is the
  `right` map.

      iex> left = %{first: :first, second: :second, third: :third}
      iex> right = %{"first" => :first, "second" => :second, "third" => :fourth}
      iex> comparison = &(&1.first == &2["first"] and &1.second == &2["second"])
      iex> assert_maps_equal(left, right, comparison)
      true

  """
  @spec assert_maps_equal(map, map, [any]) :: true | no_return
  @spec assert_maps_equal(map, map, comparison) :: true | no_return
  def assert_maps_equal(left, right, keys_or_comparison) do
    {left_diff, right_diff, equal?, message} =
      if is_list(keys_or_comparison) do
        keys = keys_or_comparison
        left = Map.take(left, keys)
        right = Map.take(right, keys)
        {left_diff, right_diff, equal?} = Comparisons.compare_maps(left, right)
        message = "Values for #{inspect(keys_or_comparison)} not equal!"
        {left_diff, right_diff, equal?, message}
      else
        comparison = keys_or_comparison
        {left, right, comparison.(left, right), "Maps not equal using given comprison"}
      end

    if equal? do
      true
    else
      raise ExUnit.AssertionError,
        args: [inspect(left), inspect(right)],
        left: left_diff,
        right: right_diff,
        message: message
    end
  end

  defp stringify_list(list) do
    Enum.map_join(list, ", ", fn
      elem when is_atom(elem) -> ":#{elem}"
      elem when is_binary(elem) -> "\"#{elem}\""
      elem -> "#{inspect(elem)}"
    end)
  end
end
