defmodule Sequin.EnumTest do
  use Sequin.Case, async: true

  # Create a simple struct for testing
  defmodule TestStruct do
    @moduledoc false
    defstruct [:a, :b]
  end

  # Create a simple struct for testing
  defmodule ComplexStruct do
    @moduledoc false
    defstruct [:value, :nested]
  end

  describe "transform_deeply/2" do
    test "transforms values in a flat map" do
      map = %{a: 1, b: 2, c: 3}
      result = Sequin.Enum.transform_deeply(map, fn x -> x * 2 || x end)
      assert result == %{a: 2, b: 4, c: 6}
    end

    test "transforms values in a nested map" do
      map = %{a: 1, b: %{c: 2, d: 3}, e: 4}
      result = Sequin.Enum.transform_deeply(map, fn x -> x * 2 || x end)
      assert result == %{a: 2, b: %{c: 4, d: 6}, e: 8}
    end

    test "transforms values in a list" do
      list = [1, 2, 3]
      result = Sequin.Enum.transform_deeply(list, fn x -> x * 2 || x end)
      assert result == [2, 4, 6]
    end

    test "transforms values in a nested list" do
      list = [1, [2, 3], 4]
      result = Sequin.Enum.transform_deeply(list, fn x -> x * 2 || x end)
      assert result == [2, [4, 6], 8]
    end

    test "transforms values in a mixed nested structure" do
      data = %{a: 1, b: [2, %{c: 3}], d: 4}
      result = Sequin.Enum.transform_deeply(data, fn x -> x * 2 || x end)
      assert result == %{a: 2, b: [4, %{c: 6}], d: 8}
    end

    test "handles empty maps and lists" do
      assert Sequin.Enum.transform_deeply(%{}, fn x -> x end) == %{}
      assert Sequin.Enum.transform_deeply([], fn x -> x end) == []
    end

    test "handles nil values" do
      assert Sequin.Enum.transform_deeply(nil, fn x -> (is_nil(x) && :transformed) || x end) == :transformed
    end

    test "transforms DateTime structs" do
      datetime = DateTime.from_naive!(~N[2023-01-01 00:00:00], "Etc/UTC")

      result =
        Sequin.Enum.transform_deeply(datetime, fn
          %DateTime{} -> :datetime_transformed
          other -> other
        end)

      assert result == :datetime_transformed
    end

    test "transforms fields within structs" do
      struct = %TestStruct{a: 1, b: 2}

      result =
        Sequin.Enum.transform_deeply(struct, fn
          x when is_integer(x) -> x * 2
          other -> other
        end)

      assert result == %TestStruct{a: 2, b: 4}
    end

    test "transforms complex nested structures with structs" do
      datetime = DateTime.from_naive!(~N[2023-01-01 00:00:00], "Etc/UTC")

      data = %{
        a: 1,
        b: [2, 3],
        c: %ComplexStruct{
          value: 4,
          nested: %{
            d: 5,
            e: datetime
          }
        }
      }

      result =
        Sequin.Enum.transform_deeply(data, fn
          %DateTime{} -> :datetime_transformed
          x when is_integer(x) -> x * 2
          other -> other
        end)

      expected = %{
        a: 2,
        b: [4, 6],
        c: %ComplexStruct{
          value: 8,
          nested: %{
            d: 10,
            e: :datetime_transformed
          }
        }
      }

      assert result == expected
    end

    test "transforms values in a DateTime map" do
      datetime = DateTime.from_naive!(~N[2023-01-01 00:00:00], "Etc/UTC")

      data = %{
        timestamp: datetime,
        other: "value",
        nested: %{
          another_timestamp: datetime
        }
      }

      unix_timestamp = DateTime.to_unix(datetime, :millisecond)

      result =
        Sequin.Enum.transform_deeply(data, fn
          %DateTime{} = dt -> DateTime.to_unix(dt, :millisecond)
          other -> other
        end)

      expected = %{
        timestamp: unix_timestamp,
        other: "value",
        nested: %{
          another_timestamp: unix_timestamp
        }
      }

      assert result == expected
    end
  end
end
