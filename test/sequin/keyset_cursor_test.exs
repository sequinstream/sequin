defmodule Sequin.Runtime.KeysetCursorTest do
  use ExUnit.Case, async: true

  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Runtime.KeysetCursor

  @sort_column_attnum 1

  describe "where_sql/2" do
    test "generates correct string with sort column and two PKs" do
      table = create_test_table()
      result = KeysetCursor.where_sql(table, ">")

      assert result == ~s{("updated_at", "id1", "id2") > (?, ?, ?)}
    end

    test "handles different column names and attnums" do
      table =
        create_test_table([
          {"modified_at", "timestamp", @sort_column_attnum, false},
          {"uuid_col", "uuid", 2, true},
          {"big_id", "bigint", 3, true}
        ])

      result = KeysetCursor.where_sql(table, "<")

      assert result == ~s{("modified_at", "uuid_col", "big_id") < (?, ?, ?)}
    end

    test "handles sort column as primary key" do
      table =
        create_test_table([
          {"id1", "bigint", @sort_column_attnum, true},
          {"id2", "uuid", 2, true},
          {"updated_at", "timestamp", 3, false}
        ])

      result = KeysetCursor.where_sql(table, ">")

      assert result == ~s{("id1", "id2") > (?, ?)}
    end
  end

  describe "order_by_sql/2" do
    test "generates correct string with default ascending order" do
      table = create_test_table()
      result = KeysetCursor.order_by_sql(table)

      assert result == ~s("updated_at" asc, "id1" asc, "id2" asc)
    end

    test "generates correct string with descending order" do
      table = create_test_table()
      result = KeysetCursor.order_by_sql(table, "desc")

      assert result == ~s("updated_at" desc, "id1" desc, "id2" desc)
    end

    test "handles sort column as primary key" do
      table =
        create_test_table([
          {"id1", "bigint", @sort_column_attnum, true},
          {"id2", "uuid", 2, true}
        ])

      result = KeysetCursor.order_by_sql(table)

      assert result == ~s("id1" asc, "id2" asc)
    end
  end

  describe "casted_cursor_values/2" do
    test "converts cursor keyed by attnum to ordered list of values" do
      table = create_test_table()

      cursor = %{
        1 => "2023-01-01T00:00:00",
        2 => 123,
        3 => "550e8400-e29b-41d4-a716-446655440000"
      }

      result = KeysetCursor.casted_cursor_values(table, cursor)

      assert result == [
               ~N[2023-01-01 00:00:00],
               123,
               Sequin.String.string_to_binary!("550e8400-e29b-41d4-a716-446655440000")
             ]
    end
  end

  describe "attnums_to_names/2" do
    test "converts cursor with attnums to cursor with column names" do
      table = create_test_table()

      cursor = %{
        1 => "2023-01-01",
        2 => 123,
        3 => "550e8400-e29b-41d4-a716-446655440000"
      }

      result = KeysetCursor.attnums_to_names(table, cursor)

      assert result == %{
               "updated_at" => "2023-01-01",
               "id1" => 123,
               "id2" => "550e8400-e29b-41d4-a716-446655440000"
             }
    end
  end

  describe "min_cursor/1" do
    test "returns cursor with minimum values for all columns" do
      table = create_test_table()
      result = KeysetCursor.min_cursor(table)

      assert result == %{
               1 => ~N[0001-01-01 00:00:00],
               2 => 0,
               3 => "00000000-0000-0000-0000-000000000000"
             }
    end

    test "handles different column types" do
      table =
        create_test_table([
          {"updated_at", "timestamp with time zone", @sort_column_attnum, false},
          {"id1", "numeric", 2, true},
          {"id2", "text", 3, true}
        ])

      result = KeysetCursor.min_cursor(table)

      assert result == %{
               1 => ~U[0001-01-01 00:00:00Z],
               2 => 0,
               3 => ""
             }
    end
  end

  describe "min_cursor/2" do
    test "returns cursor with specified minimum sort column value" do
      table = create_test_table()
      min_sort_value = ~N[2023-01-01 00:00:00]
      result = KeysetCursor.min_cursor(table, min_sort_value)

      assert result == %{
               1 => ~N[2023-01-01 00:00:00],
               2 => 0,
               3 => "00000000-0000-0000-0000-000000000000"
             }
    end
  end

  describe "cursor_from_row/2" do
    test "converts row map to cursor" do
      table = create_test_table()

      row = %{
        "updated_at" => ~N[2023-01-01 00:00:00],
        "id1" => 123,
        "id2" => "550e8400-e29b-41d4-a716-446655440000"
      }

      result = KeysetCursor.cursor_from_row(table, row)

      assert result == %{
               1 => ~N[2023-01-01 00:00:00],
               2 => 123,
               3 => "550e8400-e29b-41d4-a716-446655440000"
             }
    end

    test "handles nil UUID values" do
      table = create_test_table()

      row = %{
        "updated_at" => ~N[2023-01-01 00:00:00],
        "id1" => 123,
        "id2" => nil
      }

      result = KeysetCursor.cursor_from_row(table, row)

      assert result == %{
               1 => ~N[2023-01-01 00:00:00],
               2 => 123,
               3 => nil
             }
    end
  end

  defp create_test_table(
         columns \\ [
           {"updated_at", "timestamp", @sort_column_attnum, false},
           {"id1", "bigint", 2, true},
           {"id2", "uuid", 3, true}
         ]
       ) do
    columns =
      Enum.map(columns, fn {name, type, attnum, is_pk?} ->
        DatabasesFactory.column(%{
          name: name,
          type: type,
          attnum: attnum,
          is_pk?: is_pk?
        })
      end)

    DatabasesFactory.table(%{columns: columns, sort_column_attnum: @sort_column_attnum})
  end
end
