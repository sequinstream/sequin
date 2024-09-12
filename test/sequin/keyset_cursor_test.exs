defmodule Sequin.Consumers.KeysetCursorTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.KeysetCursor
  alias Sequin.Factory.DatabasesFactory

  describe "where/3" do
    test "generates correct string with sort column and two PKs" do
      table = create_test_table()
      sort_column_attnum = Enum.find(table.columns, &(&1.name == "updated_at")).attnum
      result = KeysetCursor.where_sql(table, sort_column_attnum, ">")

      assert result == ~s{("updated_at", "id1", "id2") > (?, ?, ?)}
    end

    test "handles different column names and attnums" do
      table =
        create_test_table([
          {"modified_at", "timestamp", 3, false},
          {"uuid_col", "uuid", 1, true},
          {"big_id", "bigint", 2, true}
        ])

      sort_column_attnum = Enum.find(table.columns, &(&1.name == "modified_at")).attnum
      result = KeysetCursor.where_sql(table, sort_column_attnum, "<")

      assert result == ~s{("modified_at", "uuid_col", "big_id") < (?, ?, ?)}
    end

    test "handles sort column as primary key" do
      table =
        create_test_table([
          {"id1", "bigint", 1, true},
          {"id2", "uuid", 2, true},
          {"updated_at", "timestamp", 3, false}
        ])

      sort_column_attnum = Enum.find(table.columns, &(&1.name == "id1")).attnum
      result = KeysetCursor.where_sql(table, sort_column_attnum, ">")

      assert result == ~s{("id1", "id2") > (?, ?)}
    end
  end

  describe "order_by/3" do
    test "generates correct string with default ascending order" do
      table = create_test_table()
      sort_column_attnum = Enum.find(table.columns, &(&1.name == "updated_at")).attnum
      result = KeysetCursor.order_by_sql(table, sort_column_attnum)

      assert result == ~s("updated_at" asc, "id1" asc, "id2" asc)
    end

    test "generates correct string with descending order" do
      table = create_test_table()
      sort_column_attnum = Enum.find(table.columns, &(&1.name == "updated_at")).attnum
      result = KeysetCursor.order_by_sql(table, sort_column_attnum, "desc")

      assert result == ~s("updated_at" desc, "id1" desc, "id2" desc)
    end

    test "handles sort column as primary key" do
      table =
        create_test_table([
          {"id1", "bigint", 1, true},
          {"id2", "uuid", 2, true},
          {"updated_at", "timestamp", 3, false}
        ])

      sort_column_attnum = Enum.find(table.columns, &(&1.name == "id1")).attnum
      result = KeysetCursor.order_by_sql(table, sort_column_attnum)

      assert result == ~s("id1" asc, "id2" asc)
    end
  end

  describe "attnums_to_names/2" do
    test "converts cursor keyed by attnums to column names" do
      table = create_test_table()
      cursor = %{1 => "2023-01-01", 2 => 123, 3 => "550e8400-e29b-41d4-a716-446655440000"}

      result = KeysetCursor.attnums_to_names(table, cursor)

      assert result == %{
               "updated_at" => "2023-01-01",
               "id1" => 123,
               "id2" => "550e8400-e29b-41d4-a716-446655440000"
             }
    end
  end

  describe "cursor_from_result/3" do
    test "converts Postgrex.Result to cursor keyed by attnums" do
      table = create_test_table()
      sort_column_attnum = Enum.find(table.columns, &(&1.name == "updated_at")).attnum

      result = %Postgrex.Result{
        columns: ["updated_at", "id1", "id2"],
        rows: [["2023-01-01", 123, UUID.string_to_binary!("550e8400-e29b-41d4-a716-446655440000")]],
        num_rows: 1
      }

      cursor = KeysetCursor.cursor_from_result(table, sort_column_attnum, result)

      assert cursor == %{
               1 => "2023-01-01",
               2 => 123,
               3 => "550e8400-e29b-41d4-a716-446655440000"
             }
    end
  end

  defp create_test_table(
         columns \\ [{"updated_at", "timestamp", 1, false}, {"id1", "bigint", 2, true}, {"id2", "uuid", 3, true}]
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

    DatabasesFactory.table(%{columns: columns})
  end
end
