defmodule Sequin.DatabasesRuntime.KeysetCursor do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.Postgres

  @spec attnums_to_names(Table.t(), map()) :: map()
  def attnums_to_names(%Table{} = table, cursor) do
    Map.new(cursor, fn {attnum, value} ->
      name =
        table.columns
        |> Sequin.Enum.find!(fn column -> column.attnum == attnum end)
        |> Map.fetch!(:name)

      {name, value}
    end)
  end

  @spec cursor_columns(Table.t()) :: [Table.Column.t()]
  def cursor_columns(%Table{sort_column_attnum: sort_column_attnum} = table) when not is_nil(sort_column_attnum) do
    sort_column = Sequin.Enum.find!(table.columns, fn column -> column.attnum == sort_column_attnum end)
    sorted_pks = table.columns |> Enum.filter(& &1.is_pk?) |> Enum.sort_by(& &1.attnum)

    if sort_column.is_pk? do
      [sort_column | Enum.filter(sorted_pks, &(&1.attnum != sort_column_attnum))]
    else
      [sort_column | sorted_pks]
    end
  end

  @spec where_sql(Table.t(), String.t()) :: String.t()
  def where_sql(%Table{} = table, operator) do
    columns = cursor_columns(table)

    quoted_columns =
      columns
      |> quoted_column_names()
      |> Enum.join(", ")

    lhs = "(#{quoted_columns})"
    rhs = "(#{Enum.map_join(1..Enum.count(columns), ", ", fn _ -> "?" end)})"
    "#{lhs} #{operator} #{rhs}"
  end

  @spec order_by_sql(Table.t(), String.t()) :: String.t()
  def order_by_sql(%Table{} = table, direction \\ "asc") do
    table
    |> cursor_columns()
    |> quoted_column_names()
    |> Enum.map_join(", ", &"#{&1} #{direction}")
  end

  @spec casted_cursor_values(Table.t(), map()) :: [any()]
  def casted_cursor_values(%Table{} = table, cursor) do
    columns = cursor_columns(table)

    Enum.map(columns, fn %Table.Column{} = column ->
      val = Map.fetch!(cursor, column.attnum)

      cond do
        column.type == "uuid" ->
          UUID.string_to_binary!(val)

        column.type == "timestamp without time zone" and is_binary(val) ->
          NaiveDateTime.from_iso8601!(val)

        column.type == "timestamp with time zone" and is_binary(val) ->
          {:ok, dt, _offset} = DateTime.from_iso8601(val)
          dt

        true ->
          val
      end
    end)
  end

  @doc """
  Result is the result of fetching the cursor column values of a single row from the database
  """
  @spec cursor_from_result(Table.t(), Postgrex.Result.t()) :: map()
  def cursor_from_result(%Table{} = table, %Postgrex.Result{num_rows: 1} = result) do
    cursor_columns = cursor_columns(table)
    result = result |> Postgres.result_to_maps() |> List.first()

    Map.new(cursor_columns, fn %Table.Column{} = column ->
      value = Map.fetch!(result, column.name)

      value =
        if column.type == "uuid" do
          UUID.binary_to_string!(value)
        else
          value
        end

      {column.attnum, value}
    end)
  end

  defp quoted_column_names(cursor_columns) do
    Enum.map(cursor_columns, fn %Table.Column{} = col -> Postgres.quote_name(col.name) end)
  end
end
