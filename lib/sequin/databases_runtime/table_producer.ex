defmodule Sequin.DatabasesRuntime.TableProducer do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Postgres

  require Logger

  # Cursor
  def fetch_cursors(consumer_id) do
    case Redix.command(:redix, ["HGETALL", cursor_key(consumer_id)]) do
      {:ok, []} ->
        :error

      {:ok, result} ->
        r = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, Jason.decode!(v)} end)
        {:ok, r}

      error ->
        error
    end
  end

  def cursor(consumer_id, type) when type in [:min, :max] do
    case Redix.command(:redix, ["HGET", cursor_key(consumer_id), type]) do
      {:ok, nil} -> nil
      {:ok, cursor} -> Jason.decode!(cursor)
    end
  end

  def update_cursor(consumer_id, type, cursor) when type in [:min, :max] do
    with {:ok, _} <- Redix.command(:redix, ["HSET", cursor_key(consumer_id), type, Jason.encode!(cursor)]) do
      :ok
    end
  end

  def delete_cursor(consumer_id) do
    case fetch_cursors(consumer_id) do
      {:ok, cursors} ->
        Logger.info("[TableProducer] Deleting cursors for consumer #{consumer_id}", cursors)
        Redix.command(:redix, ["DEL", cursor_key(consumer_id)])

      :error ->
        :ok
    end
  end

  defp cursor_key(consumer_id) do
    "table_producer:cursor:#{consumer_id}"
  end

  # Queries
  def fetch_records_in_range(_conn, _table, _min, max_cursor, _limit)
      when map_size(max_cursor) == 0 or is_nil(max_cursor),
      do: raise(ArgumentError, "max cursor cannot be empty")

  def fetch_records_in_range(conn, %Table{} = table, min_cursor, max_cursor, limit) do
    order_by_clause = KeysetCursor.order_by_sql(table)

    max_where_clause = KeysetCursor.where_sql(table, "<=")
    max_cursor_values = KeysetCursor.casted_cursor_values(table, max_cursor)

    {min_where_clause, min_cursor_values} =
      if is_nil(min_cursor) do
        {"", []}
      else
        values = KeysetCursor.casted_cursor_values(table, min_cursor)
        {" and " <> KeysetCursor.where_sql(table, ">"), values}
      end

    sql =
      """
      select * from #{Postgres.quote_name(table.schema, table.name)}
      where #{max_where_clause}#{min_where_clause}
      order by #{order_by_clause}
      limit ?
      """

    sql = Postgres.parameterize_sql(sql)

    # Careful about ordering! Note order of `?` above
    params = max_cursor_values ++ min_cursor_values ++ [limit]

    with {:ok, %Postgrex.Result{} = result} <- Postgrex.query(conn, sql, params) do
      result = result |> Postgres.result_to_maps() |> Enum.map(&parse_uuids(table.columns, &1))
      {:ok, result}
    end
  end

  def fetch_max_cursor(conn, %Table{} = table, min_cursor, limit) do
    select_columns =
      table
      |> KeysetCursor.cursor_columns()
      |> Enum.map_join(", ", fn %Table.Column{} = col -> Postgres.quote_name(col.name) end)

    {inner_sql, params} = fetch_window_query(table, select_columns, min_cursor, limit)

    sql = """
    with window_data as (
      #{inner_sql}
    )
    select #{select_columns}
    from window_data
    where row_num = (select max(row_num) from window_data)
    """

    sql = Postgres.parameterize_sql(sql)

    with {:ok, %Postgrex.Result{} = result} <- Postgrex.query(conn, sql, params) do
      case result.num_rows do
        0 -> {:ok, nil}
        _ -> {:ok, KeysetCursor.cursor_from_result(table, result)}
      end
    end
  end

  defp fetch_window_query(%Table{} = table, select_columns, nil, limit) do
    order_by = KeysetCursor.order_by_sql(table)

    sql = """
    select #{select_columns}, row_number() over (order by #{order_by}) as row_num
    from #{Postgres.quote_name(table.schema, table.name)}
    order by #{order_by}
    limit ?
    """

    {sql, [limit]}
  end

  defp fetch_window_query(%Table{} = table, select_columns, min_cursor, limit) do
    order_by = KeysetCursor.order_by_sql(table)
    min_where_clause = KeysetCursor.where_sql(table, ">")
    cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    sql = """
    select #{select_columns}, row_number() over (order by #{order_by}) as row_num
    from #{Postgres.quote_name(table.schema, table.name)}
    where #{min_where_clause}
    order by #{order_by}
    limit ?
    """

    {sql, cursor_values ++ [limit]}
  end

  defp parse_uuids(columns, map) do
    uuid_columns = Enum.filter(columns, &(&1.type == "uuid"))

    Enum.reduce(uuid_columns, map, fn column, acc ->
      Map.update(acc, column.name, nil, &UUID.binary_to_string!/1)
    end)
  end
end
