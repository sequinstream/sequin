defmodule Sequin.DatabasesRuntime.TableProducer do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable, as: Table
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Postgres

  require Logger

  # Cursor
  def fetch_cursors(backfill_id) do
    case Redix.command(:redix, ["HGETALL", cursor_key(backfill_id)]) do
      {:ok, []} ->
        :error

      {:ok, result} ->
        r = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, decode_cursor(v)} end)

        {:ok, r}

      error ->
        error
    end
  end

  def cursor(backfill_id, type) when type in [:min, :max] do
    case Redix.command(:redix, ["HGET", cursor_key(backfill_id), type]) do
      {:ok, nil} -> nil
      {:ok, cursor} -> decode_cursor(cursor)
    end
  end

  def update_cursor(backfill_id, type, cursor) when type in [:min, :max] do
    with {:ok, _} <- Redix.command(:redix, ["HSET", cursor_key(backfill_id), type, Jason.encode!(cursor)]) do
      :ok
    end
  end

  defp decode_cursor(json) do
    json
    |> Jason.decode!()
    |> Map.new(fn {k, v} -> {String.to_integer(k), v} end)
  end

  def delete_cursor(backfill_id) do
    case fetch_cursors(backfill_id) do
      {:ok, cursors} ->
        Logger.info("[TableProducer] Deleting cursors for backfill #{backfill_id}", cursors)
        {:ok, _} = Redix.command(:redix, ["DEL", cursor_key(backfill_id)])
        :ok

      :error ->
        :ok
    end
  end

  def clean_test_keys do
    if env() == :test do
      pattern = "sequin:test:table_producer:cursor:*"

      case Redix.command(:redix, ["KEYS", pattern]) do
        {:ok, []} ->
          :ok

        {:ok, keys} ->
          case Redix.command(:redix, ["DEL" | keys]) do
            {:ok, _} -> :ok
            {:error, error} -> raise error
          end
      end
    end
  end

  defp cursor_key(backfill_id) do
    "sequin:#{env()}:table_producer:cursor:#{backfill_id}"
  end

  # Queries
  def fetch_records_in_range(db, table, min_cursor, max_cursor, opts \\ [])

  def fetch_records_in_range(_db, _table, min_cursor, max_cursor, _opts)
      when map_size(max_cursor) == 0 or is_nil(max_cursor) or map_size(min_cursor) == 0 or is_nil(min_cursor),
      do: raise(ArgumentError, "cursors cannot be empty")

  def fetch_records_in_range(%PostgresDatabase{} = db, %Table{} = table, min_cursor, max_cursor, opts) do
    order_by_clause = KeysetCursor.order_by_sql(table)

    include_min = Keyword.get(opts, :include_min, false)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
    min_cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    max_where_clause = KeysetCursor.where_sql(table, "<=")
    max_cursor_values = KeysetCursor.casted_cursor_values(table, max_cursor)

    limit = Keyword.get(opts, :limit, 1000)

    select_columns = Postgres.safe_select_columns(table)

    sql =
      """
      select #{select_columns} from #{Postgres.quote_name(table.schema, table.name)}
      where #{min_where_clause} and #{max_where_clause}
      order by #{order_by_clause}
      limit ?
      """

    sql = Postgres.parameterize_sql(sql)

    # Careful about ordering! Note order of `?` above
    params = min_cursor_values ++ max_cursor_values ++ [limit]

    with {:ok, %Postgrex.Result{} = result} <- Postgres.query(db, sql, params) do
      result = result |> Postgres.result_to_maps() |> Enum.map(&parse_uuids(table.columns, &1))
      {:ok, result}
    end
  end

  def fetch_max_cursor(%PostgresDatabase{} = db, %Table{} = table, min_cursor, opts \\ []) do
    select_columns =
      table
      |> KeysetCursor.cursor_columns()
      |> Enum.map_join(", ", fn %Table.Column{} = col -> Postgres.quote_name(col.name) end)

    {inner_sql, params} = fetch_window_query(table, select_columns, min_cursor, opts)

    sql = """
    with window_data as (
      #{inner_sql}
    )
    select #{select_columns}
    from window_data
    where row_num = (select max(row_num) from window_data)
    """

    sql = Postgres.parameterize_sql(sql)

    with {:ok, %Postgrex.Result{} = result} <- Postgres.query(db, sql, params) do
      case result.num_rows do
        0 -> {:ok, nil}
        _ -> {:ok, KeysetCursor.cursor_from_result(table, result)}
      end
    end
  end

  defp fetch_window_query(%Table{} = table, select_columns, min_cursor, opts) do
    limit = Keyword.get(opts, :limit, 1000)
    include_min = Keyword.get(opts, :include_min, false)
    order_by = KeysetCursor.order_by_sql(table)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
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
      Map.update(acc, column.name, nil, fn
        nil ->
          nil

        uuid_string ->
          case Sequin.String.binary_to_string(uuid_string) do
            {:ok, uuid} ->
              uuid

            :error ->
              Logger.error("[TableProducer] Invalid UUID: #{inspect(uuid_string)}", column: column, columns: columns)
              raise "Got invalid UUID: #{inspect(uuid_string)} for column: #{column.name}"
          end
      end)
    end)
  end

  # Fetch first row
  # Can be used to both validate the sort column, show the user where we're going to start the process,
  # and initialize the min cursor
  def fetch_first_row(%PostgresDatabase{} = db, %Table{} = table) do
    order_by = KeysetCursor.order_by_sql(table, "asc")

    select_columns = Postgres.safe_select_columns(table)

    sql = """
    select #{select_columns} from #{Postgres.quote_name(table.schema, table.name)}
    order by #{order_by}
    limit 1
    """

    sql = Postgres.parameterize_sql(sql)

    with {:ok, %Postgrex.Result{} = result} <- Postgres.query(db, sql, []) do
      case result.num_rows do
        0 ->
          {:ok, nil, nil}

        _ ->
          [row] = result |> Postgres.result_to_maps() |> Enum.map(&parse_uuids(table.columns, &1))
          {:ok, row, KeysetCursor.cursor_from_result(table, result)}
      end
    end
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  # Add this new function
  def fast_count_estimate(%PostgresDatabase{} = db, %Table{} = table, min_cursor, opts \\ []) do
    include_min = Keyword.get(opts, :include_min, false)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
    cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    sql = """
    SELECT reltuples::bigint AS estimate
    FROM pg_class
    WHERE oid = '#{Postgres.quote_name(table.schema, table.name)}'::regclass
    """

    # If we have a where clause, we need to adjust the estimate
    sql =
      """
      WITH total AS (#{sql}),
      filtered AS (
        SELECT count(*) as actual_count
        FROM #{Postgres.quote_name(table.schema, table.name)}
        WHERE #{min_where_clause}
      )
      SELECT actual_count FROM filtered
      """

    sql = Postgres.parameterize_sql(sql)

    with {:ok, %Postgrex.Result{rows: [[count]]}} <- Postgres.query(db, sql, cursor_values) do
      {:ok, count}
    end
  end
end
