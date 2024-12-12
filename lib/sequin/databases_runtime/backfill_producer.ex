defmodule Sequin.DatabasesRuntime.BackfillProducer do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable, as: Table
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Postgres
  alias Sequin.Redis

  require Logger

  # Cursor
  # Note: We previously had min and max cursors. We're switching to just use min cursors.
  # We are keeping around the HSET data structure for now to allow for backwards compatibility.
  def fetch_cursors(backfill_id) do
    case Redis.command(["HGETALL", cursor_key(backfill_id)]) do
      {:ok, []} ->
        :error

      {:ok, result} ->
        r = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, decode_cursor(v)} end)

        {:ok, r}

      error ->
        error
    end
  end

  def cursor(backfill_id) do
    case Redis.command(["HGET", cursor_key(backfill_id), "min"]) do
      {:ok, nil} -> nil
      {:ok, cursor} -> decode_cursor(cursor)
    end
  end

  def update_cursor(backfill_id, cursor) do
    with {:ok, _} <- Redis.command(["HSET", cursor_key(backfill_id), "min", Jason.encode!(cursor)]) do
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
        Logger.info("[BackfillProducer] Deleting cursors for backfill #{backfill_id}", cursors)
        {:ok, _} = Redis.command(["DEL", cursor_key(backfill_id)])
        :ok

      :error ->
        :ok
    end
  end

  def clean_test_keys do
    if env() == :test do
      pattern = "sequin:test:table_producer:cursor:*"

      case Redis.command(["KEYS", pattern]) do
        {:ok, []} ->
          :ok

        {:ok, keys} ->
          case Redis.command(["DEL" | keys]) do
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
  def with_watermark(%PostgresDatabase{} = db, current_batch_id, table_oid, fun) do
    table_and_batch = "#{table_oid}:#{current_batch_id}"

    res =
      Postgres.transaction(db, fn t_conn ->
        Postgres.query(t_conn, "select pg_logical_emit_message(true, 'backfill_batch_wm_start', $1)", [table_and_batch])
        res = fun.(t_conn)
        Postgres.query(t_conn, "select pg_logical_emit_message(true, 'backfill_batch_wm_end', $1)", [table_and_batch])
        res
      end)

    case res do
      {:ok, res} -> res
      error -> error
    end
  end

  def fetch_batch(db_or_conn, %Table{} = table, min_cursor, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)
    include_min = Keyword.get(opts, :include_min, false)

    order_by = KeysetCursor.order_by_sql(table)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
    cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    select_columns = Postgres.safe_select_columns(table)

    sql = """
    select #{select_columns}
    from #{Postgres.quote_name(table.schema, table.name)}
    where #{min_where_clause}
    order by #{order_by}
    limit ?
    """

    sql = Postgres.parameterize_sql(sql)
    params = cursor_values ++ [limit]

    case Postgres.query(db_or_conn, sql, params) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, [], nil}

      {:ok, %Postgrex.Result{} = result} ->
        rows = Postgres.result_to_maps(result)
        rows = Postgres.load_rows(table, rows)

        last_row = List.last(rows)

        next_cursor =
          case rows do
            [] -> nil
            _records -> KeysetCursor.cursor_from_row(table, last_row)
          end

        {:ok, rows, next_cursor}

      error ->
        error
    end
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
          [row] = Postgres.load_rows(table, Postgres.result_to_maps(result))
          {:ok, row, KeysetCursor.cursor_from_row(table, row)}
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
