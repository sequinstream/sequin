defmodule Sequin.DatabasesRuntime.TableReader do
  @moduledoc false
  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.Redis

  require Logger

  @type batch_id :: String.t()
  @type primary_key_list :: [[String.t()]]

  @callback cursor(backfill_id :: String.t()) :: KeysetCursor.cursor() | nil

  @callback fetch_batch_primary_keys(
              db_or_conn :: Postgres.db_conn(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor()
            ) :: {:ok, primary_key_list()} | {:error, any()}

  @callback fetch_batch_primary_keys(
              db_or_conn :: Postgres.db_conn(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor(),
              opts :: Keyword.t()
            ) :: {:ok, primary_key_list()} | {:error, any()}

  @callback fetch_batch_by_primary_keys(
              db_or_conn :: Postgres.db_conn(),
              consumer :: SinkConsumer.t(),
              table :: PostgresDatabaseTable.t(),
              primary_keys :: primary_key_list()
            ) ::
              {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor()}}
              | {:error, any()}

  @callback fetch_batch_by_primary_keys(
              db_or_conn :: Postgres.db_conn(),
              consumer :: SinkConsumer.t(),
              table :: PostgresDatabaseTable.t(),
              primary_keys :: primary_key_list(),
              opts :: Keyword.t()
            ) ::
              {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor()}}
              | {:error, any()}

  @callback emit_logic_message(db :: Postgres.db_conn(), consumer_id :: SinkConsumer.id()) ::
              {:ok, lsn :: non_neg_integer()} | {:error, any()}

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
        Logger.info("[TableReader] Deleting cursors for backfill #{backfill_id}", cursors)
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

        {:ok, keys} when is_list(keys) ->
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
  @emit_logical_message_sql "select pg_logical_emit_message(true, $1, $2)"
  def with_watermark(%PostgresDatabase{} = db, backfill_id, current_batch_id, table_oid, fun) do
    payload =
      Jason.encode!(%{
        table_oid: table_oid,
        batch_id: current_batch_id,
        backfill_id: backfill_id
      })

    with {:ok, conn} <- ConnectionCache.connection(db),
         Logger.debug("[TableReader] Emitting low watermark for batch #{current_batch_id}"),
         {:ok, _} <-
           Postgres.query(conn, @emit_logical_message_sql, [
             Constants.backfill_batch_low_watermark(),
             payload
           ]),
         {:ok, res} <- fun.(conn),
         Logger.debug("[TableReader] Emitting high watermark for batch #{current_batch_id}"),
         {:ok, %{rows: [[lsn]]}} <-
           Postgres.query(conn, @emit_logical_message_sql, [
             Constants.backfill_batch_high_watermark(),
             payload
           ]) do
      {:ok, res, lsn}
    end
  end

  def fetch_batch(db_or_conn, %PostgresDatabaseTable{} = table, min_cursor, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)
    include_min = Keyword.get(opts, :include_min, false)
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

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

    case Postgres.query(db_or_conn, sql, params, timeout: timeout) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, %{rows: [], next_cursor: nil}}

      {:ok, %Postgrex.Result{} = result} ->
        rows = Postgres.result_to_maps(result)
        rows = Postgres.load_rows(table, rows)

        last_row = List.last(rows)

        next_cursor =
          case rows do
            [] -> nil
            _records -> KeysetCursor.cursor_from_row(table, last_row)
          end

        {:ok, %{rows: rows, next_cursor: next_cursor}}

      error ->
        error
    end
  end

  @spec fetch_batch_primary_keys(
          db_or_conn :: Postgres.db_conn(),
          table :: PostgresDatabaseTable.t(),
          min_cursor :: KeysetCursor.cursor(),
          opts :: Keyword.t()
        ) :: {:ok, primary_key_list()} | {:error, any()}
  def fetch_batch_primary_keys(db_or_conn, %PostgresDatabaseTable{} = table, min_cursor, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)
    include_min = Keyword.get(opts, :include_min, false)
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

    order_by = KeysetCursor.order_by_sql(table)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
    cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    primary_key_columns =
      table.columns
      |> Enum.filter(& &1.is_pk?)
      |> Enum.sort_by(& &1.attnum)

    primary_key_select_statement = Enum.map_join(primary_key_columns, ", ", &Postgres.quote_name(&1.name))

    sql = """
    select #{primary_key_select_statement}
    from #{Postgres.quote_name(table.schema, table.name)}
    where #{min_where_clause}
    order by #{order_by}
    limit ?
    """

    sql = Postgres.parameterize_sql(sql)
    params = cursor_values ++ [limit]

    case Postgres.query(db_or_conn, sql, params, timeout: timeout) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, %{rows: [], next_cursor: nil}}

      {:ok, %Postgrex.Result{} = result} ->
        rows = Postgres.result_to_maps(result)

        rows =
          table
          |> Postgres.load_rows(rows)
          |> Enum.map(fn row ->
            primary_key_columns
            |> Enum.map(fn column -> Map.fetch!(row, column.name) end)
            |> Enum.map(&to_string/1)
          end)

        {:ok, rows}

      error ->
        error
    end
  end

  # primary_keys is a list of lists, where each inner list contains the primary key values for a row
  # To select multiple rows by one or more primary keys apiece, we use unnest like so:
  #
  # select *
  # from your_table
  # where (column1, column2) in (
  #   select *
  #   from unnest($1, $2)
  # );
  # where unnest behaves like this:
  # select *
  # from unnest(array[3, 5], array[4, 6]);
  #
  # unnest | unnest
  # --------+--------
  #       3 |      4
  #       5 |      6
  #
  # So, $1 in unnest is the list of all the first primary key values across all rows,
  # $2 is the list of all the second primary key values across all rows, etc.
  @spec fetch_batch_by_primary_keys(
          db_or_conn :: Postgres.db_conn(),
          consumer :: SinkConsumer.t(),
          table :: PostgresDatabaseTable.t(),
          primary_keys :: primary_key_list(),
          opts :: Keyword.t()
        ) ::
          {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor()}}
          | {:error, any()}
  def fetch_batch_by_primary_keys(
        db_or_conn,
        %SinkConsumer{} = consumer,
        %PostgresDatabaseTable{} = table,
        primary_keys,
        opts \\ []
      ) do
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

    order_by = KeysetCursor.order_by_sql(table)

    primary_key_columns =
      table.columns
      |> Enum.filter(& &1.is_pk?)
      |> Enum.sort_by(& &1.attnum)

    select_columns = Postgres.safe_select_columns(table)
    id_col_names = Enum.map_join(primary_key_columns, ", ", &Postgres.quote_name(&1.name))
    unnest_params = Enum.map_join(primary_key_columns, ", ", fn column -> "?::#{column.type}[]" end)

    sql = """
    select #{select_columns}
    from #{Postgres.quote_name(table.schema, table.name)}
    where (#{id_col_names}) in (select * from unnest(#{unnest_params}))
    order by #{order_by}
    """

    sql = Postgres.parameterize_sql(sql)
    # create the lists that will be used in unnest
    # Enum.zip() will create a list of tuples, where the first tuple contains the first primary key values across all rows, etc
    params =
      primary_keys
      |> Enum.map(fn primary_keys ->
        primary_keys
        |> Enum.zip(primary_key_columns)
        |> Enum.map(fn {value, column} ->
          Postgres.cast_value(value, column.type)
        end)
      end)
      |> Enum.zip()
      |> Enum.map(&Tuple.to_list/1)

    case Postgres.query(db_or_conn, sql, params, timeout: timeout) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, %{rows: [], next_cursor: nil}}

      {:ok, %Postgrex.Result{} = result} ->
        rows = Postgres.result_to_maps(result)
        rows = Postgres.load_rows(table, rows)

        last_row = List.last(rows)

        next_cursor =
          case rows do
            [] -> nil
            _records -> KeysetCursor.cursor_from_row(table, last_row)
          end

        messages =
          table
          |> records_by_column_attnum(rows)
          |> Enum.filter(&Consumers.matches_record?(consumer, table.oid, &1))
          |> Enum.map(&message_from_row(consumer, table, &1))

        {:ok, %{messages: messages, next_cursor: next_cursor}}

      error ->
        error
    end
  end

  # Fetch first row
  # Can be used to both validate the sort column, show the user where we're going to start the process,
  # and initialize the min cursor
  def fetch_first_row(%PostgresDatabase{} = db, %PostgresDatabaseTable{} = table) do
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

  defp message_from_row(%SinkConsumer{message_kind: :record} = consumer, %PostgresDatabaseTable{} = table, record) do
    data = build_record_data(table, consumer, record)

    %ConsumerRecord{
      consumer_id: consumer.id,
      table_oid: table.oid,
      record_pks: record_pks(table, record),
      group_id: generate_group_id(consumer, table, record),
      replication_message_trace_id: UUID.uuid4(),
      data: data,
      payload_size_bytes: :erlang.external_size(data)
    }
  end

  defp message_from_row(%SinkConsumer{message_kind: :event} = consumer, %PostgresDatabaseTable{} = table, record) do
    data = build_event_data(table, consumer, record)

    %ConsumerEvent{
      consumer_id: consumer.id,
      record_pks: record_pks(table, record),
      group_id: generate_group_id(consumer, table, record),
      table_oid: table.oid,
      deliver_count: 0,
      replication_message_trace_id: UUID.uuid4(),
      data: data,
      payload_size_bytes: :erlang.external_size(data)
    }
  end

  defp build_record_data(table, consumer, record_attnums_to_values) do
    %ConsumerRecordData{
      action: :read,
      record: build_record_payload(table, record_attnums_to_values),
      metadata: %ConsumerRecordData.Metadata{
        database_name: consumer.replication_slot.postgres_database.name,
        table_name: table.name,
        table_schema: table.schema,
        consumer: %{
          id: consumer.id,
          name: consumer.name,
          inserted_at: consumer.inserted_at,
          updated_at: consumer.updated_at
        },
        commit_timestamp: DateTime.utc_now()
      }
    }
  end

  defp build_event_data(table, consumer, record_attnums_to_values) do
    %ConsumerEventData{
      action: :read,
      record: build_record_payload(table, record_attnums_to_values),
      metadata: %ConsumerEventData.Metadata{
        database_name: consumer.replication_slot.postgres_database.name,
        table_name: table.name,
        table_schema: table.schema,
        consumer: %{
          id: consumer.id,
          name: consumer.name,
          inserted_at: consumer.inserted_at,
          updated_at: consumer.updated_at
        },
        commit_timestamp: DateTime.utc_now()
      }
    }
  end

  defp record_pks(%PostgresDatabaseTable{} = table, record_attnums_to_values) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.sort_by(& &1.attnum)
    |> Enum.map(&Map.fetch!(record_attnums_to_values, &1.attnum))
    |> Enum.map(&to_string/1)
  end

  defp build_record_payload(table, record_attnums_to_values) do
    Map.new(table.columns, fn column -> {column.name, Map.get(record_attnums_to_values, column.attnum)} end)
  end

  defp records_by_column_attnum(%PostgresDatabaseTable{} = table, records) do
    Enum.map(records, fn record ->
      Map.new(table.columns, fn %PostgresDatabaseTable.Column{} = column ->
        {column.attnum, Map.get(record, column.name)}
      end)
    end)
  end

  defp generate_group_id(consumer, table, record_attnums_to_values) do
    group_column_attnums = group_column_attnums(consumer)

    if group_column_attnums do
      Enum.map_join(group_column_attnums, ",", fn attnum ->
        to_string(Map.get(record_attnums_to_values, attnum))
      end)
    else
      table |> record_pks(record_attnums_to_values) |> Enum.join(",")
    end
  end

  defp group_column_attnums(%{sequence_filter: %SequenceFilter{group_column_attnums: group_column_attnums}}) do
    group_column_attnums
  end

  defp group_column_attnums(%{source_tables: [source_table | _]}), do: source_table.group_column_attnums

  def emit_logic_message(%PostgresDatabase{} = db, consumer_id) do
    Postgres.emit_logical_message(
      db,
      Constants.backfill_batch_watermark(),
      Jason.encode!(%{consumer_id: consumer_id})
    )
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  # Add this new function
  def fast_count_estimate(%PostgresDatabase{} = db, %PostgresDatabaseTable{} = table, min_cursor, opts \\ []) do
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

    with {:ok, %Postgrex.Result{rows: [[count]]}} <- Postgres.query(db, sql, cursor_values, timeout: :timer.minutes(1)) do
      {:ok, count}
    end
  end

  # Add new function to fetch slot LSN
  def fetch_slot_lsn(%PostgresDatabase{} = db, slot_name) do
    sql = """
    select confirmed_flush_lsn::pg_lsn
    from pg_replication_slots
    where slot_name = $1
    """

    with {:ok, conn} <- ConnectionCache.connection(db),
         {:ok, %{rows: [[lsn]]}} <- Postgres.query(conn, sql, [slot_name]) do
      {:ok, lsn}
    else
      {:ok, %{rows: []}} -> {:error, Error.not_found(entity: :replication_slot, params: %{name: slot_name})}
      error -> error
    end
  end
end
