defmodule Sequin.Runtime.TableReader do
  @moduledoc false
  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SchemaFilter
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.Redis
  alias Sequin.Runtime.KeysetCursor

  require Logger

  @type batch_id :: String.t()

  @type primary_key_list :: [[String.t()]]

  @callback cursor(backfill_id :: String.t()) :: KeysetCursor.cursor() | nil

  @callback fetch_batch_pks(
              db_or_conn :: Postgres.db_conn(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor()
            ) :: {:ok, %{pks: primary_key_list(), next_cursor: KeysetCursor.cursor() | nil}} | {:error, any()}

  @callback fetch_batch_pks(
              db_or_conn :: Postgres.db_conn(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor(),
              opts :: Keyword.t()
            ) :: {:ok, %{pks: primary_key_list(), next_cursor: KeysetCursor.cursor() | nil}} | {:error, any()}

  @callback fetch_batch(
              db_or_conn :: Postgres.db_conn(),
              consumer :: SinkConsumer.t(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor()
            ) ::
              {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor() | nil}}
              | {:error, any()}

  @callback fetch_batch(
              db_or_conn :: Postgres.db_conn(),
              consumer :: SinkConsumer.t(),
              table :: PostgresDatabaseTable.t(),
              min_cursor :: KeysetCursor.cursor(),
              opts :: Keyword.t()
            ) ::
              {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor() | nil}}
              | {:error, any()}

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
  def with_watermark(%PostgresDatabase{} = db, replication_slot_id, backfill_id, current_batch_id, table_oid, fun) do
    payload =
      Jason.encode!(%{
        table_oid: table_oid,
        batch_id: current_batch_id,
        backfill_id: backfill_id,
        replication_slot_id: replication_slot_id
      })

    with {:ok, conn} <- ConnectionCache.connection(db),
         {:ok, res} <- fun.(conn),
         Logger.debug("[TableReader] Emitting high watermark for batch #{current_batch_id}"),
         {:ok, _} <-
           Postgres.query(conn, @emit_logical_message_sql, [
             Constants.backfill_batch_high_watermark(),
             payload
           ]),
         # Note: We can't trust the LSN returned by pg_logical_emit_message function. For reasons we
         # don't understand yet, we can have a situation where the LSN returned by this function is
         # way different from `pg_current_wal_lsn()` / the LSNs coming from the replication slot.
         {:ok, %{rows: [[appx_lsn]]}} <-
           Postgres.query(conn, "select pg_current_wal_lsn()") do
      {:ok, res, appx_lsn}
    end
  end

  @spec fetch_batch_pks(
          db_or_conn :: Postgres.db_conn(),
          table :: PostgresDatabaseTable.t(),
          min_cursor :: KeysetCursor.cursor(),
          opts :: Keyword.t()
        ) :: {:ok, %{pks: primary_key_list(), next_cursor: KeysetCursor.cursor() | nil}} | {:error, any()}
  def fetch_batch_pks(db_or_conn, %PostgresDatabaseTable{} = table, min_cursor, opts \\ []) do
    # This function is similar to fetch_batch_primary_keys but returns a map with a `pks` key
    # instead of a `rows` key for better semantic clarity. It also uses the optimized query
    # that only selects the necessary columns.
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

    # Add the option to select only primary key and cursor columns
    opts = Keyword.put(opts, :select_only_pk_and_cursor_columns, true)
    {sql, params} = fetch_batch_query(table, min_cursor, opts)

    primary_key_columns =
      table.columns
      |> Enum.filter(& &1.is_pk?)
      |> Enum.sort_by(& &1.attnum)

    case Postgres.query(db_or_conn, sql, params, timeout: timeout) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, %{pks: [], next_cursor: nil}}

      {:ok, %Postgrex.Result{} = result} ->
        rows = Postgres.result_to_maps(result)
        rows = Postgres.load_rows(table, rows)

        primary_key_rows =
          Enum.map(rows, fn row ->
            primary_key_columns |> Enum.map(fn column -> Map.fetch!(row, column.name) end) |> Enum.map(&to_string/1)
          end)

        last_row = List.last(rows)

        next_cursor =
          case rows do
            [] -> nil
            _records -> KeysetCursor.cursor_from_row(table, last_row)
          end

        {:ok, %{pks: primary_key_rows, next_cursor: next_cursor}}

      error ->
        error
    end
  end

  @spec fetch_batch(
          db_or_conn :: Postgres.db_conn(),
          consumer :: SinkConsumer.t(),
          backfill :: Backfill.t(),
          table :: PostgresDatabaseTable.t(),
          min_cursor :: KeysetCursor.cursor(),
          opts :: Keyword.t()
        ) ::
          {:ok, %{messages: [ConsumerRecord.t() | ConsumerEvent.t()], next_cursor: KeysetCursor.cursor() | nil}}
          | {:error, any()}
  def fetch_batch(
        db_or_conn,
        %SinkConsumer{} = consumer,
        %Backfill{} = backfill,
        %PostgresDatabaseTable{} = table,
        min_cursor,
        opts \\ []
      ) do
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))
    {sql, params} = fetch_batch_query(table, min_cursor, opts)

    case Postgres.query(db_or_conn, sql, params, timeout: timeout) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, %{messages: [], next_cursor: nil}}

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
          |> Enum.map(&message_from_row(consumer, backfill, table, &1))

        {:ok, %{messages: messages, next_cursor: next_cursor}}

      error ->
        error
    end
  end

  @spec fetch_batch_query(
          table :: PostgresDatabaseTable.t(),
          min_cursor :: KeysetCursor.cursor(),
          opts :: Keyword.t()
        ) :: {String.t(), list()}
  defp fetch_batch_query(%PostgresDatabaseTable{} = table, min_cursor, opts) do
    limit = Keyword.get(opts, :limit, 1000)
    include_min = Keyword.get(opts, :include_min, false)
    select_only_pk_and_cursor_columns = Keyword.get(opts, :select_only_pk_and_cursor_columns, false)

    order_by = KeysetCursor.order_by_sql(table)
    min_where_clause = KeysetCursor.where_sql(table, if(include_min, do: ">=", else: ">"))
    cursor_values = KeysetCursor.casted_cursor_values(table, min_cursor)

    # Get the select columns based on the option
    select_columns =
      if select_only_pk_and_cursor_columns do
        # Get cursor columns which include the primary keys and sort columns
        cursor_columns = KeysetCursor.cursor_columns(table)

        # Create a select statement with only the needed columns
        Enum.map_join(cursor_columns, ", ", &Postgres.quote_name(&1.name))
      else
        Postgres.safe_select_columns(table)
      end

    sql = """
    select #{select_columns}
    from #{Postgres.quote_name(table.schema, table.name)}
    where #{min_where_clause}
    order by #{order_by}
    limit ?
    """

    sql = Postgres.parameterize_sql(sql)
    params = cursor_values ++ [limit]

    {sql, params}
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

  defp env do
    Application.get_env(:sequin, :env)
  end

  # Add this new function
  def fast_count_estimate(%PostgresDatabase{} = db, %PostgresDatabaseTable{} = table, min_cursor, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))
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

    with {:ok, %Postgrex.Result{rows: [[count]]}} <- Postgres.query(db, sql, cursor_values, timeout: timeout) do
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

  defp message_from_row(
         %SinkConsumer{message_kind: :record} = consumer,
         %Backfill{},
         %PostgresDatabaseTable{} = table,
         record
       ) do
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

  defp message_from_row(
         %SinkConsumer{message_kind: :event} = consumer,
         %Backfill{} = backfill,
         %PostgresDatabaseTable{} = table,
         record
       ) do
    record_pks = record_pks(table, record)
    data = build_event_data(table, consumer, backfill, record, record_pks)

    %ConsumerEvent{
      consumer_id: consumer.id,
      record_pks: record_pks,
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
        consumer: %ConsumerRecordData.Metadata.Sink{
          id: consumer.id,
          name: consumer.name
        },
        commit_timestamp: DateTime.utc_now()
      }
    }
  end

  defp build_event_data(table, consumer, backfill, record_attnums_to_values, record_pks) do
    %ConsumerEventData{
      action: :read,
      record: build_record_payload(table, record_attnums_to_values),
      metadata: %ConsumerEventData.Metadata{
        database_name: consumer.replication_slot.postgres_database.name,
        table_name: table.name,
        table_schema: table.schema,
        consumer: %ConsumerEventData.Metadata.Sink{
          id: consumer.id,
          name: consumer.name
        },
        commit_timestamp: DateTime.utc_now(),
        idempotency_key: Base.encode64("#{backfill.id}:#{record_pks}")
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
    case Enum.find(consumer.source_tables, &(&1.table_oid == table.oid)) do
      nil ->
        table |> record_pks(record_attnums_to_values) |> Enum.join(",")

      source_table ->
        Enum.map_join(source_table.group_column_attnums, ",", fn attnum ->
          to_string(Map.get(record_attnums_to_values, attnum))
        end)
    end
  end
end
