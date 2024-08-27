defmodule Sequin.Consumers do
  @moduledoc false
  import Ecto.Query

  alias Ecto.Type
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.Query
  alias Sequin.Consumers.SourceTable.DateTimeValue
  alias Sequin.Consumers.SourceTable.NullValue
  alias Sequin.ConsumersRuntime.Supervisor, as: ConsumersSupervisor
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Postgres
  alias Sequin.ReplicationRuntime.Supervisor, as: ReplicationSupervisor
  alias Sequin.Repo

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])
  @consumer_record_state_enum Postgres.quote_name(@stream_schema, "consumer_record_state")

  @type consumer :: HttpPullConsumer.t() | HttpPushConsumer.t()

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # Consumers

  def get_consumer(consumer_id) do
    with {:error, _} <- get_http_pull_consumer(consumer_id),
         {:error, _} <- get_http_push_consumer(consumer_id) do
      {:error, Error.not_found(entity: :consumer)}
    end
  end

  def get_consumer!(consumer_id) do
    case get_consumer(consumer_id) do
      {:ok, consumer} -> consumer
      {:error, _} -> raise Error.not_found(entity: :consumer)
    end
  end

  def get_consumer_for_account(account_id, consumer_id) do
    pull_consumer =
      account_id
      |> HttpPullConsumer.where_account_id()
      |> HttpPullConsumer.where_id_or_name(consumer_id)
      |> Repo.one()

    if pull_consumer do
      pull_consumer
    else
      account_id
      |> HttpPushConsumer.where_account_id()
      |> HttpPushConsumer.where_id_or_name(consumer_id)
      |> Repo.one()
    end
  end

  def reload(%ConsumerEvent{} = ce) do
    ce.consumer_id
    |> ConsumerEvent.where_consumer_id()
    |> ConsumerEvent.where_commit_lsn(ce.commit_lsn)
    |> Repo.one()
  end

  def reload(%ConsumerRecord{} = cr) do
    cr.consumer_id
    |> ConsumerRecord.where_consumer_id()
    |> ConsumerRecord.where_id(cr.id)
    |> Repo.one()
  end

  def all_consumers do
    Repo.all(HttpPushConsumer) ++ Repo.all(HttpPullConsumer)
  end

  def list_consumers_for_account(account_id, preload \\ []) do
    pull_consumers =
      account_id
      |> HttpPullConsumer.where_account_id()
      |> preload(^preload)
      |> Repo.all()

    push_consumers =
      account_id
      |> HttpPushConsumer.where_account_id()
      |> preload(^preload)
      |> Repo.all()

    Enum.sort_by(pull_consumers ++ push_consumers, & &1.inserted_at, {:desc, DateTime})
  end

  def list_consumers_for_replication_slot(replication_slot_id) do
    pull = HttpPullConsumer.where_replication_slot_id(replication_slot_id)
    push = HttpPushConsumer.where_replication_slot_id(replication_slot_id)

    Repo.all(pull) ++ Repo.all(push)
  end

  def list_consumers_for_http_endpoint(http_endpoint_id) do
    http_endpoint_id
    |> HttpPushConsumer.where_http_endpoint_id()
    |> Repo.all()
  end

  def update_consumer(%HttpPullConsumer{} = consumer, attrs) do
    consumer
    |> HttpPullConsumer.update_changeset(attrs)
    |> Repo.update()
  end

  def update_consumer(%HttpPushConsumer{} = consumer, attrs) do
    consumer
    |> HttpPushConsumer.update_changeset(attrs)
    |> Repo.update()
  end

  def update_consumer_with_lifecycle(consumer, attrs) do
    with {:ok, updated_consumer} <- update_consumer(consumer, attrs) do
      :ok = notify_consumer_update(updated_consumer)

      {:ok, updated_consumer}
    end
  end

  def delete_consumer_with_lifecycle(consumer) do
    Repo.transact(fn ->
      case delete_consumer(consumer) do
        {:ok, _} ->
          :ok = delete_consumer_partition(consumer)
          notify_consumer_delete(consumer)
          {:ok, consumer}

        {:error, error} ->
          {:error, error}
      end
    end)
  end

  def delete_consumer(consumer) do
    Repo.delete(consumer)
  end

  def partition_name(%{message_kind: :event} = consumer) do
    "consumer_events_#{consumer.seq}"
  end

  def partition_name(%{message_kind: :record} = consumer) do
    "consumer_records_#{consumer.seq}"
  end

  # HttpPullConsumer

  def get_http_pull_consumer(consumer_id) do
    case Repo.get(HttpPullConsumer, consumer_id) do
      nil -> {:error, Error.not_found(entity: :http_pull_consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_http_pull_consumer_for_account(account_id, id_or_name) do
    res =
      account_id
      |> HttpPullConsumer.where_account_id()
      |> HttpPullConsumer.where_id_or_name(id_or_name)
      |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs, opts \\ []) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_http_pull_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        unless opts[:no_backfill] do
          backfill_consumer!(consumer)
        end

        :ok = notify_consumer_update(consumer)

        consumer = Repo.reload!(consumer)

        {:ok, consumer}
      end
    end)
  end

  def create_http_pull_consumer_with_lifecycle(attrs, opts \\ []) do
    account_id = Map.fetch!(attrs, :account_id)
    create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs, opts)
  end

  def create_http_pull_consumer(account_id, attrs) do
    %HttpPullConsumer{account_id: account_id}
    |> HttpPullConsumer.create_changeset(attrs)
    |> Repo.insert()
  end

  # HttpPushConsumer

  def get_http_push_consumer(consumer_id) do
    case Repo.get(HttpPushConsumer, consumer_id) do
      nil -> {:error, Error.not_found(entity: :http_push_consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def list_active_push_consumers do
    :active
    |> HttpPushConsumer.where_status()
    |> Repo.all()
  end

  def create_http_push_consumer_for_account_with_lifecycle(account_id, attrs, opts \\ []) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_http_push_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        unless opts[:no_backfill] do
          backfill_consumer!(consumer)
        end

        :ok = notify_consumer_update(consumer)

        consumer = Repo.reload!(consumer)

        {:ok, consumer}
      end
    end)
  end

  def create_http_push_consumer_with_lifecycle(attrs, opts \\ []) do
    account_id = Map.fetch!(attrs, :account_id)
    create_http_push_consumer_for_account_with_lifecycle(account_id, attrs, opts)
  end

  def create_http_push_consumer(account_id, attrs) do
    %HttpPushConsumer{account_id: account_id}
    |> HttpPushConsumer.create_changeset(attrs)
    |> Repo.insert()
  end

  # ConsumerEvent

  def get_consumer_event(consumer_id, commit_lsn) do
    consumer_event =
      consumer_id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_commit_lsn(commit_lsn)
      |> Repo.one()

    case consumer_event do
      nil -> {:error, Error.not_found(entity: :consumer_event)}
      consumer_event -> {:ok, consumer_event}
    end
  end

  def get_consumer_event!(consumer_id, commit_lsn) do
    case get_consumer_event(consumer_id, commit_lsn) do
      {:ok, consumer_event} -> consumer_event
      {:error, _} -> raise Error.not_found(entity: :consumer_event)
    end
  end

  def list_consumer_events_for_consumer(consumer_id, params \\ []) do
    base_query = ConsumerEvent.where_consumer_id(consumer_id)

    query =
      Enum.reduce(params, base_query, fn
        {:is_deliverable, false}, query ->
          ConsumerEvent.where_not_visible(query)

        {:is_deliverable, true}, query ->
          ConsumerEvent.where_deliverable(query)

        {:limit, limit}, query ->
          limit(query, ^limit)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)
      end)

    Repo.all(query)
  end

  def insert_consumer_events([]), do: {:ok, 0}

  def insert_consumer_events(consumer_events) do
    now = DateTime.utc_now()

    entries =
      Enum.map(consumer_events, fn event ->
        event
        |> Map.merge(%{
          updated_at: now,
          inserted_at: now
        })
        |> ConsumerEvent.from_map()
        # insert_all expects a plain outer-map, but struct embeds
        |> Sequin.Map.from_ecto()
      end)

    {count, _} = Repo.insert_all(ConsumerEvent, entries)
    {:ok, count}
  end

  # ConsumerRecord

  def get_consumer_record(consumer_id, id) do
    consumer_record =
      consumer_id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_id(id)
      |> Repo.one()

    case consumer_record do
      nil -> {:error, Error.not_found(entity: :consumer_record)}
      consumer_record -> {:ok, consumer_record}
    end
  end

  def get_consumer_record!(consumer_id, id) do
    case get_consumer_record(consumer_id, id) do
      {:ok, consumer_record} -> consumer_record
      {:error, _} -> raise Error.not_found(entity: :consumer_record)
    end
  end

  def list_consumer_records_for_consumer(consumer_id, params \\ []) do
    base_query = ConsumerRecord.where_consumer_id(consumer_id)

    query =
      Enum.reduce(params, base_query, fn
        {:is_deliverable, false}, query ->
          ConsumerRecord.where_not_visible(query)

        {:is_deliverable, true}, query ->
          ConsumerRecord.where_deliverable(query)

        {:limit, limit}, query ->
          limit(query, ^limit)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)
      end)

    Repo.all(query)
  end

  # Only way to get this fragment to compile with the dynamic enum name.
  # Part of Ecto's SQL injection protection.
  @case_frag """
    CASE
      WHEN ? IN ('delivered', 'pending_redelivery') THEN 'pending_redelivery'
      ELSE 'available'
    END::#{@consumer_record_state_enum}
  """
  def insert_consumer_records([]), do: {:ok, 0}

  def insert_consumer_records(consumer_records) do
    now = DateTime.utc_now()

    records =
      Enum.map(consumer_records, fn record ->
        record
        |> Map.put(:inserted_at, now)
        |> Map.put(:updated_at, now)
        |> ConsumerRecord.from_map()
        # insert_all expects a plain outer-map, but struct embeds
        |> Sequin.Map.from_ecto()
      end)

    conflict_target = [:consumer_id, :record_pks, :table_oid]

    on_conflict =
      from(cr in ConsumerRecord,
        update: [
          set: [
            commit_lsn: fragment("EXCLUDED.commit_lsn"),
            state:
              fragment(
                @case_frag,
                cr.state
              ),
            updated_at: fragment("EXCLUDED.updated_at")
          ]
        ]
      )

    {count, _records} =
      Repo.insert_all(
        ConsumerRecord,
        records,
        on_conflict: on_conflict,
        conflict_target: conflict_target
      )

    {:ok, count}
  end

  def delete_consumer_records([]), do: {:ok, 0}

  def delete_consumer_records(consumer_records) do
    delete_query =
      Enum.reduce(consumer_records, ConsumerRecord, fn record, query ->
        or_where(
          query,
          [cr],
          cr.consumer_id == ^record.consumer_id and
            cr.table_oid == ^record.table_oid and
            cr.record_pks == ^record.record_pks
        )
      end)

    {count, _} = Repo.delete_all(delete_query)
    {:ok, count}
  end

  # Consumer Lifecycle

  def create_consumer_partition(%{message_kind: :event} = consumer) do
    """
    CREATE TABLE #{stream_schema()}.#{partition_name(consumer)} PARTITION OF #{stream_schema()}.consumer_events FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def create_consumer_partition(%{message_kind: :record} = consumer) do
    """
    CREATE TABLE #{stream_schema()}.#{partition_name(consumer)} PARTITION OF #{stream_schema()}.consumer_records FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%{message_kind: :event} = consumer) do
    """
    DROP TABLE IF EXISTS #{stream_schema()}.#{partition_name(consumer)};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%{message_kind: :record} = consumer) do
    """
    DROP TABLE IF EXISTS #{stream_schema()}.#{partition_name(consumer)};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  # Consuming / Acking Messages
  @spec receive_for_consumer(consumer(), keyword()) ::
          {:ok, [ConsumerEvent.t()]} | {:ok, [ConsumerRecord.t()]}
  def receive_for_consumer(consumer, opts \\ [])

  def receive_for_consumer(%{message_kind: :event} = consumer, opts) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    outstanding_count =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_not_visible()
      |> ConsumerEvent.count()
      |> Repo.one()

    case min(batch_size, max_ack_pending - outstanding_count) do
      0 ->
        {:ok, []}

      batch_size ->
        {:ok, events} =
          Query.receive_consumer_events(
            batch_size: batch_size,
            consumer_id: UUID.string_to_binary!(consumer.id),
            not_visible_until: not_visible_until,
            now: now
          )

        events =
          Enum.map(events, fn event ->
            event
            |> Map.update!(:consumer_id, &UUID.binary_to_string!/1)
            |> Map.update!(:ack_id, &UUID.binary_to_string!/1)
            |> Map.update!(:inserted_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> Map.update!(:updated_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> Map.update!(:last_delivered_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> ConsumerEvent.from_map()
          end)

        if length(events) > 0 do
          Health.update(consumer, :receive, :healthy)
        end

        {:ok, events}
    end
  end

  def receive_for_consumer(%{message_kind: :record} = consumer, opts) do
    consumer = Repo.preload(consumer, :postgres_database)
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    outstanding_count =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_not_visible()
      |> ConsumerRecord.count()
      |> Repo.one()

    case min(batch_size, max_ack_pending - outstanding_count) do
      0 ->
        {:ok, []}

      batch_size ->
        {:ok, records} =
          Query.receive_consumer_records(
            batch_size: batch_size,
            consumer_id: UUID.string_to_binary!(consumer.id),
            not_visible_until: not_visible_until,
            now: now
          )

        records =
          Enum.map(records, fn record ->
            record
            |> Map.update!(:consumer_id, &UUID.binary_to_string!/1)
            |> Map.update!(:ack_id, &UUID.binary_to_string!/1)
            |> Map.update!(:inserted_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> Map.update!(:updated_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> Map.update!(:last_delivered_at, &DateTime.from_naive!(&1, "Etc/UTC"))
            |> ConsumerRecord.from_map()
          end)

        # Fetch source data for the records
        with {:ok, conn} <- ConnectionCache.connection(consumer.postgres_database),
             {:ok, fetched_records} <- put_source_data(conn, consumer, records),
             {:ok, fetched_records} <- filter_and_delete_records(consumer.id, fetched_records) do
          if length(fetched_records) > 0 do
            Health.update(consumer, :receive, :healthy)
          end

          {:ok, fetched_records}
        end
    end
  end

  def put_source_data(conn, consumer, records) do
    # I can't reproduce this behaviour outside of the test suite. But it appears that without assoc_loaded?,
    # Ecto preloads the association regardless of whether it's loaded or not.
    # This messes up tests, which modify the postgres_database directly before passing in.
    consumer =
      if Ecto.assoc_loaded?(consumer.postgres_database),
        do: consumer,
        else: Repo.preload(consumer, :postgres_database)

    postgres_db = consumer.postgres_database

    # Fetch the tables for the database
    {:ok, tables} = Databases.tables(postgres_db)

    # Group records by table_oid
    records_by_oid = Enum.group_by(records, & &1.table_oid)

    # Fetch data for each group of records
    Enum.reduce_while(records_by_oid, {:ok, []}, fn {table_oid, oid_records}, {:ok, acc} ->
      table = Enum.find(tables, &(&1.oid == table_oid))

      if table do
        case fetch_records_data(conn, table, oid_records) do
          {:ok, fetched_records} -> {:cont, {:ok, acc ++ fetched_records}}
          {:error, _} = error -> {:halt, error}
        end
      else
        Logger.error("Table not found for table_oid: #{table_oid}")

        error =
          Error.not_found(
            entity: :table,
            params: %{table_oid: table_oid, consumer_id: consumer.id}
          )

        {:halt, {:error, error}}
      end
    end)
  end

  defp fetch_records_data(conn, %PostgresDatabase.Table{} = table, records) do
    record_count = length(records)
    # Get the primary key columns and their types
    pk_columns = Enum.filter(table.columns, & &1.is_pk?)
    ordered_pk_columns = Enum.sort_by(pk_columns, & &1.attnum)
    pk_column_count = length(pk_columns)
    pk_types = Enum.map(pk_columns, & &1.type)

    # Cast record_pks to the correct types
    casted_pks =
      records
      |> Enum.map(fn record ->
        record.record_pks
        |> Enum.zip(pk_types)
        |> Enum.map(fn {value, type} -> cast_value(value, type) end)
      end)
      |> List.flatten()

    where_clause =
      if pk_column_count == 1 do
        # This one needs to not use a row tuple on the left- or right-hand sides
        # pk_column_name = Postgres.quote_name(List.first(pk_columns).name)
        pk_column_name = "id"
        "where #{pk_column_name} in #{Postgres.parameterized_tuple(record_count)}"
      else
        # the where clause is (col1, col2) IN ((val1, val2), (val3, val4))
        # which is too challenging to pull off with Ecto fragments
        pk_column_names =
          pk_columns |> Enum.map(& &1.name) |> Enum.map_join(", ", &Postgres.quote_name/1)

        value_params =
          Enum.map_join(1..record_count, ", ", fn n ->
            Postgres.parameterized_tuple(pk_column_count, (n - 1) * pk_column_count)
          end)

        "where (#{pk_column_names}) in (#{value_params})"
      end

    query = "select * from #{Postgres.quote_name(table.schema, table.name)} #{where_clause}"

    # Execute the query
    with {:ok, result} <- Postgrex.query(conn, query, casted_pks) do
      # Convert result to map
      rows = Postgres.result_to_map(result)

      rows = PostgresDatabase.cast_rows(table, rows)

      # Match the results with the original records
      updated_records =
        Enum.map(records, fn record ->
          metadata = %ConsumerRecordData.Metadata{
            table_name: table.name,
            table_schema: table.schema
          }

          source_row =
            Enum.find(rows, fn row ->
              # Using ordered pk_columns is important to ensure it lines up with `record_pks`
              pk_values =
                ordered_pk_columns
                |> Enum.map(fn column -> Map.fetch!(row, column.name) end)
                |> Enum.map(&to_string/1)

              pk_values == record.record_pks
            end)

          if source_row do
            %{record | data: %ConsumerRecordData{record: source_row, metadata: metadata}}
          else
            %{record | data: %ConsumerRecordData{record: nil, metadata: metadata}}
          end
        end)

      {:ok, updated_records}
    end
  end

  # If they deleted a record from the source table, but that hasn't propagated to ConsumerRecord yet,
  # this function will clean those records out.
  def filter_and_delete_records(consumer_id, records) do
    {valid_records, nil_records} = Enum.split_with(records, &(&1.data.record != nil))

    if Enum.any?(nil_records) do
      nil_record_ids = Enum.map(nil_records, & &1.id)

      {deleted_count, _} =
        ConsumerRecord
        |> where([cr], cr.id in ^nil_record_ids)
        |> where([cr], cr.consumer_id == ^consumer_id)
        |> Repo.delete_all()

      Logger.info("Deleted #{deleted_count} ConsumerRecords with nil data.record")
    end

    {:ok, valid_records}
  end

  # Helper function to cast values using Ecto's type system
  defp cast_value(value, "uuid"), do: UUID.string_to_binary!(value)

  defp cast_value(value, pg_type) do
    ecto_type = Postgres.pg_type_to_ecto_type(pg_type)

    case Type.cast(ecto_type, value) do
      {:ok, casted_value} ->
        casted_value

      :error ->
        Logger.warning("Failed to cast value #{inspect(value)} (pg_type: #{pg_type}) to ecto_type: #{ecto_type}")

        # Return original value if casting fails
        value
    end
  end

  @spec ack_messages(consumer(), [integer()]) :: :ok
  def ack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> Repo.delete_all()

    Health.update(consumer, :acknowledgement, :healthy)

    :ok
  end

  @spec ack_messages(consumer(), [String.t()]) :: :ok
  def ack_messages(%{message_kind: :record} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> ConsumerRecord.where_state_not(:pending_redelivery)
      |> Repo.delete_all()

    {_, _} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [state: :available, not_visible_until: nil])

    Health.update(consumer, :acknowledgement, :healthy)

    :ok
  end

  @spec nack_messages(consumer(), [integer()]) :: :ok
  def nack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [not_visible_until: nil])

    :ok
  end

  @spec nack_messages(consumer(), [String.t()]) :: :ok
  def nack_messages(%{message_kind: :record} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [not_visible_until: nil, state: :available])

    :ok
  end

  # HttpEndpoint

  def get_http_endpoint(id) do
    case Repo.get(HttpEndpoint, id) do
      nil -> {:error, Error.not_found(entity: :http_endpoint)}
      http_endpoint -> {:ok, http_endpoint}
    end
  end

  def get_http_endpoint!(id) do
    case get_http_endpoint(id) do
      {:ok, http_endpoint} -> http_endpoint
      {:error, _} -> raise Error.not_found(entity: :http_endpoint)
    end
  end

  def list_http_endpoints_for_account(account_id, preload \\ []) do
    account_id
    |> HttpEndpoint.where_account_id()
    |> preload(^preload)
    |> Repo.all()
  end

  def get_http_endpoint_for_account(account_id, id) do
    account_id
    |> HttpEndpoint.where_account_id()
    |> Repo.get(id)
    |> case do
      %HttpEndpoint{} = http_endpoint -> {:ok, http_endpoint}
      nil -> {:error, Error.not_found(entity: :http_endpoint)}
    end
  end

  def create_http_endpoint_for_account(account_id, attrs) do
    %HttpEndpoint{account_id: account_id}
    |> HttpEndpoint.create_changeset(attrs)
    |> Repo.insert()
  end

  def update_http_endpoint(%HttpEndpoint{} = http_endpoint, attrs) do
    http_endpoint
    |> HttpEndpoint.update_changeset(attrs)
    |> Repo.update()
  end

  def delete_http_endpoint(%HttpEndpoint{} = http_endpoint) do
    http_endpoint
    |> Ecto.Changeset.change()
    |> Ecto.Changeset.foreign_key_constraint(:http_push_consumers, name: "http_push_consumers_http_endpoint_id_fkey")
    |> Repo.delete()
  end

  # Backfills

  def backfill_limit, do: 10_000

  defp backfill_consumer!(_consumer) do
    # {:ok, messages} = backfill_messages_for_consumer(consumer)

    # if length(messages) < backfill_limit() do
    #   {:ok, _} = update_consumer_with_lifecycle(consumer, %{backfill_completed_at: DateTime.utc_now()})
    # end

    # next_seq = messages |> Enum.map(& &1.seq) |> Enum.max(fn -> 0 end)
    # {:ok, _} = ConsumerBackfillWorker.create(consumer.id, next_seq)

    :ok
  end

  # def backfill_messages_for_consumer(_consumer, seq \\ 0) do
  def backfill_messages_for_consumer(_consumer, _seq) do
    #   messages =
    #     Streams.list_messages_for_stream(consumer.stream_id,
    #       seq_gt: seq,
    #       limit: backfill_limit(),
    #       order_by: [asc: :seq],
    #       select: [:key, :seq]
    #     )

    #   {:ok, _} =
    #     messages
    #     |> Enum.filter(fn message ->
    #       Sequin.Key.matches?(consumer.filter_key_pattern, message.key)
    #     end)
    #     |> Enum.map(fn message ->
    #       %ConsumerMessage{
    #         consumer_id: consumer.id,
    #         message_key: message.key,
    #         message_seq: message.seq
    #       }
    #     end)

    #   # |> then(&upsert_consumer_messages(consumer, &1))

    #   {:ok, messages}
    :ok
  end

  # Source Table Matching

  def matches_message?(consumer, message) do
    Logger.info("[Consumers] Matching message to consumer #{consumer.id}")

    matches? =
      Enum.any?(consumer.source_tables, fn source_table ->
        table_matches = source_table.oid == message.table_oid
        action_matches = action_matches?(source_table.actions, message.action)
        column_filters_match = column_filters_match?(source_table.column_filters, message)

        Logger.debug("""
        [Consumers]
          matches?: #{table_matches && action_matches && column_filters_match}
            table_matches: #{table_matches}
            action_matches: #{action_matches}
            column_filters_match: #{column_filters_match}

          consumer:
            #{inspect(consumer, pretty: true)}

          message:
            #{inspect(message, pretty: true)}
        """)

        table_matches && action_matches && column_filters_match
      end)

    Health.update(consumer, :filters, :healthy)

    matches?
  rescue
    error in [ArgumentError] ->
      Health.update(
        consumer,
        :filters,
        :unhealthy,
        Error.service(
          code: :argument_error,
          message: Exception.message(error)
        )
      )

      reraise error, __STACKTRACE__
  end

  defp action_matches?(source_table_actions, message_action) do
    message_action in source_table_actions
  end

  defp column_filters_match?([], _message), do: true

  defp column_filters_match?(column_filters, message) do
    Enum.all?(column_filters, fn filter ->
      fields = if message.action == :delete, do: message.old_fields, else: message.fields
      field = Enum.find(fields, &(&1.column_attnum == filter.column_attnum))
      field && apply_filter(filter.operator, field.value, filter.value)
    end)
  end

  defp apply_filter(operator, %Date{} = field_value, %DateTimeValue{} = filter_value) do
    field_value_as_datetime = DateTime.new!(field_value, ~T[00:00:00])
    apply_filter(operator, field_value_as_datetime, filter_value)
  end

  defp apply_filter(operator, %NaiveDateTime{} = field_value, %DateTimeValue{} = filter_value) do
    field_value_as_datetime = DateTime.from_naive!(field_value, "Etc/UTC")
    apply_filter(operator, field_value_as_datetime, filter_value)
  end

  defp apply_filter(:==, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.compare(field_value, filter_value) == :eq
  end

  defp apply_filter(:!=, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.compare(field_value, filter_value) != :eq
  end

  defp apply_filter(:>, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.after?(field_value, filter_value)
  end

  defp apply_filter(:<, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.before?(field_value, filter_value)
  end

  defp apply_filter(:>=, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.compare(field_value, filter_value) in [:gt, :eq]
  end

  defp apply_filter(:<=, field_value, %DateTimeValue{value: filter_value}) do
    DateTime.compare(field_value, filter_value) in [:lt, :eq]
  end

  defp apply_filter(:is_null, field_value, %NullValue{}) do
    is_nil(field_value)
  end

  defp apply_filter(:not_null, field_value, %NullValue{}) do
    not is_nil(field_value)
  end

  defp apply_filter(op, field_value, %{value: filter_value}) when op in [:==, :!=, :>, :<, :>=, :<=],
    do: apply(Kernel, op, [field_value, filter_value])

  defp apply_filter(:is_null, field_value, _), do: is_nil(field_value)
  defp apply_filter(:not_null, field_value, _), do: not is_nil(field_value)

  defp apply_filter(:in, field_value, %{value: filter_value}) when is_list(filter_value) do
    field_value in filter_value or to_string(field_value) in Enum.map(filter_value, &to_string/1)
  end

  defp apply_filter(:not_in, field_value, %{value: filter_value}) when is_list(filter_value) do
    field_value not in filter_value and
      to_string(field_value) not in Enum.map(filter_value, &to_string/1)
  end

  defp notify_consumer_update(%HttpPullConsumer{} = consumer) do
    ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
  end

  defp notify_consumer_update(%HttpPushConsumer{} = consumer) do
    if env() == :test do
      ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
    else
      with :ok <- ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id),
           {:ok, _} <- ConsumersSupervisor.restart_for_push_consumer(consumer) do
        :ok
      end
    end
  end

  defp notify_consumer_delete(%HttpPullConsumer{} = consumer) do
    ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
  end

  defp notify_consumer_delete(%HttpPushConsumer{} = consumer) do
    if env() == :test do
      ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
    else
      with :ok <- ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id) do
        ConsumersSupervisor.stop_for_push_consumer(consumer)
      end
    end
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  def enrich_source_tables(source_tables, %PostgresDatabase{} = postgres_database) do
    table_oids = Enum.map(source_tables, & &1.oid)

    postgres_database.tables
    |> Enum.filter(&(&1.oid in table_oids))
    |> Enum.map(fn table ->
      %Sequin.Consumers.SourceTable{
        oid: table.oid,
        schema_name: table.schema,
        table_name: table.name,
        # Default empty list for actions
        actions: [],
        # Default empty list for column_filters
        column_filters: []
      }
    end)
  end
end
