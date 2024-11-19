defmodule Sequin.Consumers do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Accounts
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.Query
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.CiStringValue
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequenceFilter.DateTimeValue
  alias Sequin.Consumers.SequenceFilter.NullValue
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SourceTable
  alias Sequin.ConsumersRuntime.Supervisor, as: ConsumersSupervisor
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.Supervisor, as: DatabasesRuntimeSupervisor
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Postgres
  alias Sequin.Posthog
  alias Sequin.ReplicationRuntime.Supervisor, as: ReplicationSupervisor
  alias Sequin.Repo
  alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])
  @consumer_record_state_enum Postgres.quote_name(@stream_schema, "consumer_record_state")

  @type consumer :: HttpPullConsumer.t() | SinkConsumer.t()

  def posthog_ets_table do
    :consumer_ack_events
  end

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # Consumers

  def kind(%HttpPullConsumer{}), do: :pull
  def kind(%SinkConsumer{type: type}), do: type

  def source_table(%{source_tables: [], sequence: %Sequence{} = sequence} = consumer) do
    %PostgresDatabase{} = postgres_database = consumer.postgres_database
    %SequenceFilter{} = filter = consumer.sequence_filter
    table = Sequin.Enum.find!(postgres_database.tables, &(&1.oid == sequence.table_oid))
    primary_key_attnums = table.columns |> Enum.filter(& &1.is_pk?) |> Enum.map(& &1.attnum)

    %SourceTable{
      actions: filter.actions,
      group_column_attnums: filter.group_column_attnums || primary_key_attnums,
      sort_column_attnum: sequence.sort_column_attnum,
      oid: sequence.table_oid,
      schema_name: sequence.table_schema,
      table_name: sequence.table_name,
      column_filters:
        Enum.map(consumer.sequence_filter.column_filters, fn filter_column ->
          %SequenceFilter.ColumnFilter{
            column_attnum: filter_column.column_attnum,
            operator: filter_column.operator,
            value: filter_column.value,
            is_jsonb: filter_column.is_jsonb,
            jsonb_path: filter_column.jsonb_path
          }
        end)
    }
  end

  def source_table(%{source_tables: [source_table]}) do
    source_table
  end

  def source_table(_), do: nil

  def get_consumer(consumer_id) do
    with {:error, _} <- get_http_pull_consumer(consumer_id),
         {:error, _} <- get_sink_consumer(consumer_id) do
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
      |> preload(:sequence)
      |> Repo.one()

    if pull_consumer do
      pull_consumer
    else
      account_id
      |> SinkConsumer.where_account_id()
      |> SinkConsumer.where_id_or_name(consumer_id)
      |> preload(:sequence)
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
    Repo.all(SinkConsumer) ++ Repo.all(HttpPullConsumer)
  end

  def list_consumers_for_account(account_id, preload \\ []) do
    pull_consumers =
      account_id
      |> HttpPullConsumer.where_account_id()
      |> preload(^preload)
      |> Repo.all()

    sink_consumers =
      account_id
      |> SinkConsumer.where_account_id()
      |> preload(^preload)
      |> Repo.all()

    Enum.sort_by(pull_consumers ++ sink_consumers, & &1.inserted_at, {:desc, DateTime})
  end

  def list_sink_consumers_for_account(account_id, preload \\ []) do
    account_id
    |> SinkConsumer.where_account_id()
    |> preload(^preload)
    |> Repo.all()
  end

  def list_http_pull_consumers_for_account(account_id, preload \\ []) do
    account_id
    |> HttpPullConsumer.where_account_id()
    |> preload(^preload)
    |> Repo.all()
  end

  def list_consumers_for_replication_slot(replication_slot_id) do
    pull = HttpPullConsumer.where_replication_slot_id(replication_slot_id)
    push = SinkConsumer.where_replication_slot_id(replication_slot_id)

    Repo.all(pull) ++ Repo.all(push)
  end

  def list_consumers_for_sequence(sequence_id) do
    pull = HttpPullConsumer.where_sequence_id(sequence_id)
    push = SinkConsumer.where_sequence_id(sequence_id)

    Repo.all(pull) ++ Repo.all(push)
  end

  def list_sink_consumers_for_http_endpoint(http_endpoint_id) do
    http_endpoint_id
    |> SinkConsumer.where_http_endpoint_id()
    |> Repo.all()
  end

  def list_consumers_where_table_producer do
    pull = Repo.all(preload(HttpPullConsumer.where_table_producer(), replication_slot: :postgres_database))
    push = Repo.all(preload(SinkConsumer.where_table_producer(), replication_slot: :postgres_database))

    pull ++ push
  end

  def table_producer_finished(consumer_id) do
    consumer = get_consumer!(consumer_id)
    state = Map.from_struct(consumer.record_consumer_state)
    update_consumer(consumer, %{record_consumer_state: %{state | producer: :wal}})
  end

  def update_consumer(%HttpPullConsumer{} = consumer, attrs) do
    consumer
    |> HttpPullConsumer.update_changeset(attrs)
    |> Repo.update()
  end

  def update_consumer(%SinkConsumer{} = consumer, attrs) do
    consumer
    |> SinkConsumer.update_changeset(attrs)
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
          notify_consumer_delete(consumer)
          :ok = delete_consumer_partition(consumer)
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

  def find_http_pull_consumer(account_id, params \\ []) do
    params
    |> Enum.reduce(HttpPullConsumer.where_account_id(account_id), fn
      {:name, name}, query -> HttpPullConsumer.where_name(query, name)
      {:sequence_id, sequence_id}, query -> HttpPullConsumer.where_sequence_id(query, sequence_id)
    end)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :consumer)}
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

  def create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_http_pull_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        :ok = notify_consumer_update(consumer)
        notify_consumer_create(consumer)

        {:ok, consumer}
      end
    end)
  end

  def create_http_pull_consumer_with_lifecycle(attrs) do
    account_id = Map.fetch!(attrs, :account_id)
    create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs)
  end

  def create_http_pull_consumer(account_id, attrs) do
    %HttpPullConsumer{account_id: account_id}
    |> HttpPullConsumer.create_changeset(attrs)
    |> Repo.insert()
  end

  # SinkConsumer

  def get_sink_consumer(consumer_id) do
    case Repo.get(SinkConsumer, consumer_id) do
      nil -> {:error, Error.not_found(entity: :sink_consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_sink_consumer_for_account(account_id, consumer_id) do
    account_id
    |> SinkConsumer.where_account_id()
    |> SinkConsumer.where_id(consumer_id)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def find_sink_consumer(account_id, params \\ []) do
    params
    |> Enum.reduce(SinkConsumer.where_account_id(account_id), fn
      {:name, name}, query -> SinkConsumer.where_name(query, name)
      {:id_or_name, id_or_name}, query -> SinkConsumer.where_id_or_name(query, id_or_name)
      {:type, type}, query -> SinkConsumer.where_type(query, type)
      {:sequence_id, sequence_id}, query -> SinkConsumer.where_sequence_id(query, sequence_id)
    end)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def list_active_sink_consumers(preloads \\ []) do
    :active
    |> SinkConsumer.where_status()
    |> preload(^preloads)
    |> Repo.all()
  end

  def create_sink_consumer_for_account_with_lifecycle(account_id, attrs) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_sink_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        :ok = notify_consumer_update(consumer)
        notify_consumer_create(consumer)

        consumer = Repo.reload!(consumer)

        {:ok, consumer}
      end
    end)
  end

  def create_sink_consumer_with_lifecycle(attrs) do
    account_id = Map.fetch!(attrs, :account_id)
    create_sink_consumer_for_account_with_lifecycle(account_id, attrs)
  end

  def create_sink_consumer(account_id, attrs) do
    %SinkConsumer{account_id: account_id}
    |> SinkConsumer.create_changeset(attrs)
    |> Repo.insert()
  end

  @legacy_event_singleton_transform_cutoff_date ~D[2024-11-06]
  def consumer_features(%SinkConsumer{} = consumer) do
    consumer = Repo.lazy_preload(consumer, [:account])

    cond do
      Accounts.has_feature?(consumer.account, :legacy_event_transform) ->
        [legacy_event_transform: true]

      Date.before?(consumer.account.inserted_at, @legacy_event_singleton_transform_cutoff_date) ->
        [legacy_event_singleton_transform: true]

      true ->
        []
    end
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

        {:offset, offset}, query ->
          offset(query, ^offset)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)
      end)

    Repo.all(query)
  end

  def insert_consumer_events([]), do: {:ok, 0}

  def insert_consumer_events(consumer_events) do
    now = DateTime.utc_now()

    events =
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

    {count, _} = Repo.insert_all(ConsumerEvent, events)

    # Broadcast messages ingested to consumers for ie. push consumers
    consumer_events
    |> Stream.map(& &1.consumer_id)
    |> Enum.uniq()
    |> Enum.each(&Phoenix.PubSub.broadcast(Sequin.PubSub, "messages_ingested:#{&1}", :messages_ingested))

    {:ok, count}
  end

  # ConsumerRecord

  def get_consumer_record(consumer_id, id) when is_integer(id) do
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

  def get_consumer_record(consumer_id, params) when is_list(params) or is_map(params) do
    consumer_id
    |> consumer_record_query(params)
    |> Repo.one()
    |> case do
      %ConsumerRecord{} = consumer_record -> {:ok, consumer_record}
      nil -> {:error, Error.not_found(entity: :consumer_record)}
    end
  end

  def get_consumer_record!(consumer_id, id) when is_integer(id) do
    case get_consumer_record(consumer_id, id) do
      {:ok, consumer_record} -> consumer_record
      {:error, error} -> raise error
    end
  end

  def list_consumer_records_for_consumer(consumer_id, params \\ []) do
    consumer_id
    |> consumer_record_query(params)
    |> Repo.all()
  end

  defp consumer_record_query(consumer_id, params) do
    base_query = ConsumerRecord.where_consumer_id(consumer_id)

    Enum.reduce(params, base_query, fn
      {:is_deliverable, false}, query ->
        ConsumerRecord.where_not_visible(query)

      {:is_deliverable, true}, query ->
        ConsumerRecord.where_deliverable(query)

      {:is_delivered, true}, query ->
        ConsumerRecord.where_not_visible(query)

      {:limit, limit}, query ->
        limit(query, ^limit)

      {:offset, offset}, query ->
        offset(query, ^offset)

      {:order_by, order_by}, query ->
        order_by(query, ^order_by)
    end)
  end

  @fast_count_threshold 50_000
  def fast_count_threshold, do: @fast_count_threshold

  def fast_count_messages_for_consumer(consumer, params \\ []) do
    query = consumer_messages_query(consumer, params)

    # This number can be pretty inaccurate
    result = Ecto.Adapters.SQL.explain(Repo, :all, query)
    [_, rows] = Regex.run(~r/rows=(\d+)/, result)

    case String.to_integer(rows) do
      count when count > @fast_count_threshold ->
        count

      _ ->
        count_messages_for_consumer(consumer, params)
    end
  end

  defp consumer_messages_query(%{message_kind: :record} = consumer, params) do
    Enum.reduce(params, ConsumerRecord.where_consumer_id(consumer.id), fn
      {:delivery_count_gte, delivery_count}, query ->
        ConsumerRecord.where_delivery_count_gte(query, delivery_count)

      {:is_delivered, true}, query ->
        ConsumerRecord.where_not_visible(query)

      {:is_deliverable, true}, query ->
        ConsumerRecord.where_deliverable(query)

      {:limit, limit}, query ->
        limit(query, ^limit)
    end)
  end

  defp consumer_messages_query(%{message_kind: :event} = consumer, params) do
    Enum.reduce(params, ConsumerEvent.where_consumer_id(consumer.id), fn
      {:delivery_count_gte, delivery_count}, query ->
        ConsumerEvent.where_delivery_count_gte(query, delivery_count)
    end)
  end

  def count_messages_for_consumer(consumer, params \\ []) do
    consumer
    |> consumer_messages_query(params)
    |> Repo.aggregate(:count, :id)
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
      consumer_records
      |> Enum.map(fn record ->
        record
        |> Map.put(:inserted_at, now)
        |> Map.put(:updated_at, now)
        |> ConsumerRecord.from_map()
      end)
      |> Enum.sort_by(& &1.commit_lsn, :desc)
      |> Enum.uniq_by(&{&1.consumer_id, &1.record_pks, &1.table_oid})
      # insert_all expects a plain outer-map, but struct embeds
      |> Enum.map(&Sequin.Map.from_ecto/1)

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

    # Broadcast messages ingested to consumers for ie. push consumers
    consumer_records
    |> Stream.map(& &1.consumer_id)
    |> Enum.uniq()
    |> Enum.each(&Phoenix.PubSub.broadcast(Sequin.PubSub, "messages_ingested:#{&1}", :messages_ingested))

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

    case receive_batch_size(consumer, batch_size, max_ack_pending, outstanding_count) do
      0 ->
        {:ok, []}

      batch_size ->
        {:ok, events} =
          Query.receive_consumer_events(
            batch_size: batch_size,
            consumer_id: Sequin.String.string_to_binary!(consumer.id),
            not_visible_until: not_visible_until,
            now: now
          )

        events = Enum.map(events, fn event -> Ecto.embedded_load(ConsumerEvent, event, :json) end)

        if length(events) > 0 do
          Health.update(consumer, :receive, :healthy)
          TracerServer.messages_received(consumer, events)

          Enum.each(
            events,
            &Sequin.Logs.log_for_consumer_message(
              :info,
              consumer.account_id,
              &1.replication_message_trace_id,
              "Consumer produced event"
            )
          )
        end

        {:ok, events}
    end
  end

  def receive_for_consumer(%{message_kind: :record} = consumer, opts) do
    consumer = Repo.lazy_preload(consumer, [:postgres_database])
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

    case receive_batch_size(consumer, batch_size, max_ack_pending, outstanding_count) do
      0 ->
        {:ok, []}

      batch_size ->
        {:ok, records} =
          Query.receive_consumer_records(
            batch_size: batch_size,
            consumer_id: Sequin.String.string_to_binary!(consumer.id),
            not_visible_until: not_visible_until,
            now: now
          )

        records = Enum.map(records, fn record -> Ecto.embedded_load(ConsumerRecord, record, :json) end)

        # Fetch source data for the records
        with {:ok, fetched_records} <- put_source_data(consumer, records),
             {:ok, fetched_records} <- filter_and_delete_records(consumer.id, fetched_records) do
          if length(fetched_records) > 0 do
            Health.update(consumer, :receive, :healthy)
            TracerServer.messages_received(consumer, fetched_records)
          end

          Enum.each(
            fetched_records,
            &Sequin.Logs.log_for_consumer_message(
              :info,
              consumer.account_id,
              &1.replication_message_trace_id,
              "Consumer produced record"
            )
          )

          {:ok, fetched_records}
        end
    end
  end

  defp receive_batch_size(consumer, batch_size, max_ack_pending, outstanding_count) do
    batch_size = min(batch_size, max_ack_pending - outstanding_count)

    if batch_size < 0 do
      Logger.warning(
        "Consumer #{consumer.id} has negative batch size: #{batch_size}",
        consumer_id: consumer.id,
        batch_size: batch_size,
        max_ack_pending: max_ack_pending,
        outstanding_count: outstanding_count
      )

      0
    else
      batch_size
    end
  end

  def put_source_data(consumer, records) do
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
        case fetch_records_data(postgres_db, table, oid_records) do
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

  defp fetch_records_data(%PostgresDatabase{} = postgres_db, %PostgresDatabaseTable{} = table, records) do
    record_count = length(records)
    # Get the primary key columns and their types
    pk_columns =
      if Postgres.is_event_table?(table) do
        [Sequin.Enum.find!(table.columns, &(&1.name == "id"))]
      else
        Enum.filter(table.columns, & &1.is_pk?)
      end

    ordered_pk_columns = Enum.sort_by(pk_columns, & &1.attnum)
    pk_column_count = length(pk_columns)
    pk_types = Enum.map(pk_columns, & &1.type)

    # Cast record_pks to the correct types
    casted_pks =
      records
      |> Enum.map(fn record ->
        record.record_pks
        |> Enum.zip(pk_types)
        |> Enum.map(fn {value, type} -> Postgres.cast_value(value, type) end)
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

    select_columns = Postgres.safe_select_columns(table)

    query =
      "select #{select_columns} from #{Postgres.quote_name(table.schema, table.name)} #{where_clause}"

    # Execute the query
    with {:ok, result} <- Postgres.query(postgres_db, query, casted_pks) do
      # Convert result to map
      rows = Postgres.result_to_maps(result)

      rows = Postgres.load_rows(table, rows)

      # Match the results with the original records
      updated_records =
        Enum.map(records, fn record ->
          metadata = %ConsumerRecordData.Metadata{
            consumer: %{id: record.consumer_id},
            table_name: table.name,
            table_schema: table.schema
          }

          source_row =
            if Postgres.is_event_table?(table) do
              # Don't use `committed_at`/timestamp column to match records in the event table,
              # as timestamp comparisons are fraught.
              Enum.find(rows, fn row ->
                to_string(Map.fetch!(row, "id")) == to_string(List.first(record.record_pks))
              end)
            else
              Enum.find(rows, fn row ->
                # Using ordered pk_columns is important to ensure it lines up with `record_pks`
                pk_values =
                  ordered_pk_columns
                  |> Enum.map(fn column -> Map.fetch!(row, column.name) end)
                  |> Enum.map(&to_string/1)

                pk_values == record.record_pks
              end)
            end

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

  @spec ack_messages(consumer(), [integer()]) :: :ok
  def ack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {count, events} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> select([ce], ce)
      |> Repo.delete_all()

    send_posthog_ack_event(consumer)

    Health.update(consumer, :acknowledge, :healthy)
    Metrics.incr_consumer_messages_processed_count(consumer, count)

    Metrics.incr_consumer_messages_processed_throughput(consumer, count)

    TracerServer.messages_acked(consumer, ack_ids)

    Enum.each(
      events,
      &Sequin.Logs.log_for_consumer_message(
        :info,
        consumer.account_id,
        &1.replication_message_trace_id,
        "Event acknowledged"
      )
    )

    AcknowledgedMessages.store_messages(consumer.id, events)

    {:ok, count}
  end

  @spec ack_messages(consumer(), [String.t()]) :: :ok
  def ack_messages(%{message_kind: :record} = consumer, ack_ids) do
    {count_deleted, deleted_records} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> ConsumerRecord.where_state_not(:pending_redelivery)
      |> select([cr], cr)
      |> Repo.delete_all()

    {count_updated, updated_records} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> select([cr], cr)
      |> Repo.update_all(set: [state: :available, not_visible_until: nil])

    send_posthog_ack_event(consumer)

    Health.update(consumer, :acknowledge, :healthy)

    Metrics.incr_consumer_messages_processed_count(consumer, count_deleted + count_updated)
    Metrics.incr_consumer_messages_processed_throughput(consumer, count_deleted + count_updated)

    TracerServer.messages_acked(consumer, ack_ids)

    Enum.each(
      deleted_records,
      &Sequin.Logs.log_for_consumer_message(
        :info,
        consumer.account_id,
        &1.replication_message_trace_id,
        "Record acknowledged"
      )
    )

    Enum.each(
      updated_records,
      &Sequin.Logs.log_for_consumer_message(
        :info,
        consumer.account_id,
        &1.replication_message_trace_id,
        "Record acknowledged"
      )
    )

    AcknowledgedMessages.store_messages(consumer.id, deleted_records ++ updated_records)

    {:ok, count_deleted + count_updated}
  end

  defp send_posthog_ack_event(consumer) do
    now = :os.system_time(:second)
    key = consumer.id

    case :ets.lookup(posthog_ets_table(), key) do
      [] ->
        # No previous event, send it and store the timestamp
        do_send_posthog_event(consumer)
        :ets.insert(posthog_ets_table(), {key, now})

      [{^key, last_sent}] ->
        # Check if an hour has passed since the last event
        if now - last_sent >= 3600 do
          do_send_posthog_event(consumer)
          :ets.insert(posthog_ets_table(), {key, now})
        end
    end
  end

  defp do_send_posthog_event(consumer) do
    Posthog.capture("Consumer Messages Acked", %{
      distinct_id: "00000000-0000-0000-0000-000000000000",
      properties: %{
        consumer_id: consumer.id,
        "$groups": %{account: consumer.account_id}
      }
    })
  end

  @spec nack_messages(consumer(), [String.t()]) :: {:ok, integer()}
  def nack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {count, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [not_visible_until: nil])

    {:ok, count}
  end

  @spec nack_messages(consumer(), [String.t()]) :: {:ok, integer()}
  def nack_messages(%{message_kind: :record} = consumer, ack_ids) do
    {count, _} =
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> ConsumerRecord.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [not_visible_until: nil, state: :available])

    {:ok, count}
  end

  @doc """
  Nack messages with backoff allows us to both nack a message and set its not_visible_until
  to some time in the future. This is used to start for backing off in the HttpPushPipeline
  This is easy to do in Postgres with a single entry. When we want to perform an update
  for multiple messages, cleanest thing to do is to craft an upsert query.
  """
  def nack_messages_with_backoff(%{message_kind: :event} = consumer, ack_ids_with_not_visible_until) do
    nack_messages_with_backoff(ConsumerEvent, consumer, ack_ids_with_not_visible_until)
  end

  def nack_messages_with_backoff(%{message_kind: :record} = consumer, ack_ids_with_not_visible_until) do
    nack_messages_with_backoff(ConsumerRecord, consumer, ack_ids_with_not_visible_until)
  end

  def nack_messages_with_backoff(model, consumer, ack_ids_with_not_visible_until) do
    Repo.transaction(fn ->
      # Get the list of ack_ids
      ack_ids = Map.keys(ack_ids_with_not_visible_until)

      # Select existing records and lock them
      # This will let us do an upsert on conflict to update each row individually
      # We don't want to insert a message that was already acked, hence the select
      # before the upsert
      existing_records =
        consumer.id
        |> model.where_consumer_id()
        |> model.where_ack_ids(ack_ids)
        |> lock("FOR UPDATE")
        |> Repo.all()

      # Prepare updates only for existing records
      updates =
        Enum.map(existing_records, fn existing_record ->
          not_visible_until = Map.fetch!(ack_ids_with_not_visible_until, existing_record.ack_id)

          existing_record
          |> Sequin.Map.from_ecto()
          |> Map.put(:not_visible_until, not_visible_until)
        end)

      # Perform the upsert
      Repo.insert_all(model, updates,
        on_conflict: [set: [not_visible_until: dynamic([cr], fragment("EXCLUDED.not_visible_until"))]],
        conflict_target: [:consumer_id, :ack_id]
      )
    end)
  end

  @doc """
  Resets the not_visible_until field to the current time for a given consumer and record/event ID.
  """
  @spec reset_message_visibility(consumer(), String.t()) :: {:ok, integer()} | {:error, term()}
  def reset_message_visibility(consumer, id) do
    now = DateTime.utc_now()

    query =
      case consumer.message_kind do
        :event ->
          from(ce in ConsumerEvent,
            where: ce.consumer_id == ^consumer.id and ce.id == ^id,
            update: [set: [not_visible_until: ^now]],
            select: ce
          )

        :record ->
          from(cr in ConsumerRecord,
            where: cr.consumer_id == ^consumer.id and cr.id == ^id,
            update: [set: [not_visible_until: ^now, state: :available]],
            select: cr
          )
      end

    case Repo.update_all(query, []) do
      {1, [updated_message]} ->
        {:ok, updated_message}

      {0, _} ->
        {:error, Error.not_found(entity: consumer.message_kind)}
    end
  end

  @doc """
  Min active cursor: the value of the sort_key column for the sequence row that corresponds to the min value of id in the consumer records table
  Max active cursor: the value of the sort_key column for the sequence row that corresponds to the max value of id in the consumer records table for delivered records
  Min/max possible cursors: the min and max values of the sort_key column from the underlying sequences table
  """
  def cursors(consumer) do
    consumer = Repo.preload(consumer, [:postgres_database, :sequence])

    with {:ok, min_active_cursor} <- get_min_active_cursor(consumer),
         {:ok, max_active_cursor} <- get_max_active_cursor(consumer),
         {:ok, next_active_cursor} <- get_next_active_cursor(consumer),
         {:ok, min_possible_cursor} <- get_min_possible_cursor(consumer),
         {:ok, max_possible_cursor} <- get_max_possible_cursor(consumer),
         processing_count = fast_count_messages_for_consumer(consumer, is_delivered: true),
         {:ok, to_process_count} <- fast_count_to_process_count(consumer, max_active_cursor || next_active_cursor) do
      {:ok,
       %{
         min_active_cursor: min_active_cursor,
         max_active_cursor: max_active_cursor,
         next_active_cursor: next_active_cursor,
         min_possible_cursor: min_possible_cursor,
         max_possible_cursor: max_possible_cursor,
         processing_count: processing_count,
         to_process_count: to_process_count
       }}
    end
  end

  defp get_min_active_cursor(consumer) do
    get_active_cursor(consumer, [order_by: [asc: :id], is_delivered: true, limit: 1], :min_active_cursor)
  end

  defp get_max_active_cursor(consumer) do
    get_active_cursor(consumer, [order_by: [desc: :id], limit: 1, is_delivered: true], :max_active_cursor)
  end

  defp get_next_active_cursor(consumer) do
    get_active_cursor(consumer, [order_by: [asc: :id], is_deliverable: true, limit: 1], :next_active_cursor)
  end

  defp get_active_cursor(consumer, params, entity_name) do
    table = Sequin.Enum.find!(consumer.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))

    with {:ok, %ConsumerRecord{} = consumer_record} <- get_consumer_record(consumer.id, params),
         {:ok, [%ConsumerRecord{data: %ConsumerRecordData{record: record}}]} when not is_nil(record) <-
           fetch_records_data(consumer.postgres_database, table, [consumer_record]) do
      {:ok, record[consumer.sequence.sort_column_name]}
    else
      {:error, %NotFoundError{entity: :consumer_record}} ->
        {:ok, nil}

      {:ok, [%ConsumerRecord{data: %ConsumerRecordData{record: nil}}]} ->
        {:error, Error.not_found(entity: entity_name, params: params)}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp get_min_possible_cursor(consumer) do
    get_possible_cursor(consumer, "MIN", :min_possible_cursor)
  end

  defp get_max_possible_cursor(consumer) do
    get_possible_cursor(consumer, "MAX", :max_possible_cursor)
  end

  defp get_possible_cursor(consumer, aggregate_function, entity_name) do
    table_name = Postgres.quote_name(consumer.sequence.table_name)
    sort_column_name = Postgres.quote_name(consumer.sequence.sort_column_name)

    sql = "SELECT #{aggregate_function}(#{sort_column_name}) FROM #{table_name}"

    case Postgres.query(consumer.postgres_database, sql, []) do
      {:ok, %Postgrex.Result{rows: [[possible_cursor]]}} ->
        {:ok, possible_cursor}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:error, Error.not_found(entity: entity_name)}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp fast_count_to_process_count(consumer, max_active_cursor) do
    sql = """
    EXPLAIN SELECT COUNT(*)
    FROM #{consumer.sequence.table_name}
    WHERE #{consumer.sequence.sort_column_name} >= $1
    """

    case Postgres.query(consumer.postgres_database, sql, [max_active_cursor]) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        case extract_row_count(rows) do
          {:ok, count} when count > @fast_count_threshold ->
            {:ok, count}

          _ ->
            # If the count is small or couldn't be extracted, run the actual query
            sql = """
            SELECT COUNT(*)
            FROM #{consumer.sequence.table_name}
            WHERE #{consumer.sequence.sort_column_name} >= $1
            """

            case Postgres.query(consumer.postgres_database, sql, [max_active_cursor]) do
              {:ok, %Postgrex.Result{rows: [[count]]}} -> {:ok, count}
              {:error, error} -> {:error, error}
            end
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp extract_row_count(rows) do
    Enum.reduce_while(rows, :error, fn [row], acc ->
      case Regex.run(~r/rows=(\d+)/, row) do
        [_, count_str] -> {:halt, {:ok, String.to_integer(count_str)}}
        _ -> {:cont, acc}
      end
    end)
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

  def list_http_endpoints(preload \\ []) do
    HttpEndpoint
    |> preload(^preload)
    |> Repo.all()
  end

  def list_http_endpoints_for_account(account_id, preload \\ []) do
    account_id
    |> HttpEndpoint.where_account_id()
    |> preload(^preload)
    |> Repo.all()
  end

  def list_local_tunnel_http_endpoints_for_account(account_id, preload \\ []) do
    account_id
    |> HttpEndpoint.where_account_id()
    |> HttpEndpoint.where_use_local_tunnel()
    |> preload(^preload)
    |> Repo.all()
  end

  def find_http_endpoint_for_account(account_id, params \\ []) do
    params
    |> Enum.reduce(HttpEndpoint.where_account_id(account_id), fn
      {:id, id}, query -> HttpEndpoint.where_id(query, id)
      {:name, name}, query -> HttpEndpoint.where_name(query, name)
    end)
    |> Repo.one()
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

  def update_http_endpoint_with_lifecycle(%HttpEndpoint{} = http_endpoint, attrs) do
    with {:ok, http_endpoint} <- update_http_endpoint(http_endpoint, attrs),
         :ok <- notify_http_endpoint_update(http_endpoint) do
      {:ok, http_endpoint}
    end
  end

  def delete_http_endpoint(%HttpEndpoint{} = http_endpoint) do
    http_endpoint
    |> Ecto.Changeset.change()
    # |> Ecto.Changeset.foreign_key_constraint(:sink_consumers, name: "http_push_consumers_http_endpoint_id_fkey")
    |> Repo.delete()
  end

  def test_reachability(%HttpEndpoint{} = http_endpoint) do
    case HttpEndpoint.uri(http_endpoint) do
      %URI{host: host} when is_binary(host) ->
        case :inet.gethostbyname(String.to_charlist(host)) do
          {:ok, _} -> {:ok, :reachable}
          {:error, reason} -> {:error, reason}
        end

      _ ->
        {:error, :invalid_url}
    end
  end

  def test_connect(%HttpEndpoint{} = http_endpoint) do
    case HttpEndpoint.uri(http_endpoint) do
      %URI{host: host, port: port} when is_binary(host) ->
        # Convert host to charlist as required by :gen_tcp.connect
        host_charlist = String.to_charlist(host)

        # Attempt to establish a TCP connection
        case :gen_tcp.connect(host_charlist, port, [], 5000) do
          {:ok, socket} ->
            :gen_tcp.close(socket)
            {:ok, :connected}

          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error, :invalid_url}
    end
  end

  # Source Table Matching
  def matches_message?(
        %{sequence: %Sequence{} = sequence, sequence_filter: %SequenceFilter{} = sequence_filter} = consumer,
        message
      ) do
    matches? = matches_message?(sequence, sequence_filter, message)

    Health.update(consumer, :filters, :healthy)

    matches?
  rescue
    error in [ArgumentError] ->
      Health.update(
        consumer,
        :filters,
        :error,
        Error.service(
          code: :argument_error,
          message: Exception.message(error)
        )
      )

      reraise error, __STACKTRACE__
  end

  def matches_message?(consumer_or_wal_pipeline, message) do
    matches? =
      Enum.any?(consumer_or_wal_pipeline.source_tables, fn %SourceTable{} = source_table ->
        table_matches = source_table.oid == message.table_oid
        action_matches = action_matches?(source_table.actions, message.action)
        column_filters_match = column_filters_match_message?(source_table.column_filters, message)

        # Logger.debug("""
        # [Consumers]
        #   matches?: #{table_matches && action_matches && column_filters_match}
        #     table_matches: #{table_matches}
        #     action_matches: #{action_matches}
        #     column_filters_match: #{column_filters_match}

        #   consumer_or_wal_pipeline:
        #     #{inspect(consumer_or_wal_pipeline, pretty: true)}

        #   message:
        #     #{inspect(message, pretty: true)}
        # """)

        table_matches && action_matches && column_filters_match
      end)

    Health.update(consumer_or_wal_pipeline, :filters, :healthy)

    matches?
  rescue
    error in [ArgumentError] ->
      Health.update(
        consumer_or_wal_pipeline,
        :filters,
        :error,
        Error.service(
          code: :argument_error,
          message: Exception.message(error)
        )
      )

      reraise error, __STACKTRACE__
  end

  def matches_message?(%Sequence{} = sequence, %SequenceFilter{} = sequence_filter, message) do
    table_matches? = sequence.table_oid == message.table_oid
    actions_match? = action_matches?(sequence_filter.actions, message.action)
    column_filters_match? = column_filters_match_message?(sequence_filter.column_filters, message)

    table_matches? and actions_match? and column_filters_match?
  end

  def matches_record?(
        %{sequence: %Sequence{} = sequence, sequence_filter: %SequenceFilter{} = sequence_filter} = consumer,
        table_oid,
        record
      ) do
    table_matches? = sequence.table_oid == table_oid
    column_filters_match? = column_filters_match_record?(sequence_filter.column_filters, record)

    Health.update(consumer, :filters, :healthy)

    table_matches? and column_filters_match?
  end

  def matches_record?(consumer, table_oid, record_attnums_to_values) do
    source_table = Sequin.Enum.find!(consumer.source_tables, &(&1.oid == table_oid))
    matches? = column_filters_match_record?(source_table.column_filters, record_attnums_to_values)

    Health.update(consumer, :filters, :healthy)

    matches?
  end

  defp action_matches?(source_table_actions, message_action) do
    message_action in source_table_actions
  end

  defp column_filters_match_message?([], _message), do: true

  defp column_filters_match_message?(column_filters, message) do
    Enum.all?(column_filters, fn filter ->
      fields = if message.action == :delete, do: message.old_fields, else: message.fields
      field = Enum.find(fields, &(&1.column_attnum == filter.column_attnum))
      field && apply_filter(filter.operator, coerce_field_value(field.value, filter), filter.value)
    end)
  end

  defp column_filters_match_record?([], _message), do: true

  defp column_filters_match_record?(column_filters, record_attnums_to_values) do
    Enum.all?(column_filters, fn %ColumnFilter{} = filter ->
      field_value =
        record_attnums_to_values
        |> Map.get(filter.column_attnum)
        |> coerce_field_value(filter)

      apply_filter(filter.operator, field_value, filter.value)
    end)
  end

  defp coerce_field_value(value, %ColumnFilter{value: %CiStringValue{}}) when is_binary(value) do
    String.downcase(value)
  end

  defp coerce_field_value(value, %ColumnFilter{jsonb_path: jsonb_path}) when jsonb_path in [nil, ""], do: value

  defp coerce_field_value(value, %ColumnFilter{jsonb_path: jsonb_path}) when is_map(value) do
    path = String.split(jsonb_path, ".")
    get_in(value, path)
  rescue
    # Errors will happen when traversal hits an unsupported value type, like an array or a string.
    ArgumentError ->
      nil

    FunctionClauseError ->
      nil
  end

  defp coerce_field_value(value, _filter), do: value

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
    if consumer.status == :disabled, do: maybe_disable_table_producer(consumer)

    with :ok <- ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id) do
      restart_table_producer? =
        consumer.status == :active and consumer.message_kind == :record and
          consumer.record_consumer_state.producer == :table_and_wal and env() != :test

      if restart_table_producer? do
        DatabasesRuntimeSupervisor.restart_table_producer(consumer)
      end

      :ok
    end
  end

  defp notify_consumer_update(%SinkConsumer{} = consumer) do
    if consumer.status == :disabled, do: maybe_disable_table_producer(consumer)

    if env() == :test do
      ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
    else
      with :ok <- ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id) do
        case consumer.status do
          :active ->
            if consumer.message_kind == :record and consumer.record_consumer_state.producer == :table_and_wal do
              DatabasesRuntimeSupervisor.restart_table_producer(consumer)
            end

            ConsumersSupervisor.restart_for_sink_consumer(consumer)
            :ok

          :disabled ->
            ConsumersSupervisor.stop_for_sink_consumer(consumer)
            :ok
        end
      end
    end
  end

  defp notify_consumer_create(consumer) do
    if consumer.message_kind == :record and consumer.record_consumer_state.producer == :table_and_wal and env() != :test do
      DatabasesRuntimeSupervisor.start_table_producer(consumer)
    end
  end

  defp notify_consumer_delete(%HttpPullConsumer{} = consumer) do
    maybe_disable_table_producer(consumer)
    async_refresh_message_handler_ctx(consumer.replication_slot_id)
  end

  defp notify_consumer_delete(%SinkConsumer{} = consumer) do
    maybe_disable_table_producer(consumer)

    if env() == :test do
      ReplicationSupervisor.refresh_message_handler_ctx(consumer.replication_slot_id)
    else
      with %Task{} <- async_refresh_message_handler_ctx(consumer.replication_slot_id) do
        ConsumersSupervisor.stop_for_sink_consumer(consumer)
      end
    end
  end

  defp maybe_disable_table_producer(%{message_kind: :record} = consumer) do
    unless env() == :test do
      DatabasesRuntimeSupervisor.stop_table_producer(consumer)
    end
  end

  defp maybe_disable_table_producer(_consumer), do: :ok

  defp async_refresh_message_handler_ctx(replication_slot_id) do
    Task.Supervisor.async_nolink(
      Sequin.TaskSupervisor,
      fn ->
        ReplicationSupervisor.refresh_message_handler_ctx(replication_slot_id)
      end,
      # Until we make Replication more responsive, this can take a while
      timeout: :timer.minutes(2)
    )
  end

  defp notify_http_endpoint_update(%HttpEndpoint{} = http_endpoint) do
    sink_consumers = list_sink_consumers_for_http_endpoint(http_endpoint.id)
    Enum.each(sink_consumers, &ConsumersSupervisor.restart_for_sink_consumer(&1))
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  def enrich_source_tables(source_tables, %PostgresDatabase{} = postgres_database) do
    Enum.map(source_tables, fn source_table ->
      table = Sequin.Enum.find!(postgres_database.tables, &(&1.oid == source_table.oid))

      %Sequin.Consumers.SourceTable{
        source_table
        | schema_name: table.schema,
          table_name: table.name,
          column_filters: enrich_column_filters(source_table.column_filters, table.columns)
      }
    end)
  end

  defp enrich_column_filters(column_filters, columns) do
    Enum.map(column_filters, fn column_filter ->
      column = Sequin.Enum.find!(columns, &(&1.attnum == column_filter.column_attnum))
      %{column_filter | column_name: column.name}
    end)
  end

  @doc """
  Checks if there are any consumers that haven't been migrated to use Sequences.

  Returns `true` if there are any unmigrated consumers, `false` otherwise.
  """
  def any_unmigrated_consumers? do
    Enum.any?(all_consumers(), fn consumer -> is_nil(consumer.sequence_id) end)
  end

  def group_column_values(%SinkConsumer{} = consumer, record_data) do
    table = Sequin.Enum.find!(consumer.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))
    group_column_attnums = consumer.sequence_filter.group_column_attnums
    group_column_names = PostgresDatabaseTable.column_attnums_to_names(table, group_column_attnums)

    Enum.map(group_column_names, fn group_column_name ->
      Map.get(record_data.record, group_column_name)
    end)
  end
end
