defmodule Sequin.Consumers do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.Query
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.ReplicationRuntime.Supervisor
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage

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
      :ok = Supervisor.refresh_message_handler_ctx(updated_consumer.replication_slot_id)

      {:ok, updated_consumer}
    end
  end

  def delete_consumer_with_lifecycle(consumer) do
    Repo.transact(fn ->
      case delete_consumer(consumer) do
        {:ok, _} ->
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

  # HttpPullConsumer

  def get_http_pull_consumer(consumer_id) do
    case Repo.get(HttpPullConsumer, consumer_id) do
      nil -> {:error, Error.not_found(entity: :http_pull_consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_http_pull_consumer_for_account(account_id, id_or_name) do
    res = account_id |> HttpPullConsumer.where_account_id() |> HttpPullConsumer.where_id_or_name(id_or_name) |> Repo.one()

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
    :push
    |> HttpPushConsumer.where_kind()
    |> HttpPushConsumer.where_status(:active)
    |> Repo.all()
  end

  def create_http_push_consumer_for_account_with_lifecycle(account_id, attrs, opts \\ []) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_http_push_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        unless opts[:no_backfill] do
          backfill_consumer!(consumer)
        end

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
  def insert_consumer_records(consumer_records) do
    now = DateTime.utc_now()

    records =
      Enum.map(consumer_records, fn record ->
        record
        |> Map.put(:inserted_at, now)
        |> Map.put(:updated_at, now)
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

  # Consumer Lifecycle

  defp create_consumer_partition(%{message_kind: :event} = consumer) do
    """
    CREATE TABLE #{stream_schema()}.consumer_events_#{consumer.name} PARTITION OF #{stream_schema()}.consumer_events FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp create_consumer_partition(%{message_kind: :record} = consumer) do
    """
    CREATE TABLE #{stream_schema()}.consumer_records_#{consumer.name} PARTITION OF #{stream_schema()}.consumer_records FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%{message_kind: :event} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    DROP TABLE IF EXISTS #{stream_schema()}.consumer_events_#{consumer.name};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%{message_kind: :record} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    DROP TABLE IF EXISTS #{stream_schema()}.consumer_records_#{consumer.name};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  # Consuming / Acking Messages
  @spec receive_for_consumer(consumer(), keyword()) :: {:ok, [ConsumerEvent.t()]} | {:ok, [ConsumerRecord.t()]}
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

        {:ok, events}
    end
  end

  # TODO: Populate data by joining against source tables
  def receive_for_consumer(%{message_kind: :record} = consumer, opts) do
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

        {:ok, records}
    end
  end

  @spec ack_messages(consumer(), [integer()]) :: :ok
  def ack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> Repo.delete_all()

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

  # Backfills

  def backfill_limit, do: 10_000

  defp backfill_consumer!(consumer) do
    {:ok, messages} = backfill_messages_for_consumer(consumer)

    if length(messages) < backfill_limit() do
      {:ok, _} = update_consumer_with_lifecycle(consumer, %{backfill_completed_at: DateTime.utc_now()})
    end

    next_seq = messages |> Enum.map(& &1.seq) |> Enum.max(fn -> 0 end)
    {:ok, _} = ConsumerBackfillWorker.create(consumer.id, next_seq)

    :ok
  end

  def backfill_messages_for_consumer(consumer, seq \\ 0) do
    messages =
      Streams.list_messages_for_stream(consumer.stream_id,
        seq_gt: seq,
        limit: backfill_limit(),
        order_by: [asc: :seq],
        select: [:key, :seq]
      )

    {:ok, _} =
      messages
      |> Enum.filter(fn message ->
        Sequin.Key.matches?(consumer.filter_key_pattern, message.key)
      end)
      |> Enum.map(fn message ->
        %ConsumerMessage{
          consumer_id: consumer.id,
          message_key: message.key,
          message_seq: message.seq
        }
      end)

    # |> then(&upsert_consumer_messages(consumer, &1))

    {:ok, messages}
  end

  # Source Table Matching

  def matches_message?(consumer, message) do
    Enum.any?(consumer.source_tables, fn source_table ->
      source_table.oid == message.table_oid &&
        action_matches?(source_table.actions, message.action) &&
        column_filters_match?(source_table.column_filters, message)
    end)
  end

  defp action_matches?(source_table_actions, message_action) do
    message_action in source_table_actions
  end

  defp column_filters_match?([], _message), do: true

  defp column_filters_match?(column_filters, message) do
    Enum.all?(column_filters, fn filter ->
      field = Enum.find(message.fields, &(&1.column_attnum == filter.column_attnum))
      field && apply_filter(filter.operator, field.value, filter.value)
    end)
  end

  defp apply_filter(:==, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.compare(field_value, filter_value) == :eq

  defp apply_filter(:!=, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.compare(field_value, filter_value) != :eq

  defp apply_filter(:>, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.after?(field_value, filter_value)

  defp apply_filter(:<, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.before?(field_value, filter_value)

  defp apply_filter(:>=, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.compare(field_value, filter_value) in [:gt, :eq]

  defp apply_filter(:<=, field_value, %{value: filter_value, __type__: :datetime}),
    do: DateTime.compare(field_value, filter_value) in [:lt, :eq]

  defp apply_filter(op, field_value, %{value: filter_value}) when op in [:==, :!=, :>, :<, :>=, :<=],
    do: apply(Kernel, op, [field_value, filter_value])

  defp apply_filter(:is_null, field_value, _), do: is_nil(field_value)
  defp apply_filter(:not_null, field_value, _), do: not is_nil(field_value)

  defp apply_filter(:in, field_value, %{value: filter_value}) when is_list(filter_value) do
    field_value in filter_value or to_string(field_value) in Enum.map(filter_value, &to_string/1)
  end

  defp apply_filter(:not_in, field_value, %{value: filter_value}) when is_list(filter_value) do
    field_value not in filter_value and to_string(field_value) not in Enum.map(filter_value, &to_string/1)
  end
end
