defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Cache
  alias Sequin.Error
  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.ConsumerMessageWithConsumerInfoss
  alias Sequin.Streams.Message
  alias Sequin.Streams.Migrations
  alias Sequin.Streams.NewMessage
  alias Sequin.Streams.Query
  alias Sequin.Streams.SourceTable
  alias Sequin.Streams.Stream
  alias Sequin.Streams.StreamTable
  alias Sequin.Streams.StreamTableColumn
  alias Sequin.StreamsRuntime

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # General

  def reload(%Message{} = msg) do
    # Repo.reload/2 does not support compound pks
    msg.key |> Message.where_key_and_stream_id(msg.stream_id) |> Repo.one()
  end

  def reload(%ConsumerMessage{} = cm) do
    cm.consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_key(cm.message_key)
    |> Repo.one()
  end

  def maybe_seed do
    if Sequin.Repo.all(Account) == [] do
      account = Sequin.Repo.insert!(%Account{})
      {:ok, _stream} = create_stream_for_account_with_lifecycle(account.id, %{name: "default"})

      Logger.info("Created default account and stream")
    end
  end

  # Streams

  def list_streams_for_account(account_id) do
    account_id |> Stream.where_account_id() |> Stream.order_by(desc: :inserted_at) |> Repo.all()
  end

  def get_stream_for_account(account_id, id_or_name) do
    res = account_id |> Stream.where_account_id() |> Stream.where_id_or_name(id_or_name) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :stream)}
      stream -> {:ok, stream}
    end
  end

  def create_stream_for_account_with_lifecycle(account_id, attrs) do
    Repo.transaction(fn ->
      case create_stream(account_id, attrs) do
        {:ok, stream} ->
          create_records_partition(stream)
          SequinWeb.ObserveChannel.broadcast("stream:created", stream)
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  def delete_stream_with_lifecycle(%Stream{} = stream) do
    Repo.transaction(fn ->
      consumers = list_consumers_for_stream(stream.id)
      Enum.each(consumers, fn consumer -> {:ok, _} = delete_consumer_with_lifecycle(consumer) end)

      webhooks = Replication.list_webhooks_for_stream(stream.id)
      Enum.each(webhooks, fn webhook -> {:ok, _} = Replication.delete_webhook(webhook) end)

      case delete_stream(stream) do
        {:ok, stream} ->
          drop_records_partition(stream)
          SequinWeb.ObserveChannel.broadcast("stream:deleted", stream)
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  def all_streams, do: Repo.all(Stream)

  def create_stream(account_id, attrs) do
    %Stream{account_id: account_id}
    |> Stream.changeset(attrs)
    |> Repo.insert()
  end

  defp create_records_partition(%Stream{} = stream) do
    Repo.query!("""
    CREATE TABLE #{stream_schema()}.messages_#{stream.name} PARTITION OF #{stream_schema()}.messages FOR VALUES IN ('#{stream.id}');
    """)
  end

  def delete_stream(%Stream{} = stream) do
    Repo.delete(stream)
  end

  defp drop_records_partition(%Stream{} = stream) do
    Repo.query!("""
    DROP TABLE IF EXISTS #{stream_schema()}.messages_#{stream.name};
    """)
  end

  # Consumers

  def all_consumers do
    Repo.all(Consumer)
  end

  def count_consumers_for_stream(stream_id) do
    stream_id |> Consumer.where_stream_id() |> Repo.aggregate(:count, :id)
  end

  def get_consumer(consumer_id) do
    case consumer_id |> Consumer.where_id() |> Repo.one() do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_consumer!(consumer_id) do
    case get_consumer(consumer_id) do
      {:ok, consumer} -> consumer
      {:error, _} -> raise Error.not_found(entity: :consumer)
    end
  end

  def list_consumers_for_account(account_id) do
    account_id |> Consumer.where_account_id() |> Repo.all()
  end

  def list_consumers_for_stream(stream_id) do
    stream_id |> Consumer.where_stream_id() |> Repo.all()
  end

  def list_active_push_consumers do
    :push
    |> Consumer.where_kind()
    |> Consumer.where_status(:active)
    |> Repo.all()
  end

  def cached_list_consumers_for_stream(stream_id) do
    Cache.get_or_store(
      list_consumers_for_stream_cache_key(stream_id),
      fn -> list_consumers_for_stream(stream_id) end,
      :timer.minutes(10)
    )
  end

  def delete_cached_list_consumers_for_stream(stream_id) do
    Cache.delete(list_consumers_for_stream_cache_key(stream_id))
  end

  defp list_consumers_for_stream_cache_key(stream_id), do: "list_consumers_for_stream_#{stream_id}"

  def get_consumer_for_account(account_id, id_or_name) do
    res = account_id |> Consumer.where_account_id() |> Consumer.where_id_or_name(id_or_name) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_consumer_for_stream(stream_id, id_or_name) do
    res = stream_id |> Consumer.where_stream_id() |> Consumer.where_id_or_name(id_or_name) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def create_consumer_for_account_with_lifecycle(account_id, attrs, opts \\ []) do
    res =
      Repo.transact(fn ->
        with {:ok, consumer} <- create_consumer(account_id, attrs),
             :ok <- create_consumer_partition(consumer) do
          unless opts[:no_backfill] do
            backfill_consumer!(consumer)
          end

          if consumer.kind == :push and env() != :test do
            StreamsRuntime.Supervisor.start_for_push_consumer(consumer)
          end

          consumer = Repo.reload!(consumer)

          SequinWeb.ObserveChannel.broadcast("consumer:created", consumer)

          {:ok, consumer}
        end
      end)

    case res do
      {:ok, consumer} ->
        delete_cached_list_consumers_for_stream(consumer.stream_id)

        {:ok, consumer}

      error ->
        error
    end
  end

  def create_consumer_with_lifecycle(attrs, opts \\ []) do
    account_id = Map.fetch!(attrs, :account_id)

    create_consumer_for_account_with_lifecycle(account_id, attrs, opts)
  end

  def delete_consumer_with_lifecycle(consumer) do
    res =
      Repo.transact(fn ->
        case delete_consumer(consumer) do
          {:ok, _} ->
            :ok = delete_consumer_partition(consumer)
            SequinWeb.ObserveChannel.broadcast("consumer:deleted", consumer)
            {:ok, consumer}

          {:error, error} ->
            {:error, error}
        end
      end)

    case res do
      {:ok, consumer} ->
        delete_cached_list_consumers_for_stream(consumer.stream_id)
        {:ok, consumer}

      error ->
        error
    end
  end

  def update_consumer_with_lifecycle(%Consumer{} = consumer, attrs) do
    consumer
    |> Consumer.update_changeset(attrs)
    |> Repo.update()
    |> case do
      {:ok, consumer} ->
        delete_cached_list_consumers_for_stream(consumer.stream_id)
        SequinWeb.ObserveChannel.broadcast("consumer:updated", consumer)
        {:ok, consumer}

      error ->
        error
    end
  end

  def create_consumer(account_id, attrs) do
    %Consumer{account_id: account_id}
    |> Consumer.create_changeset(attrs)
    |> Repo.insert()
  end

  def delete_consumer(%Consumer{} = consumer) do
    Repo.delete(consumer)
  end

  defp create_consumer_partition(%Consumer{} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    CREATE TABLE #{stream_schema()}.consumer_messages_#{consumer.stream.name}_#{consumer.name} PARTITION OF #{stream_schema()}.consumer_messages FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%Consumer{} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    DROP TABLE IF EXISTS #{stream_schema()}.consumer_messages_#{consumer.stream.name}_#{consumer.name};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def receive_for_consumer(%Consumer{} = consumer, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    {:ok, messages} =
      Query.receive_for_consumer(
        batch_size: batch_size,
        consumer_id: UUID.string_to_binary!(consumer.id),
        max_ack_pending: max_ack_pending,
        not_visible_until: not_visible_until,
        now: now
      )

    messages =
      Enum.map(messages, fn message ->
        message
        |> Map.update!(:stream_id, &UUID.binary_to_string!/1)
        |> Map.update!(:ack_id, &UUID.binary_to_string!/1)
        |> Map.update!(:inserted_at, &DateTime.from_naive!(&1, "Etc/UTC"))
        |> Map.update!(:updated_at, &DateTime.from_naive!(&1, "Etc/UTC"))
        |> then(&struct!(Message, &1))
      end)

    {:ok, messages}
  end

  def backfill_limit, do: 10_000

  # We make the first backfill pull synchronous and maybe mark the consumer as backfilled
  # The Oban job always runs to catch up with race conditions then to clear acked messages
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
      list_messages_for_stream(consumer.stream_id,
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
      |> upsert_consumer_messages()

    {:ok, messages}
  end

  # Messages

  defp messages_query(stream_id, params) do
    Enum.reduce(params, Message.where_stream_id(stream_id), fn
      {:seq_gt, seq}, query ->
        Message.where_seq_gt(query, seq)

      {:limit, limit}, query ->
        limit(query, ^limit)

      {:select, select}, query ->
        select(query, ^select)

      {:order_by, order_by}, query ->
        order_by(query, ^order_by)

      {:key_pattern, pattern}, query ->
        Message.where_key_pattern(query, pattern)

      {:keys, keys}, query ->
        Message.where_key_in(query, keys)
    end)
  end

  def list_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.all()
  end

  def get_message_for_stream(stream_id, key) do
    res =
      key
      |> Message.where_key_and_stream_id(stream_id)
      |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :message)}
      message -> {:ok, message}
    end
  end

  def get_message_for_stream!(stream_id, key) do
    case get_message_for_stream(stream_id, key) do
      {:ok, message} -> message
      {:error, _} -> raise Error.not_found(entity: :message)
    end
  end

  def count_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.aggregate(:count, :key)
  end

  @fast_count_threshold 50_000
  def fast_count_threshold, do: @fast_count_threshold

  def fast_count_messages_for_stream(stream_id, params \\ []) do
    query = messages_query(stream_id, params)

    # This number can be pretty inaccurate
    result = Ecto.Adapters.SQL.explain(Repo, :all, query)
    [_, rows] = Regex.run(~r/rows=(\d+)/, result)

    case String.to_integer(rows) do
      count when count > @fast_count_threshold ->
        count

      _ ->
        count_messages_for_stream(stream_id, params)
    end
  end

  def approximate_storage_size_for_stream(stream_id) do
    %Stream{name: name} = Repo.get!(Stream, stream_id)

    query = """
    SELECT pg_total_relation_size('#{stream_schema()}.messages_#{name}') AS size
    """

    case Repo.query(query) do
      {:ok, %{rows: [[size]]}} -> size
      _ -> 0
    end
  end

  def upsert_messages(stream_id, messages) do
    with :ok <- validate_messages(messages) do
      do_upsert_messages(stream_id, messages)
    end
  end

  defp do_upsert_messages(stream_id, messages, is_retry? \\ false) do
    now = DateTime.utc_now()

    messages =
      Enum.map(messages, fn message ->
        message
        |> Sequin.Map.from_ecto()
        |> Message.put_tokens()
        |> Message.put_data_hash()
        |> Map.put(:updated_at, now)
        |> Map.put(:inserted_at, now)
        |> Map.put(:stream_id, stream_id)
      end)

    seq_nextval = "#{stream_schema()}.messages_seq"

    on_conflict =
      from(m in Message,
        where: fragment("? IS DISTINCT FROM ?", m.data_hash, fragment("EXCLUDED.data_hash")),
        update: [
          set: [
            data: fragment("EXCLUDED.data"),
            data_hash: fragment("EXCLUDED.data_hash"),
            seq: fragment("nextval(?::text::regclass)", ^seq_nextval),
            updated_at: fragment("EXCLUDED.updated_at")
          ]
        ]
      )

    consumers = cached_list_consumers_for_stream(stream_id)

    fn ->
      {count, messages} =
        Repo.insert_all(
          Message,
          messages,
          on_conflict: on_conflict,
          conflict_target: [:key, :stream_id],
          timeout: :timer.seconds(30),
          # FIXME: Do not select data here. It's just to pass data to ObserveChannel.
          returning: [:key, :stream_id, :seq]
        )

      consumers
      |> Enum.reject(&(&1.backfill_completed_at == nil))
      |> Enum.flat_map(fn consumer ->
        messages
        |> Enum.filter(fn message -> Sequin.Key.matches?(consumer.filter_key_pattern, message.key) end)
        |> Enum.map(fn message ->
          %ConsumerMessage{consumer_id: consumer.id, message_key: message.key, message_seq: message.seq}
        end)
      end)
      |> upsert_consumer_messages()

      {:ok, %{count: count, messages: messages}}
    end
    |> Repo.transact()
    |> case do
      {:ok, %{count: count, messages: messages}} ->
        SequinWeb.ObserveChannel.broadcast("messages:upserted", {stream_id, messages})
        {:ok, count}

      {:error, e} ->
        {:error, e}
    end
  rescue
    e in Postgrex.Error ->
      if e.postgres.code == :character_not_in_repertoire and is_retry? == false do
        messages =
          Enum.map(messages, fn %{data: data} = message ->
            Map.put(message, :data, String.replace(data, "\u0000", ""))
          end)

        do_upsert_messages(stream_id, messages, true)
      else
        reraise e, __STACKTRACE__
      end
  end

  defp validate_messages(messages) do
    Enum.reduce_while(messages, :ok, fn message, :ok ->
      case message do
        %{key: key, data: data} when is_binary(key) and is_binary(data) ->
          case Sequin.Key.validate_key(key) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, Error.bad_request(message: "Invalid key format: #{reason}")}}
          end

        _ ->
          {:halt, {:error, Error.bad_request(message: "Invalid message format")}}
      end
    end)
  end

  # Consumer Messages

  def all_consumer_messages do
    Repo.all(ConsumerMessage)
  end

  def list_consumer_messages_for_consumer(stream_id, consumer_id, params \\ []) do
    base_query =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.join_message(stream_id)
      |> ConsumerMessage.where_state_not(:acked)

    query =
      Enum.reduce(params, base_query, fn
        {:state, state}, query ->
          ConsumerMessage.where_state(query, state)

        {:state_in, states}, query ->
          ConsumerMessage.where_state_in(query, states)

        {:is_deliverable, false}, query ->
          ConsumerMessage.where_not_visible(query)

        {:is_deliverable, true}, query ->
          ConsumerMessage.where_deliverable(query)

        {:limit, limit}, query ->
          limit(query, ^limit)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)

        {:key_pattern, pattern}, query ->
          ConsumerMessage.where_key_pattern(query, pattern)
      end)

    query
    |> select([cm, m], %{consumer_message: cm, message: m})
    |> Repo.all()
    |> Enum.map(fn %{consumer_message: cm, message: m} ->
      %{cm | message: m}
    end)
  end

  def get_consumer_details_for_message(message_key, stream_id) do
    consumers = cached_list_consumers_for_stream(stream_id)

    consumer_message_details =
      consumers
      |> Enum.filter(fn consumer -> Sequin.Key.matches?(consumer.filter_key_pattern, message_key) end)
      |> Enum.map(fn consumer ->
        case get_consumer_message(consumer.id, message_key) do
          {:ok, consumer_message} ->
            %ConsumerMessageWithConsumerInfoss{
              consumer_id: consumer.id,
              consumer_name: consumer.name,
              consumer_filter_key_pattern: consumer.filter_key_pattern,
              state: ConsumerMessage.external_state(consumer_message),
              ack_id: consumer_message.ack_id,
              deliver_count: consumer_message.deliver_count,
              last_delivered_at: consumer_message.last_delivered_at,
              not_visible_until: consumer_message.not_visible_until
            }

          {:error, %Error.NotFoundError{}} ->
            external_state =
              if Consumer.should_delete_acked_messages?(consumer), do: :acked, else: :available

            %ConsumerMessageWithConsumerInfoss{
              consumer_id: consumer.id,
              consumer_name: consumer.name,
              consumer_filter_key_pattern: consumer.filter_key_pattern,
              state: external_state,
              ack_id: nil,
              deliver_count: nil,
              last_delivered_at: nil,
              not_visible_until: nil
            }
        end
      end)

    {:ok, consumer_message_details}
  end

  def get_consumer_message(consumer_id, message_key) do
    consumer_message =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_message_key(message_key)
      |> Repo.one()

    case consumer_message do
      nil -> {:error, Error.not_found(entity: :consumer_message)}
      consumer_message -> {:ok, consumer_message}
    end
  end

  def get_consumer_message!(consumer_id, message_key) do
    case get_consumer_message(consumer_id, message_key) do
      {:ok, consumer_message} -> consumer_message
      {:error, _} -> raise Error.not_found(entity: :consumer_message)
    end
  end

  def upsert_consumer_messages([]), do: {:ok, []}

  def upsert_consumer_messages(consumer_messages) do
    {consumer_ids, message_keys, message_seqs} =
      consumer_messages
      |> Enum.map(fn message ->
        {message.consumer_id, message.message_key, message.message_seq}
      end)
      |> Enum.reduce({[], [], []}, fn {consumer_id, message_key, message_seq}, {ids, keys, seqs} ->
        {[consumer_id | ids], [message_key | keys], [message_seq | seqs]}
      end)

    Query.upsert_consumer_messages(
      consumer_ids: Enum.map(consumer_ids, &UUID.string_to_binary!/1),
      message_keys: message_keys,
      message_seqs: message_seqs
    )
  end

  def delete_acked_consumer_messages_for_consumer(consumer_id, limit \\ 1000) do
    subquery =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_state(:acked)
      |> limit(^limit)

    query =
      from(cm in ConsumerMessage,
        join: acm in subquery(subquery),
        on: cm.consumer_id == acm.consumer_id and cm.message_key == acm.message_key
      )

    Repo.delete_all(query)
  end

  @spec ack_messages(Sequin.Streams.Consumer.t(), any()) :: :ok
  def ack_messages(%Consumer{} = consumer, ack_ids) do
    Repo.transact(fn ->
      {_, _} =
        consumer.id
        |> ConsumerMessage.where_consumer_id()
        |> ConsumerMessage.where_ack_ids(ack_ids)
        |> ConsumerMessage.where_state(:pending_redelivery)
        |> Repo.update_all(set: [state: :available, not_visible_until: nil])

      if Consumer.should_delete_acked_messages?(consumer) do
        {_, _} =
          consumer.id
          |> ConsumerMessage.where_consumer_id()
          |> ConsumerMessage.where_ack_ids(ack_ids)
          |> ConsumerMessage.where_state(:delivered)
          |> Repo.delete_all()
      else
        {_, _} =
          consumer.id
          |> ConsumerMessage.where_consumer_id()
          |> ConsumerMessage.where_ack_ids(ack_ids)
          |> ConsumerMessage.where_state(:delivered)
          |> Repo.update_all(set: [state: :acked])
      end

      :ok
    end)

    :ok
  end

  @spec nack_messages(Sequin.Streams.Consumer.t(), any()) :: :ok
  def nack_messages(%Consumer{} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_state_not(:acked)
      |> ConsumerMessage.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [state: :available, not_visible_until: nil])

    :ok
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  def message_from_replication_change(%SourceTable{} = source_table, change) do
    user_fields =
      Enum.reduce(source_table.column_mappings, %{}, fn mapping, acc ->
        Map.put(acc, mapping.stream_column.name, extract_field_value(mapping.mapping, change, source_table))
      end)

    %NewMessage{
      data: %{
        record: get_record(change),
        changes: get_changes(change)
      },
      recorded_at: DateTime.utc_now(),
      user_fields: user_fields,
      deleted: is_struct(change, DeletedRecord)
    }
  end

  defp get_record(%NewRecord{record: record}), do: record
  defp get_record(%UpdatedRecord{record: record}), do: record
  defp get_record(%DeletedRecord{old_record: old_record}), do: old_record

  defp extract_field_value(%{type: :record_field, field_name: field_name}, change, _source_table) do
    case change do
      %NewRecord{record: record} -> record[field_name]
      %UpdatedRecord{record: record} -> record[field_name]
      %DeletedRecord{old_record: old_record} -> old_record[field_name]
    end
  end

  defp extract_field_value(%{type: :metadata, field_name: field_name}, change, source_table) do
    case field_name do
      "action" -> get_action(change)
      "table_name" -> source_table.name
    end
  end

  defp get_changes(%UpdatedRecord{old_record: old_record, record: record}) do
    old_record
    |> Map.new(fn {k, v} -> {k, if(record[k] == v, do: nil, else: v)} end)
    |> Map.reject(fn {_, v} -> is_nil(v) end)
  end

  defp get_changes(%NewRecord{}), do: nil
  defp get_changes(%DeletedRecord{}), do: nil

  defp get_action(%NewRecord{}), do: :insert
  defp get_action(%UpdatedRecord{}), do: :update
  defp get_action(%DeletedRecord{}), do: :delete

  # StreamTable

  def list_stream_tables_for_account(account_id) do
    account_id |> StreamTable.where_account_id() |> Repo.all()
  end

  def get_stream_table_for_account(account_id, id) do
    res = account_id |> StreamTable.where_account_id() |> StreamTable.where_id(id) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :stream_table)}
      stream_table -> {:ok, stream_table}
    end
  end

  def create_stream_table_for_account_with_lifecycle(account_id, attrs) do
    Repo.transact(fn ->
      with {:ok, stream_table} <- create_stream_table(account_id, attrs),
           stream_table = Repo.preload(stream_table, :columns),
           :ok <- Migrations.provision_stream_table(stream_table) do
        {:ok, stream_table}
      end
    end)
  end

  def update_stream_table_with_lifecycle(%StreamTable{} = stream_table, attrs) do
    stream_table = Repo.preload(stream_table, :columns)

    Repo.transact(fn ->
      with {:ok, updated_stream_table} <- update_stream_table(stream_table, attrs),
           updated_stream_table = Repo.preload(updated_stream_table, :columns),
           :ok <- Migrations.migrate_stream_table(stream_table, updated_stream_table) do
        {:ok, updated_stream_table}
      end
    end)
  end

  def delete_stream_table_with_lifecycle(%StreamTable{} = stream_table) do
    res =
      Repo.transact(fn ->
        with {:ok, _} <- delete_stream_table(stream_table) do
          Migrations.drop_stream_table(stream_table)
        end
      end)

    case res do
      {:ok, :transaction_committed} -> :ok
      error -> error
    end
  end

  def create_stream_table(account_id, attrs) do
    %StreamTable{account_id: account_id}
    |> StreamTable.create_changeset(attrs)
    |> Repo.insert()
  end

  def update_stream_table(%StreamTable{} = stream_table, attrs) do
    stream_table
    |> StreamTable.update_changeset(attrs)
    |> Repo.update()
  end

  def delete_stream_table(%StreamTable{} = stream_table) do
    Repo.delete(stream_table)
  end

  def insert_into_stream_table(%StreamTable{insert_mode: :append} = stream_table, records) do
    stream_table = Repo.preload(stream_table, :columns)

    Repo.insert_all(stream_table.table_name, records_for_insert(stream_table.columns, records),
      prefix: stream_table.table_schema_name
    )
  end

  def insert_into_stream_table(%StreamTable{insert_mode: :upsert} = stream_table, records) do
    stream_table = Repo.preload(stream_table, :columns)

    conflict_target =
      stream_table.columns
      |> Enum.filter(& &1.is_conflict_key)
      |> Enum.map(& &1.name)

    replace =
      stream_table
      |> StreamTable.all_column_names()
      |> Enum.reject(&(&1 in conflict_target or &1 == "sequin_id"))

    Repo.insert_all(
      stream_table.table_name,
      records_for_insert(stream_table.columns, records),
      conflict_target: conflict_target,
      on_conflict: {:replace, replace},
      prefix: stream_table.table_schema_name
    )
  end

  defp records_for_insert(columns, records) do
    Enum.map(records, fn record ->
      record = Map.update!(record, "sequin_id", &UUID.string_to_binary!/1)

      Enum.reduce(columns, record, fn %StreamTableColumn{} = column, record ->
        if column.type == :uuid do
          Map.update!(record, to_string(column.name), &UUID.string_to_binary!/1)
        else
          record
        end
      end)
    end)
  end
end
