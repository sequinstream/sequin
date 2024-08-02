defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Cache
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Sources
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.ConsumerMessageWithConsumerInfoss
  alias Sequin.Streams.Message
  alias Sequin.Streams.Query
  alias Sequin.Streams.Stream
  alias Sequin.StreamsRuntime

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # General

  def reload(%Message{} = msg) do
    # Repo.reload/2 does not support compound pks
    msg.id |> Message.where_id_and_stream_id(msg.stream_id) |> Repo.one()
  end

  def reload(%ConsumerMessage{} = cm) do
    cm.consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_id(cm.message_id)
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

  def get_stream(stream_id) do
    stream_id
    |> Stream.where_id()
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :stream)}
      stream -> {:ok, stream}
    end
  end

  def create_stream_for_account_with_lifecycle(account_id, attrs) do
    Repo.transaction(fn ->
      case create_stream(account_id, attrs) do
        {:ok, stream} ->
          create_messages_partition(stream)
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

      webhooks = Sources.list_webhooks_for_stream(stream.id)
      Enum.each(webhooks, fn webhook -> {:ok, _} = Sources.delete_webhook(webhook) end)

      case delete_stream(stream) do
        {:ok, stream} ->
          drop_messages_partition(stream)
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

  defp create_messages_partition(%Stream{} = stream) do
    Repo.query!("""
    CREATE TABLE #{stream_schema()}.#{message_table(stream)} PARTITION OF #{stream_schema()}.messages FOR VALUES IN ('#{stream.id}');
    """)

    if stream.one_message_per_key do
      Repo.query!("""
      ALTER TABLE #{stream_schema()}.#{message_table(stream)}
      ADD CONSTRAINT #{message_table(stream)}_key_unique UNIQUE (stream_id, key);
      """)
    end
  end

  defp message_table(%Stream{name: name}), do: "messages_#{name}"

  def delete_stream(%Stream{} = stream) do
    Repo.delete(stream)
  end

  defp drop_messages_partition(%Stream{} = stream) do
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
        with {:ok, %Consumer{} = consumer} <- create_consumer(account_id, attrs),
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
    |> Repo.insert(preload: [:stream])
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
      {:ok, %Postgrex.Result{command: :create_table}} ->
        if consumer.stream.one_message_per_key do
          Repo.query!("""
          ALTER TABLE #{stream_schema()}.consumer_messages_#{consumer.stream.name}_#{consumer.name}
          ADD CONSTRAINT consumer_messages_#{consumer.stream.name}_#{consumer.name}_message_key_unique UNIQUE (consumer_id, message_key);
          """)
        end

        :ok

      {:error, error} ->
        {:error, error}
    end
  end

  defp consumer_partition_name(%Consumer{} = consumer) do
    "consumer_messages_#{consumer.stream.name}_#{consumer.name}"
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
        |> Map.update!(:id, &UUID.binary_to_string!/1)
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
    consumer = Repo.preload(consumer, :stream)

    messages =
      list_messages_for_stream(consumer.stream_id,
        seq_gt: seq,
        limit: backfill_limit(),
        order_by: [asc: :seq],
        select: [:id, :key, :seq]
      )

    operation = if consumer.stream.one_message_per_key, do: :upsert, else: :insert

    {:ok, _} =
      messages
      |> Enum.filter(fn message ->
        Sequin.Key.matches?(consumer.filter_key_pattern, message.key)
      end)
      |> Enum.map(fn message ->
        %ConsumerMessage{
          consumer_id: consumer.id,
          message_id: message.id,
          message_key: message.key,
          message_seq: message.seq
        }
      end)
      |> then(&perform_consumer_message_operation(operation, consumer, &1))

    {:ok, messages}
  end

  # Messages

  defp messages_query(stream_id, params) do
    Enum.reduce(params, Message.where_stream_id(stream_id), fn
      {:ids, ids}, query ->
        Message.where_id_in(query, ids)

      {:key, key}, query ->
        Message.where_key(query, key)

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

  def get_message_for_stream(stream_id, id) do
    res =
      id
      |> Message.where_id_and_stream_id(stream_id)
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

  def send_messages(stream_id, messages) when is_binary(stream_id) do
    with {:ok, stream} <- get_stream(stream_id) do
      send_messages(stream, messages)
    end
  end

  def send_messages(%Stream{} = stream, messages) do
    with :ok <- validate_messages(messages) do
      operation = if stream.one_message_per_key, do: :upsert, else: :insert
      do_send_messages(stream, messages, operation)
    end
  end

  defp do_send_messages(%Stream{} = stream, messages, operation, is_retry? \\ false) do
    now = DateTime.utc_now()

    messages = prepare_messages(stream, messages, now)
    consumers = cached_list_consumers_for_stream(stream.id)

    fn ->
      {count, inserted_messages} = perform_message_operation(operation, stream, messages)

      Enum.each(consumers, fn consumer ->
        consumer_messages = create_consumer_messages(consumer, inserted_messages, now)
        perform_consumer_message_operation(operation, consumer, consumer_messages)
      end)

      {:ok, %{count: count, messages: inserted_messages}}
    end
    |> Repo.transact()
    |> handle_transaction_result(stream, operation)
  rescue
    e in Postgrex.Error ->
      if e.postgres.code == :character_not_in_repertoire and not is_retry? do
        messages =
          Enum.map(messages, fn %{data: data} = message ->
            Map.put(message, :data, String.replace(data, "\u0000", ""))
          end)

        do_send_messages(stream, messages, operation, true)
      else
        reraise e, __STACKTRACE__
      end
  end

  defp prepare_messages(stream, messages, now) do
    Enum.map(messages, fn message ->
      message
      |> Sequin.Map.from_ecto()
      |> Message.put_tokens()
      |> Message.put_data_hash()
      |> Map.put(:updated_at, now)
      |> Map.put(:inserted_at, now)
      |> Map.put(:stream_id, stream.id)
    end)
  end

  defp perform_message_operation(:upsert, %Stream{} = stream, messages) do
    seq_nextval = "#{stream_schema()}.messages_seq"
    on_conflict = upsert_conflict_clause(stream, seq_nextval)

    Repo.insert_all(
      {message_table(stream), Message},
      messages,
      on_conflict: on_conflict,
      conflict_target: [:key, :stream_id],
      timeout: :timer.seconds(30),
      returning: [:id, :stream_id, :key, :seq]
    )
  end

  defp perform_message_operation(:insert, _stream, messages) do
    Repo.insert_all(
      Message,
      messages,
      timeout: :timer.seconds(30),
      returning: [:id, :stream_id, :key, :seq]
    )
  end

  defp upsert_conflict_clause(%Stream{} = stream, seq_nextval) do
    from(m in {message_table(stream), Message},
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
  end

  defp create_consumer_messages(consumer, inserted_messages, now) do
    if is_nil(consumer.backfill_completed_at) do
      []
    else
      inserted_messages
      |> Enum.filter(fn message -> Sequin.Key.matches?(consumer.filter_key_pattern, message.key) end)
      |> Enum.map(fn message ->
        %{
          consumer_id: consumer.id,
          message_key: message.key,
          message_seq: message.seq,
          message_id: message.id,
          state: :available,
          updated_at: now,
          inserted_at: now
        }
      end)
    end
  end

  defp handle_transaction_result({:ok, %{count: count, messages: messages}}, stream, operation) do
    broadcast_event(operation, stream.id, messages)
    {:ok, count}
  end

  defp handle_transaction_result({:error, e}, _, _), do: {:error, e}

  defp broadcast_event(:upsert, stream_id, messages) do
    SequinWeb.ObserveChannel.broadcast("messages:upserted", {stream_id, messages})
  end

  defp broadcast_event(:insert, stream_id, messages) do
    SequinWeb.ObserveChannel.broadcast("messages:inserted", {stream_id, messages})
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

  def get_consumer_details_for_message(message_id, stream_id) do
    with {:ok, message} <- get_message_for_stream(stream_id, message_id) do
      consumers = cached_list_consumers_for_stream(stream_id)

      consumer_message_details =
        consumers
        |> Enum.filter(fn consumer -> Sequin.Key.matches?(consumer.filter_key_pattern, message.key) end)
        |> Enum.map(fn consumer ->
          case get_consumer_message(consumer.id, message_id) do
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

  defp perform_consumer_message_operation(_operation, _consumer, []), do: {:ok, []}

  defp perform_consumer_message_operation(:insert, _consumer, consumer_messages) do
    now = DateTime.utc_now()

    entries =
      Enum.map(consumer_messages, fn message ->
        %{
          consumer_id: message.consumer_id,
          message_key: message.message_key,
          message_seq: message.message_seq,
          message_id: message.message_id,
          state: :available,
          updated_at: now,
          inserted_at: now
        }
      end)

    {count, _} = Repo.insert_all(ConsumerMessage, entries)
    {:ok, count}
  end

  defp perform_consumer_message_operation(:upsert, %Consumer{} = consumer, consumer_messages) do
    # Preload stream to generate the partition name
    consumer = Repo.preload(consumer, :stream)

    now = DateTime.utc_now()

    entries =
      Enum.map(consumer_messages, fn message ->
        %{
          consumer_id: message.consumer_id,
          message_key: message.message_key,
          message_seq: message.message_seq,
          message_id: message.message_id,
          state: :available,
          updated_at: now,
          inserted_at: now
        }
      end)

    conflict_target = [:consumer_id, :message_key]

    {count, _} =
      Repo.insert_all({consumer_partition_name(consumer), ConsumerMessage}, entries,
        on_conflict: consumer_message_on_conflict_query(consumer),
        conflict_target: conflict_target
      )

    {:ok, count}
  end

  defp consumer_message_on_conflict_query(consumer) do
    from(cm in {consumer_partition_name(consumer), ConsumerMessage},
      update: [
        set: [
          state:
            fragment(
              """
                CASE ?
                  WHEN 'acked'::sequin_streams.consumer_message_state THEN 'available'::sequin_streams.consumer_message_state
                  WHEN 'available'::sequin_streams.consumer_message_state THEN 'available'::sequin_streams.consumer_message_state
                  WHEN 'delivered'::sequin_streams.consumer_message_state THEN 'pending_redelivery'::sequin_streams.consumer_message_state
                  WHEN 'pending_redelivery'::sequin_streams.consumer_message_state THEN 'pending_redelivery'::sequin_streams.consumer_message_state
                END
              """,
              cm.state
            ),
          message_seq:
            fragment(
              "CASE WHEN EXCLUDED.message_seq > ? THEN EXCLUDED.message_seq ELSE ? END",
              field(cm, :message_seq),
              cm.message_seq
            ),
          updated_at: fragment("NOW()")
        ]
      ],
      where: fragment("EXCLUDED.message_seq > ?", cm.message_seq)
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
end
