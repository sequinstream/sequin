defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Cache
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.Message
  alias Sequin.Streams.Query
  alias Sequin.Streams.Stream

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # General

  def reload(%Message{} = msg) do
    # Repo.reload/2 does not support compound pks
    msg.subject |> Message.where_subject_and_stream_id(msg.stream_id) |> Repo.one()
  end

  def reload(%ConsumerMessage{} = cm) do
    cm.consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_subject(cm.message_subject)
    |> Repo.one()
  end

  def maybe_seed do
    if Sequin.Repo.all(Account) == [] do
      account = Sequin.Repo.insert!(%Account{})
      {:ok, _stream} = create_stream_for_account_with_lifecycle(account.id, %{slug: "default"})

      Logger.info("Created default account and stream")
    end
  end

  # Streams

  def list_streams_for_account(account_id) do
    account_id |> Stream.where_account_id() |> Repo.all()
  end

  def get_stream_for_account(account_id, id_or_slug) do
    res = account_id |> Stream.where_account_id() |> Stream.where_id_or_slug(id_or_slug) |> Repo.one()

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
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  def delete_stream_with_lifecycle(%Stream{} = stream) do
    Repo.transaction(fn ->
      case delete_stream(stream) do
        {:ok, stream} ->
          drop_records_partition(stream)
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
    CREATE TABLE #{stream_schema()}.messages_#{stream.slug} PARTITION OF #{stream_schema()}.messages FOR VALUES IN ('#{stream.id}');
    """)
  end

  def delete_stream(%Stream{} = stream) do
    Repo.delete(stream)
  end

  defp drop_records_partition(%Stream{} = stream) do
    Repo.query!("""
    DROP TABLE IF EXISTS #{stream_schema()}.messages_#{stream.slug};
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

  def get_consumer_for_account(account_id, id_or_slug) do
    res = account_id |> Consumer.where_account_id() |> Consumer.where_id_or_slug(id_or_slug) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_consumer_for_stream(stream_id, id_or_slug) do
    res = stream_id |> Consumer.where_stream_id() |> Consumer.where_id_or_slug(id_or_slug) |> Repo.one()

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
            {:ok, _} = ConsumerBackfillWorker.create(consumer.id)
          end

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
          :ok ->
            :ok = delete_consumer_partition(consumer)
            consumer

          error ->
            error
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
    CREATE TABLE #{stream_schema()}.consumer_messages_#{consumer.stream.slug}_#{consumer.slug} PARTITION OF #{stream_schema()}.consumer_messages FOR VALUES IN ('#{consumer.id}');
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
    DROP TABLE IF EXISTS #{stream_schema()}.consumer_messages_#{consumer.stream.slug}_#{consumer.slug};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def next_for_consumer(%Consumer{} = consumer, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    {:ok, messages} =
      Query.next_for_consumer(
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

      {:subject_pattern, pattern}, query ->
        Message.where_subject_pattern(query, pattern)
    end)
  end

  def list_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.all()
  end

  def get_message_for_stream(stream_id, key) do
    res =
      stream_id
      |> Message.where_key(key)
      |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :message)}
      message -> {:ok, message}
    end
  end

  def get_message_for_stream!(subject, stream_id) do
    case get_message_for_stream(stream_id, subject) do
      {:ok, message} -> message
      {:error, _} -> raise Error.not_found(entity: :message)
    end
  end

  def count_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.aggregate(:count, :subject)
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
    %Stream{slug: slug} = Repo.get!(Stream, stream_id)

    query = """
    SELECT pg_total_relation_size('#{stream_schema()}.messages_#{slug}') AS size
    """

    case Repo.query(query) do
      {:ok, %{rows: [[size]]}} -> size
      _ -> 0
    end
  end

  def upsert_messages(stream_id, messages, is_retry? \\ false) do
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

    Repo.transact(fn ->
      {count, messages} =
        Repo.insert_all(
          Message,
          messages,
          on_conflict: on_conflict,
          conflict_target: [:subject, :stream_id],
          timeout: :timer.seconds(30),
          returning: [:subject, :stream_id, :seq]
        )

      consumers
      |> Enum.reject(&(&1.backfill_completed_at == nil))
      |> Enum.flat_map(fn consumer ->
        messages
        |> Enum.filter(fn message -> Sequin.Subject.matches?(consumer.filter_subject_pattern, message.subject) end)
        |> Enum.map(fn message ->
          %ConsumerMessage{consumer_id: consumer.id, message_subject: message.subject, message_seq: message.seq}
        end)
      end)
      |> upsert_consumer_messages()

      {:ok, count}
    end)
  rescue
    e in Postgrex.Error ->
      if e.postgres.code == :character_not_in_repertoire and is_retry? == false do
        messages =
          Enum.map(messages, fn %{data: data} = message ->
            Map.put(message, :data, String.replace(data, "\u0000", ""))
          end)

        upsert_messages(stream_id, messages, true)
      else
        reraise e, __STACKTRACE__
      end
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

        {:subject_pattern, pattern}, query ->
          ConsumerMessage.where_subject_pattern(query, pattern)
      end)

    query
    |> select([cm, m], %{consumer_message: cm, message: m})
    |> Repo.all()
    |> Enum.map(fn %{consumer_message: cm, message: m} ->
      %{cm | message: m}
    end)
  end

  def get_consumer_message!(consumer_id, message_subject) do
    # Repo.get!(ConsumerMessage, consumer_id: consumer_id, message_subject: message_subject)
    consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_subject(message_subject)
    |> Repo.one!()
  end

  def upsert_consumer_messages([]), do: {:ok, []}

  def upsert_consumer_messages(consumer_messages) do
    {consumer_ids, message_subjects, message_seqs} =
      consumer_messages
      |> Enum.map(fn message ->
        {message.consumer_id, message.message_subject, message.message_seq}
      end)
      |> Enum.reduce({[], [], []}, fn {consumer_id, message_subject, message_seq}, {ids, subjects, seqs} ->
        {[consumer_id | ids], [message_subject | subjects], [message_seq | seqs]}
      end)

    Query.upsert_consumer_messages(
      consumer_ids: Enum.map(consumer_ids, &UUID.string_to_binary!/1),
      message_subjects: message_subjects,
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
        on: cm.consumer_id == acm.consumer_id and cm.message_subject == acm.message_subject
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
end
