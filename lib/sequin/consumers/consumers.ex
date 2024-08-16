defmodule Sequin.Consumers do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Cache
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.Query
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage

  require Logger

  def reload(%ConsumerEvent{} = ce) do
    ce.consumer_id
    |> ConsumerEvent.where_consumer_id()
    |> ConsumerEvent.where_commit_lsn(ce.commit_lsn)
    |> Repo.one()
  end

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  def all_consumers do
    Repo.all(HttpPushConsumer)
  end

  def count_consumers_for_stream(_stream_id) do
    0
    # stream_id |> HttpPushConsumer.where_stream_id() |> Repo.aggregate(:count, :id)
  end

  def get_consumer(consumer_id) do
    case consumer_id |> HttpPushConsumer.where_id() |> Repo.one() do
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
    account_id |> HttpPushConsumer.where_account_id() |> Repo.all()
  end

  def list_consumers_for_stream(_stream_id) do
    Repo.all(HttpPushConsumer)
  end

  def list_active_push_consumers do
    :push
    |> HttpPushConsumer.where_kind()
    |> HttpPushConsumer.where_status(:active)
    |> Repo.all()
  end

  def cached_list_consumers_for_stream(stream_id) do
    Cache.get_or_store(
      list_consumers_for_stream_cache_key(stream_id),
      fn -> list_consumers_for_stream(stream_id) end,
      :timer.minutes(10)
    )
  end

  defp list_consumers_for_stream_cache_key(stream_id), do: "list_consumers_for_stream_#{stream_id}"

  def get_consumer_for_account(account_id, id_or_name) do
    res = account_id |> HttpPushConsumer.where_account_id() |> HttpPushConsumer.where_id_or_name(id_or_name) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def get_consumer_for_stream(_stream_id, id_or_name) do
    res = id_or_name |> HttpPushConsumer.where_id_or_name() |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def create_consumer_for_account_with_lifecycle(account_id, attrs, opts \\ []) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        unless opts[:no_backfill] do
          backfill_consumer!(consumer)
        end

        # if consumer.kind == :push and env() != :test do
        #   StreamsRuntime.Supervisor.start_for_push_consumer(consumer)
        # end

        consumer = Repo.reload!(consumer)

        {:ok, consumer}
      end
    end)
  end

  def create_consumer_with_lifecycle(attrs, opts \\ []) do
    account_id = Map.fetch!(attrs, :account_id)

    create_consumer_for_account_with_lifecycle(account_id, attrs, opts)
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

  def update_consumer_with_lifecycle(%HttpPushConsumer{} = consumer, attrs) do
    consumer
    |> HttpPushConsumer.update_changeset(attrs)
    |> Repo.update()
  end

  def create_consumer(account_id, attrs) do
    %HttpPushConsumer{account_id: account_id}
    |> HttpPushConsumer.create_changeset(attrs)
    |> Repo.insert()
  end

  def delete_consumer(%HttpPushConsumer{} = consumer) do
    Repo.delete(consumer)
  end

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
    CREATE TABLE #{stream_schema()}.consumer_messages_#{consumer.name} PARTITION OF #{stream_schema()}.consumer_messages FOR VALUES IN ('#{consumer.id}');
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
    DROP TABLE IF EXISTS #{stream_schema()}.consumer_messages_#{consumer.name};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def upsert_consumer_messages(%{}, []), do: {:ok, []}

  def upsert_consumer_messages(%{}, consumer_messages) do
    {consumer_ids, message_keys, message_seqs} =
      consumer_messages
      |> Enum.map(fn message ->
        {message.consumer_id, message.message_key, message.message_seq}
      end)
      |> Enum.reduce({[], [], []}, fn {consumer_id, message_key, message_seq}, {ids, keys, seqs} ->
        {[consumer_id | ids], [message_key | keys], [message_seq | seqs]}
      end)

    Query.upsert_consumer_records(
      consumer_ids: Enum.map(consumer_ids, &UUID.string_to_binary!/1),
      message_keys: message_keys,
      message_seqs: message_seqs
    )
  end

  def receive_for_consumer(%{message_kind: :event} = consumer, opts \\ []) do
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
            |> then(&struct!(ConsumerEvent, &1))
          end)

        {:ok, events}
    end
  end

  @spec ack_messages(Sequin.Consumers.HttpPushConsumer.t(), [integer()]) :: :ok
  def ack_messages(%{message_kind: :event} = consumer, ack_ids) do
    Repo.transact(fn ->
      {_, _} =
        consumer.id
        |> ConsumerEvent.where_consumer_id()
        |> ConsumerEvent.where_ack_ids(ack_ids)
        |> Repo.delete_all()

      :ok
    end)

    :ok
  end

  @spec nack_messages(Sequin.Consumers.HttpPushConsumer.t(), [integer()]) :: :ok
  def nack_messages(%{message_kind: :event} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [not_visible_until: nil])

    :ok
  end

  # ConsumerEvent

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

  def insert_consumer_events(consumer_id, consumer_events) do
    now = DateTime.utc_now()

    entries =
      Enum.map(consumer_events, fn event ->
        Map.merge(event, %{
          consumer_id: consumer_id,
          updated_at: now,
          inserted_at: now
        })
      end)

    {count, _} = Repo.insert_all(ConsumerEvent, entries)
    {:ok, count}
  end

  # HttpPushConsumer Backfills

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
      |> then(&upsert_consumer_messages(consumer, &1))

    {:ok, messages}
  end
end
