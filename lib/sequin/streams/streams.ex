defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Repo
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerState
  alias Sequin.Streams.Message
  alias Sequin.Streams.OutstandingMessage
  alias Sequin.Streams.Query
  alias Sequin.Streams.Stream

  def list, do: Repo.all(Stream)

  def create(attrs) do
    %Stream{}
    |> Stream.changeset(attrs)
    |> Repo.insert()
  end

  def create_with_lifecycle(attrs) do
    Repo.transaction(fn ->
      case create(attrs) do
        {:ok, stream} ->
          create_records_partition(stream)
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  defp create_records_partition(%Stream{} = stream) do
    Repo.query!("""
    CREATE TABLE streams.messages_#{stream.idx} PARTITION OF streams.messages FOR VALUES IN ('#{stream.id}');
    """)
  end

  def delete(%Stream{} = stream) do
    Repo.delete(stream)
  end

  def delete_with_lifecycle(%Stream{} = stream) do
    Repo.transaction(fn ->
      with {:ok, stream} <- delete(stream) do
        drop_records_partition(stream)
        stream
      end
    end)
  end

  defp drop_records_partition(%Stream{} = stream) do
    Repo.query!("""
    DROP TABLE IF EXISTS streams.messages_#{stream.idx};
    """)
  end

  # Consumers

  def all_consumers do
    Repo.all(Consumer)
  end

  def consumer!(consumer_id) do
    consumer_id
    |> Consumer.where_id()
    |> Repo.one!()
  end

  def create_consumer_with_lifecycle(attrs) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_consumer(attrs),
           {:ok, _} <- create_consumer_state(consumer) do
        {:ok, consumer}
      end
    end)
  end

  def create_consumer(attrs) do
    %Consumer{}
    |> Consumer.create_changeset(attrs)
    |> Repo.insert()
  end

  def create_consumer_state(%Consumer{} = consumer) do
    %ConsumerState{}
    |> ConsumerState.create_changeset(%{consumer_id: consumer.id})
    |> Repo.insert()
  end

  def consumer_state(consumer_id) do
    consumer_id
    |> ConsumerState.where_consumer_id()
    |> Repo.one!()
  end

  def next_for_consumer(%Consumer{} = consumer, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()

    # Subquery to select ids of outstanding messages
    subquery =
      consumer.id
      |> OutstandingMessage.where_consumer_id()
      |> OutstandingMessage.where_deliverable()
      |> order_by([om], asc_nulls_first: om.last_delivered_at)
      |> limit(^batch_size)
      |> select([om], om.id)

    # Update the selected outstanding messages
    {_count, outstanding_messages} =
      from(om in OutstandingMessage, where: om.id in subquery(subquery))
      |> select([om], om)
      |> Repo.update_all(
        set: [
          state: :delivered,
          not_visible_until: not_visible_until,
          deliver_count: dynamic([om], om.deliver_count + 1),
          last_delivered_at: now
        ]
      )

    message_key_and_stream_ids = Enum.map(outstanding_messages, fn om -> {om.message_key, om.message_stream_id} end)

    # Retrieve messages and outstanding message info in one query
    messages =
      message_key_and_stream_ids
      |> Message.where_key_and_stream_id_in()
      |> Repo.all()

    # Wrap messages with outstanding message info
    wrapped =
      Enum.map(messages, fn message ->
        om =
          Sequin.Enum.find!(outstanding_messages, fn om ->
            om.message_key == message.key and om.message_stream_id == message.stream_id
          end)

        %{om | message: message}
      end)

    {:ok, wrapped}
  end

  # Messages

  def message!(key, stream_id) do
    key
    |> Message.where_key_and_stream_id(stream_id)
    |> Repo.one!()
  end

  def assign_message_seqs_with_lock(limit \\ 10_000) do
    lock_key = :erlang.phash2("assign_message_seqs_with_lock")

    Repo.transact(fn ->
      case acquire_lock(lock_key) do
        :ok ->
          assign_message_seqs(limit)

          :ok

        {:error, :locked} ->
          {:error, :locked}
      end
    end)
  end

  def assign_message_seqs(limit \\ 10_000) do
    subquery =
      from(m in Message,
        where: is_nil(m.seq),
        order_by: [asc: m.updated_at],
        select: %{key: m.key, stream_id: m.stream_id},
        limit: ^limit
      )

    Repo.update_all(from(m in Message, join: s in subquery(subquery), on: m.key == s.key and m.stream_id == s.stream_id),
      set: [seq: dynamic([_m], fragment("nextval('streams.messages_seq')"))]
    )
  end

  defp acquire_lock(lock_key) do
    case Repo.query("SELECT pg_try_advisory_xact_lock($1)", [lock_key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      _ -> {:error, :locked}
    end
  end

  def upsert_messages(messages, is_retry? \\ false) do
    messages =
      Enum.map(messages, fn message ->
        message
        |> Sequin.Map.from_ecto()
        |> Message.put_data_hash()
        |> Map.put(:updated_at, DateTime.utc_now())
        |> Map.put(:inserted_at, DateTime.utc_now())
        |> Map.put(:seq, nil)
      end)

    on_conflict =
      from(m in Message,
        where: fragment("? IS DISTINCT FROM ?", m.data_hash, fragment("EXCLUDED.data_hash")),
        update: [
          set: [
            data: fragment("EXCLUDED.data"),
            data_hash: fragment("EXCLUDED.data_hash"),
            updated_at: fragment("EXCLUDED.updated_at"),
            seq: nil
          ]
        ]
      )

    {count, nil} =
      Repo.insert_all(
        Message,
        messages,
        on_conflict: on_conflict,
        conflict_target: [:key, :stream_id],
        timeout: :timer.seconds(30)
      )

    {:ok, count}
  rescue
    e in Postgrex.Error ->
      if e.postgres.code == :character_not_in_repertoire and is_retry? == false do
        messages =
          Enum.map(messages, fn %{data: data} = message ->
            Map.put(message, :data, String.replace(data, "\u0000", ""))
          end)

        upsert_messages(messages, true)
      else
        reraise e, __STACKTRACE__
      end
  end

  # Outstanding Messages

  def outstanding_messages_for_consumer(consumer_id) do
    consumer_id
    |> OutstandingMessage.where_consumer_id()
    |> Repo.all()
  end

  def populate_outstanding_messages_with_lock(consumer_id) do
    lock_key = :erlang.phash2(["populate_outstanding_messages_with_lock", consumer_id])

    Repo.transact(fn ->
      case acquire_lock(lock_key) do
        :ok ->
          populate_outstanding_messages(consumer_id)

          :ok

        {:error, :locked} ->
          {:error, :locked}
      end
    end)
  end

  def populate_outstanding_messages(%Consumer{} = consumer) do
    now = NaiveDateTime.utc_now()

    res =
      Query.populate_outstanding_messages(
        consumer_id: UUID.string_to_binary!(consumer.id),
        stream_id: UUID.string_to_binary!(consumer.stream_id),
        now: now,
        max_slots: consumer.max_ack_pending * 5,
        table_schema: "streams"
      )

    case res do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def populate_outstanding_messages(consumer_id) do
    consumer = consumer!(consumer_id)
    populate_outstanding_messages(consumer)
  end
end
