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

  # Messages

  def get!(key, stream_id) do
    key
    |> Message.where_key_and_stream(stream_id)
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
