defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Repo
  alias Sequin.Streams.Message
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
end
