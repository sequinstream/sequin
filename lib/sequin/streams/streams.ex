defmodule Sequin.Streams do
  @moduledoc false
  alias Sequin.Repo
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
end
