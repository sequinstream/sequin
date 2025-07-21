defmodule Sequin.Sinks.Postgres.Implementation do
  @moduledoc false
  @behaviour Sequin.Sinks.Postgres

  alias Sequin.Consumers.PostgresSink
  alias Sequin.Sinks.Postgres
  alias Sequin.Sinks.Postgres.ConnectionCache

  @impl Postgres
  def insert_records(%PostgresSink{} = sink, [record]) do
    action = record[:action]
    record = record[:record]
    changes = record[:changes]
    metadata = record[:metadata]

    with {:ok, conn} <- ConnectionCache.connection(sink),
         {:ok, _} <-
           Postgrex.query(
             conn,
             "INSERT INTO #{sink.table_name} (action, record, changes, metadata) VALUES ($1, $2, $3, $4)",
             [action, record, changes, metadata]
           ) do
      :ok
    end
  end

  @impl Postgres
  def upsert_records(_sink, _records) do
    raise "Not implemented"
  end

  @impl Postgres
  def test_connection(_sink) do
    raise "Not implemented"
  end
end
