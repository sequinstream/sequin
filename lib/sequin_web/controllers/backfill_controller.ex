defmodule SequinWeb.BackfillController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Consumers.SchemaFilter
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Runtime.KeysetCursor
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, %{"sink_id_or_name" => sink_id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name) do
      backfills = Consumers.list_backfills_for_sink_consumer(sink_consumer.id)
      render(conn, "index.json", backfills: backfills)
    end
  end

  def show(conn, %{"sink_id_or_name" => sink_id_or_name, "id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         {:ok, backfill} <- Consumers.get_backfill_for_sink_consumer(sink_consumer.id, id) do
      render(conn, "show.json", backfill: backfill)
    end
  end

  def create(conn, %{"sink_id_or_name" => sink_id_or_name} = params) do
    params = Map.delete(params, "sink_id_or_name")
    {table_reference, params} = Map.pop(params, "table")
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         sink_consumer = Repo.preload(sink_consumer, [:postgres_database, :sequence]),
         {:ok, table} <- find_table(sink_consumer, table_reference),
         {:ok, backfill_params} <- Transforms.from_external_backfill(params),
         backfill_params =
           Map.merge(backfill_params, %{
             account_id: account_id,
             sink_consumer_id: sink_consumer.id,
             initial_min_cursor: KeysetCursor.min_cursor(table),
             table_oid: table.oid
           }),
         {:ok, backfill} <- Consumers.create_backfill(backfill_params) do
      render(conn, "show.json", backfill: backfill)
    end
  end

  def update(conn, %{"sink_id_or_name" => sink_id_or_name, "id" => id} = params) do
    params = Map.drop(params, ["sink_id_or_name", "id"])
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         {:ok, backfill} <- Consumers.get_backfill(id),
         true <- backfill.sink_consumer_id == sink_consumer.id,
         {:ok, backfill_params} <- Transforms.from_external_backfill(params),
         {:ok, updated_backfill} <- Consumers.update_backfill(backfill, backfill_params) do
      render(conn, "show.json", backfill: updated_backfill)
    end
  end

  defp find_table(
         %SinkConsumer{postgres_database: %PostgresDatabase{tables: tables}, sequence: %Sequence{table_oid: table_oid}},
         _table_name
       ) do
    case Enum.find(tables, &(&1.oid == table_oid)) do
      nil -> {:error, Error.validation(summary: "Table not found")}
      table -> {:ok, table}
    end
  end

  defp find_table(%SinkConsumer{sequence: nil}, nil) do
    {:error, Error.validation(summary: "Must specify a table to backfill.")}
  end

  defp find_table(
         %SinkConsumer{
           postgres_database: %PostgresDatabase{tables: tables},
           schema_filter: %SchemaFilter{} = schema_filter
         },
         table_reference
       ) do
    {schema, table_name} = Transforms.parse_table_reference(table_reference)
    table = Enum.find(tables, &(&1.name == table_name and &1.schema == schema))

    cond do
      schema_filter.schema != schema ->
        {:error, Error.validation(summary: "Table #{table_reference} not in sink's schema #{schema_filter.schema}")}

      is_nil(table) ->
        {:error, Error.validation(summary: "Table #{table_reference} not found")}

      true ->
        {:ok, table}
    end
  end
end
