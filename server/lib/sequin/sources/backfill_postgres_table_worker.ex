defmodule Sequin.Sources.BackfillPostgresTableWorker do
  @moduledoc false
  use Oban.Worker

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Sources.PostgresReplication
  alias Sequin.SourcesRuntime
  alias Sequin.Streams

  require Logger

  @limit 1000

  def create(postgres_database_id, schema, table, postgres_replication_id, offset \\ 0) do
    %{
      postgres_database_id: postgres_database_id,
      schema: schema,
      table: table,
      offset: offset,
      postgres_replication_id: postgres_replication_id
    }
    |> new()
    |> Oban.insert()
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    %{
      "postgres_database_id" => postgres_database_id,
      "schema" => schema,
      "table" => table,
      "offset" => offset,
      "postgres_replication_id" => postgres_replication_id
    } = args

    Logger.metadata(
      postgres_database_id: postgres_database_id,
      schema: schema,
      table: table,
      offset: offset,
      postgres_replication_id: postgres_replication_id
    )

    postgres_database = Sequin.Repo.get!(PostgresDatabase, postgres_database_id)
    postgres_replication = Sequin.Repo.get!(PostgresReplication, postgres_replication_id)

    {:ok, conn} = ConnectionCache.connection(postgres_database)

    query = """
    SELECT *
    FROM #{schema}.#{table}
    ORDER BY ctid
    LIMIT #{@limit} OFFSET $1
    """

    {:ok, %{rows: rows, columns: columns}} = Postgrex.query(conn, query, [offset])

    if rows == [] do
      # This is not perfect - other backfills may very well be running (in the case of multiple
      # publications). We'll handle that later.
      postgres_replication =
        postgres_replication
        |> Ecto.Changeset.change(
          backfill_completed_at: DateTime.utc_now(),
          status: :active
        )
        |> Sequin.Repo.update()

      SourcesRuntime.Supervisor.start_for_pg_replication(postgres_replication)

      Logger.info("Backfill completed for postgres_replication_id: #{postgres_replication_id}")
    else
      messages = create_messages(postgres_database.name, schema, table, columns, rows)
      Streams.upsert_messages(postgres_replication.stream_id, messages)

      next_offset = offset + length(rows)
      create(postgres_database_id, schema, table, postgres_replication_id, next_offset)

      Logger.info("Processed #{length(rows)} rows for postgres_replication_id: #{postgres_replication_id}")
    end

    :ok
  end

  defp create_messages(db_name, schema, table, columns, rows) do
    Enum.map(rows, fn row ->
      record = columns |> Enum.zip(row) |> Map.new()
      subject = subject_from_message(db_name, schema, table, record["id"])

      %{
        subject: subject,
        data:
          Jason.encode!(%{
            data: record,
            deleted: false
          })
      }
    end)
  end

  defp subject_from_message(db_name, schema, table, record_id) do
    Enum.join(
      [
        db_name,
        Sequin.Subject.to_subject_token(schema),
        Sequin.Subject.to_subject_token(table),
        "insert",
        record_id
      ],
      "."
    )
  end
end
