defmodule Sequin.Sources.BackfillPostgresTableWorkerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Sources
  alias Sequin.Sources.BackfillPostgresTableWorker
  alias Sequin.Streams

  describe "create/5" do
    test "creates a job with valid args" do
      postgres_replication = SourcesFactory.insert_postgres_replication!()

      assert {:ok, %Oban.Job{}} =
               BackfillPostgresTableWorker.create(
                 postgres_replication.postgres_database_id,
                 "public",
                 "users",
                 postgres_replication.id
               )

      assert_enqueued(
        worker: BackfillPostgresTableWorker,
        args: %{
          "postgres_database_id" => postgres_replication.postgres_database_id,
          "schema" => "public",
          "table" => "users",
          "offset" => 0,
          "postgres_replication_id" => postgres_replication.id
        }
      )
    end
  end

  describe "perform/1" do
    @table_name "backfill_postgres_worker_test_cities"
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)

      postgres_replication =
        [postgres_database_id: database.id, account_id: account.id, backfill_completed_at: nil, status: :backfilling]
        |> SourcesFactory.insert_postgres_replication!()
        |> Repo.preload(:postgres_database)

      # Create a test table and insert some data
      {:ok, conn} = Databases.start_link(postgres_replication.postgres_database)

      Postgrex.query!(conn, "DROP TABLE IF EXISTS public.#{@table_name}", [])
      Postgrex.query!(conn, "CREATE TABLE IF NOT EXISTS public.#{@table_name} (id SERIAL PRIMARY KEY, name TEXT)", [])
      Postgrex.query!(conn, "INSERT INTO public.#{@table_name} (name) VALUES ($1), ($2)", ["San Francisco", "Honolulu"])

      on_exit(fn ->
        # Need to restart the conn to do this
        {:ok, conn} = Databases.start_link(postgres_replication.postgres_database)
        Postgrex.query!(conn, "DROP TABLE IF EXISTS public.#{@table_name}", [])
      end)

      %{postgres_replication: postgres_replication, conn: conn}
    end

    test "backfills messages and creates next job when rows are returned", %{postgres_replication: postgres_replication} do
      args = %{
        "postgres_database_id" => postgres_replication.postgres_database_id,
        "schema" => "public",
        "table" => @table_name,
        "offset" => 0,
        "postgres_replication_id" => postgres_replication.id
      }

      perform_job(BackfillPostgresTableWorker, args)

      # Verify messages were upserted
      messages = Streams.list_messages_for_stream(postgres_replication.stream_id)
      assert length(messages) == 2
      assert Enum.at(messages, 0).subject == "#{postgres_replication.postgres_database.name}.public.#{@table_name}.1"
      assert Enum.at(messages, 1).subject == "#{postgres_replication.postgres_database.name}.public.#{@table_name}.2"

      # Verify next job was enqueued
      assert_enqueued(
        worker: BackfillPostgresTableWorker,
        args: Map.put(args, "offset", 2)
      )

      # Run it again
      Oban.drain_queue(queue: :default)

      # Verify no next job was enqueued
      refute_enqueued(worker: BackfillPostgresTableWorker)

      # Verify backfill_completed_at was set
      {:ok, updated_postgres_replication} = Sources.get_pg_replication(postgres_replication.id)
      assert updated_postgres_replication.backfill_completed_at
      assert updated_postgres_replication.status == :active
    end
  end
end
