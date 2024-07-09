defmodule SequinWeb.PostgresReplicationControllerTest do
  use SequinWeb.ConnCase

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Sources

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
    other_database = DatabasesFactory.insert_configured_postgres_database!(account_id: other_account.id)
    stream = StreamsFactory.insert_stream!(account_id: account.id)

    postgres_replication =
      SourcesFactory.insert_postgres_replication!(
        account_id: account.id,
        postgres_database_id: database.id,
        stream_id: stream.id
      )

    other_postgres_replication =
      SourcesFactory.insert_postgres_replication!(
        account_id: other_account.id,
        postgres_database_id: other_database.id,
        stream_id: StreamsFactory.insert_stream!(account_id: other_account.id).id
      )

    %{
      postgres_replication: postgres_replication,
      other_postgres_replication: other_postgres_replication,
      database: database,
      other_database: other_database,
      stream: stream,
      other_account: other_account
    }
  end

  describe "index" do
    test "lists postgres replications in the given account", %{
      conn: conn,
      postgres_replication: postgres_replication
    } do
      conn = get(conn, ~p"/api/postgres_replications")
      assert %{"data" => prs} = json_response(conn, 200)
      assert length(prs) == 1
      [pr] = prs

      assert pr["id"] == postgres_replication.id
    end
  end

  describe "show" do
    test "shows postgres replication details", %{conn: conn, postgres_replication: postgres_replication} do
      conn = get(conn, ~p"/api/postgres_replications/#{postgres_replication.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(postgres_replication, atomized_response, [
        :id,
        :slot_name,
        :publication_name,
        :postgres_database_id,
        :stream_id
      ])
    end

    test "returns 404 if postgres replication belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication
    } do
      conn = get(conn, ~p"/api/postgres_replications/#{other_postgres_replication.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    setup %{database: database, stream: stream} do
      postgres_replication_attrs =
        SourcesFactory.postgres_replication_attrs(postgres_database_id: database.id, stream_id: stream.id)

      %{postgres_replication_attrs: postgres_replication_attrs}
    end

    test "creates a postgres replication under the authenticated account", %{
      conn: conn,
      account: account,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      conn = post(conn, ~p"/api/postgres_replications", postgres_replication_attrs)
      assert %{"id" => id} = json_response(conn, 201)

      {:ok, postgres_replication} = Sources.get_pg_replication_for_account(account.id, id)
      assert postgres_replication.account_id == account.id
    end

    test "creates a postgres replication with a new postgres database", %{
      conn: conn,
      account: account,
      stream: stream
    } do
      postgres_replication_attrs = %{
        slot_name: "test_slot",
        publication_name: "test_publication",
        stream_id: stream.id,
        postgres_database: DatabasesFactory.postgres_database_attrs(database: "new_test_db")
      }

      conn = post(conn, ~p"/api/postgres_replications", postgres_replication_attrs)
      assert %{"id" => id} = json_response(conn, 201)

      {:ok, postgres_replication} = Sources.get_pg_replication_for_account(account.id, id)
      postgres_replication = Repo.preload(postgres_replication, :postgres_database)
      assert postgres_replication.account_id == account.id
      assert %PostgresDatabase{} = postgres_replication.postgres_database
      assert postgres_replication.postgres_database.database == "new_test_db"
      assert postgres_replication.postgres_database.account_id == account.id
    end

    test "returns validation error for invalid attributes", %{
      conn: conn,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      invalid_attrs = Map.put(postgres_replication_attrs, :slot_name, nil)
      conn = post(conn, ~p"/api/postgres_replications", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "cannot create a postgres replication for a database in another account", %{
      conn: conn,
      other_database: other_database,
      stream: stream
    } do
      attrs = SourcesFactory.postgres_replication_attrs(postgres_database_id: other_database.id, stream_id: stream.id)
      conn = post(conn, ~p"/api/postgres_replications", attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "update" do
    test "updates the postgres replication with valid attributes", %{
      conn: conn,
      postgres_replication: postgres_replication
    } do
      update_attrs = %{status: "disabled"}
      conn = put(conn, ~p"/api/postgres_replications/#{postgres_replication.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_postgres_replication} = Sources.get_pg_replication(id)
      assert updated_postgres_replication.status == :disabled
    end

    test "returns validation error for invalid attributes", %{conn: conn, postgres_replication: postgres_replication} do
      invalid_attrs = %{slot_name: nil}
      conn = put(conn, ~p"/api/postgres_replications/#{postgres_replication.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if postgres replication belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication
    } do
      conn = put(conn, ~p"/api/postgres_replications/#{other_postgres_replication.id}", %{status: "disabled"})
      assert json_response(conn, 404)
    end

    test "cannot update postgres_database_id", %{
      conn: conn,
      postgres_replication: postgres_replication,
      other_database: other_database
    } do
      update_attrs = %{postgres_database_id: other_database.id}
      conn = put(conn, ~p"/api/postgres_replications/#{postgres_replication.id}", update_attrs)
      assert json_response(conn, 422)

      assert json_response(conn, 422) == %{
               "summary" => "Cannot update stream_id or postgres_database_id",
               "validation_errors" => %{
                 "base" => ["Updating stream_id or postgres_database_id is not allowed"]
               }
             }

      # Verify the postgres_replication was not updated
      {:ok, unchanged_postgres_replication} = Sources.get_pg_replication(postgres_replication.id)
      assert unchanged_postgres_replication.postgres_database_id == postgres_replication.postgres_database_id
    end
  end

  describe "delete" do
    test "deletes the postgres replication", %{conn: conn, postgres_replication: postgres_replication} do
      conn = delete(conn, ~p"/api/postgres_replications/#{postgres_replication.id}")
      assert response(conn, 204)

      assert {:error, _} =
               Sources.get_pg_replication_for_account(postgres_replication.account_id, postgres_replication.id)
    end

    test "returns 404 if postgres replication belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication
    } do
      conn = delete(conn, ~p"/api/postgres_replications/#{other_postgres_replication.id}")
      assert json_response(conn, 404)
    end
  end
end
