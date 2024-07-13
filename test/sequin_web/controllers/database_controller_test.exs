defmodule SequinWeb.DatabaseControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Databases
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Test.Support.ReplicationSlots

  setup :authenticated_conn

  @publication_name "__database_controller_test_pub__"
  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
    other_database = DatabasesFactory.insert_configured_postgres_database!(account_id: other_account.id)

    on_exit(fn ->
      Repo.query("DROP PUBLICATION IF EXISTS #{@publication_name}")
    end)

    %{database: database, other_database: other_database, other_account: other_account}
  end

  describe "index" do
    test "lists databases in the given account", %{conn: conn, account: account, database: database} do
      another_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      conn = get(conn, ~p"/api/databases")
      assert %{"data" => databases} = json_response(conn, 200)
      assert length(databases) == 2
      atomized_databases = Enum.map(databases, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([database, another_database], atomized_databases, &(&1.id == &2.id))
    end

    test "does not list databases from another account", %{conn: conn, other_database: other_database} do
      conn = get(conn, ~p"/api/databases")
      assert %{"data" => databases} = json_response(conn, 200)
      refute Enum.any?(databases, &(&1["id"] == other_database.id))
    end
  end

  describe "show" do
    test "shows database details", %{conn: conn, database: database} do
      conn = get(conn, ~p"/api/databases/#{database.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(database, atomized_response, [:id, :hostname, :port, :database, :username])
    end

    test "returns 404 if database belongs to another account", %{conn: conn, other_database: other_database} do
      conn = get(conn, ~p"/api/databases/#{other_database.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    setup do
      database_attrs = DatabasesFactory.configured_postgres_database_attrs()
      %{database_attrs: database_attrs}
    end

    test "creates a database under the authenticated account", %{
      conn: conn,
      account: account,
      database_attrs: database_attrs
    } do
      conn = post(conn, ~p"/api/databases", database_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, database} = Databases.get_db_for_account(account.id, id)
      assert database.account_id == account.id
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{hostname: nil}
      conn = post(conn, ~p"/api/databases", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account,
      database_attrs: database_attrs
    } do
      conn = post(conn, ~p"/api/databases", Map.put(database_attrs, :account_id, other_account.id))
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, database} = Databases.get_db_for_account(account.id, id)
      assert database.account_id == account.id
      assert database.account_id != other_account.id
    end

    @tag capture_log: true
    test "rejects creation if database is not reachable", %{conn: conn, database_attrs: database_attrs} do
      unreachable_attrs = Map.merge(database_attrs, %{hostname: "unreachable.host", port: 5432})
      conn = post(conn, ~p"/api/databases", unreachable_attrs)
      assert json_response(conn, 422)
    end
  end

  describe "update" do
    test "updates the database with valid attributes", %{conn: conn, database: database} do
      hostname = database.hostname
      {:ok, _} = Databases.update_db(database, %{hostname: "some-old-host.com"})
      update_attrs = %{hostname: hostname}
      conn = put(conn, ~p"/api/databases/#{database.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_database} = Databases.get_db(id)
      assert updated_database.hostname == "localhost"
    end

    test "returns validation error for invalid attributes", %{conn: conn, database: database} do
      invalid_attrs = %{port: "invalid"}
      conn = put(conn, ~p"/api/databases/#{database.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if database belongs to another account", %{conn: conn, other_database: other_database} do
      conn = put(conn, ~p"/api/databases/#{other_database.id}", %{hostname: "new.hostname.com"})
      assert json_response(conn, 404)
    end

    @tag capture_log: true
    test "rejects update if new configuration is not reachable", %{conn: conn, database: database} do
      unreachable_attrs = %{hostname: "unreachable.host"}
      conn = put(conn, ~p"/api/databases/#{database.id}", unreachable_attrs)
      assert json_response(conn, 422)
    end
  end

  describe "delete" do
    test "deletes the database", %{conn: conn, database: database} do
      conn = delete(conn, ~p"/api/databases/#{database.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Databases.get_db_for_account(database.account_id, id)
    end

    test "returns 404 if database belongs to another account", %{conn: conn, other_database: other_database} do
      conn = delete(conn, ~p"/api/databases/#{other_database.id}")
      assert json_response(conn, 404)
    end
  end

  describe "test_connection" do
    test "succeeds for a reachable database", %{conn: conn, database: database} do
      conn = post(conn, ~p"/api/databases/#{database.id}/test_connection")
      assert %{"success" => true} = json_response(conn, 200)
    end

    @tag capture_log: true
    test "fails for an unreachable database", %{conn: conn, account: account} do
      unreachable_database =
        DatabasesFactory.insert_postgres_database!(
          account_id: account.id,
          hostname: "unreachable.host",
          port: 5432
        )

      conn = post(conn, ~p"/api/databases/#{unreachable_database.id}/test_connection")
      assert %{"success" => false} = json_response(conn, 422)
    end

    test "returns 404 if database belongs to another account", %{conn: conn, other_database: other_database} do
      conn = post(conn, ~p"/api/databases/#{other_database.id}/test_connection")
      assert json_response(conn, 404)
    end
  end

  describe "test_connection_params" do
    test "succeeds for reachable database parameters", %{conn: conn} do
      params = DatabasesFactory.configured_postgres_database_attrs()
      conn = post(conn, ~p"/api/databases/test_connection", params)
      assert %{"success" => true} = json_response(conn, 200)
    end

    @tag capture_log: true
    test "fails for unreachable database parameters", %{conn: conn} do
      params = DatabasesFactory.configured_postgres_database_attrs()
      unreachable_params = Map.merge(params, %{hostname: "unreachable.host", port: 5432})
      conn = post(conn, ~p"/api/databases/test_connection", unreachable_params)
      assert %{"success" => false} = json_response(conn, 422)
    end
  end

  describe "setup_replication" do
    test "sets up replication slot and publication for a database", %{conn: conn, database: database} do
      # We can't actually create the rep slot in this test, due to issues with creating
      # replication slots and the ecto sandbox - see test/support/replication_slots.ex

      conn =
        post(conn, ~p"/api/databases/#{database.id}/setup_replication", %{
          slot_name: replication_slot(),
          publication_name: @publication_name
        })

      assert %{"success" => true, "slot_name" => _, "publication_name" => @publication_name} =
               json_response(conn, 200)

      assert {:ok, %{num_rows: 1}} =
               Repo.query("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1", [@publication_name])
    end

    test "returns error for a database belonging to another account", %{conn: conn, other_database: other_database} do
      conn =
        post(conn, ~p"/api/databases/#{other_database.id}/setup_replication", %{
          slot_name: "test_slot",
          publication_name: "test_pub"
        })

      assert json_response(conn, 404)
    end

    test "returns error for invalid slot or publication name", %{conn: conn, database: database} do
      conn =
        post(conn, ~p"/api/databases/#{database.id}/setup_replication", %{
          slot_name: "",
          publication_name: ""
        })

      assert %{
               "summary" => "slot_name and publication_name are required"
             } = json_response(conn, 422)
    end
  end
end
