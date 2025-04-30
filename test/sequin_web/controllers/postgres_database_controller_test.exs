defmodule SequinWeb.PostgresDatabaseControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)

    slot =
      ReplicationFactory.insert_configured_postgres_replication!(
        postgres_database_id: database.id,
        account_id: account.id
      )

    other_database =
      DatabasesFactory.insert_configured_postgres_database!(account_id: other_account.id)

    %{database: database, other_database: other_database, other_account: other_account, slot: slot}
  end

  describe "index" do
    test "lists databases in the given account", %{
      conn: conn,
      account: account,
      database: database,
      slot: slot
    } do
      another_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      conn = get(conn, ~p"/api/postgres_databases")
      assert %{"data" => databases_json} = json_response(conn, 200)
      assert length(databases_json) == 2

      first_db_json = Enum.find(databases_json, &(&1["id"] == database.id))
      assert first_db_json["name"] == database.name
      assert first_db_json["password"] == Sequin.String.obfuscate(database.password)
      assert is_list(first_db_json["replication_slots"])
      assert first_db_json["replication_slots"] != []

      assert first_db_json["replication_slots"] == [
               %{
                 "publication_name" => slot.publication_name,
                 "slot_name" => slot.slot_name,
                 "status" => to_string(slot.status),
                 "id" => slot.id
               }
             ]

      second_db_json = Enum.find(databases_json, &(&1["id"] == another_database.id))
      assert second_db_json["name"] == another_database.name
      assert second_db_json["password"] == Sequin.String.obfuscate(another_database.password)
    end

    test "lists databases with sensitive data when requested", %{conn: conn, database: database} do
      conn = get(conn, ~p"/api/postgres_databases?show_sensitive=true")
      assert %{"data" => databases_json} = json_response(conn, 200)

      db_json = Enum.find(databases_json, &(&1["id"] == database.id))
      assert db_json["password"] == database.password
      assert db_json["password"] != Sequin.String.obfuscate(database.password)
    end

    test "does not list databases from another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = get(conn, ~p"/api/postgres_databases")
      assert %{"data" => databases} = json_response(conn, 200)
      refute Enum.any?(databases, &(&1["id"] == other_database.id))
    end
  end

  describe "show" do
    test "shows database details", %{conn: conn, database: database, slot: slot} do
      conn = get(conn, ~p"/api/postgres_databases/#{database.id}")
      assert json_response = json_response(conn, 200)
      assert json_response["id"] == database.id
      assert json_response["name"] == database.name
      assert json_response["hostname"] == database.hostname
      assert json_response["password"] == Sequin.String.obfuscate(database.password)
      assert is_list(json_response["replication_slots"])
      assert json_response["replication_slots"] != []

      assert json_response["replication_slots"] == [
               %{
                 "publication_name" => slot.publication_name,
                 "slot_name" => slot.slot_name,
                 "status" => to_string(slot.status),
                 "id" => slot.id
               }
             ]
    end

    test "shows database details with sensitive data", %{conn: conn, database: database} do
      conn = get(conn, ~p"/api/postgres_databases/#{database.id}?show_sensitive=true")
      assert json_response = json_response(conn, 200)
      assert json_response["id"] == database.id
      assert json_response["password"] == database.password
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = get(conn, ~p"/api/postgres_databases/#{other_database.id}")
      assert json_response(conn, 404)
    end
  end

  # describe "create" do
  #   setup do
  #     database_attrs = DatabasesFactory.configured_postgres_database_attrs()
  #     %{database_attrs: database_attrs}
  #   end

  #   test "creates a database under the authenticated account", %{
  #     conn: conn,
  #     account: account,
  #     database_attrs: database_attrs
  #   } do
  #     conn = post(conn, ~p"/api/postgres_databases", database_attrs)
  #     assert %{"id" => id, "name" => name} = json_response(conn, 200)
  #     assert name == database_attrs.name

  #     {:ok, database} = Databases.get_db_for_account(account.id, id)
  #     assert database.account_id == account.id
  #   end

  #   test "returns validation error for invalid attributes", %{conn: conn} do
  #     invalid_attrs = %{hostname: nil}
  #     conn = post(conn, ~p"/api/postgres_databases", invalid_attrs)
  #     assert json_response(conn, 422)["errors"] != %{}
  #   end

  #   test "ignores provided account_id and uses authenticated account", %{
  #     conn: conn,
  #     account: account,
  #     other_account: other_account,
  #     database_attrs: database_attrs
  #   } do
  #     conn =
  #       post(conn, ~p"/api/postgres_databases", Map.put(database_attrs, :account_id, other_account.id))

  #     assert %{"id" => id, "name" => name} = json_response(conn, 200)
  #     assert name == database_attrs.name

  #     {:ok, database} = Databases.get_db_for_account(account.id, id)
  #     assert database.account_id == account.id
  #     assert database.account_id != other_account.id
  #   end

  #   @tag capture_log: true
  #   test "rejects creation if database is not reachable", %{
  #     conn: conn,
  #     database_attrs: database_attrs
  #   } do
  #     unreachable_attrs = Map.merge(database_attrs, %{hostname: "unreachable.host", port: 5432})
  #     conn = post(conn, ~p"/api/postgres_databases", unreachable_attrs)
  #     assert json_response(conn, 200)["id"]
  #   end
  # end

  # describe "update" do
  #   test "updates the database with valid attributes", %{conn: conn, database: database} do
  #     hostname = database.hostname
  #     {:ok, _} = Databases.update_db(database, %{hostname: "some-old-host.com"})
  #     update_attrs = %{hostname: hostname}
  #     conn = put(conn, ~p"/api/postgres_databases/#{database.id}", update_attrs)
  #     assert %{"id" => id, "hostname" => updated_hostname} = json_response(conn, 200)
  #     assert updated_hostname == hostname

  #     {:ok, updated_database} = Databases.get_db(id)
  #     assert updated_database.hostname == hostname
  #   end

  #   test "returns validation error for invalid attributes", %{conn: conn, database: database} do
  #     invalid_attrs = %{port: "invalid"}
  #     conn = put(conn, ~p"/api/postgres_databases/#{database.id}", invalid_attrs)
  #     assert json_response(conn, 422)["errors"] != %{}
  #   end

  #   test "returns 404 if database belongs to another account", %{
  #     conn: conn,
  #     other_database: other_database
  #   } do
  #     conn = put(conn, ~p"/api/postgres_databases/#{other_database.id}", %{hostname: "new.hostname.com"})
  #     assert json_response(conn, 404)
  #   end

  #   @tag capture_log: true
  #   test "rejects update if new configuration is not reachable", %{conn: conn, database: database} do
  #     unreachable_attrs = %{hostname: "unreachable.host"}
  #     conn = put(conn, ~p"/api/postgres_databases/#{database.id}", unreachable_attrs)
  #     assert json_response(conn, 200)["hostname"] == "unreachable.host"
  #   end
  # end

  # describe "delete" do
  #   test "deletes the database", %{conn: conn, database: database} do
  #     conn = delete(conn, ~p"/api/postgres_databases/#{database.id}")
  #     assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

  #     assert {:error, _} = Databases.get_db_for_account(database.account_id, id)
  #   end

  #   test "returns 404 if database belongs to another account", %{
  #     conn: conn,
  #     other_database: other_database
  #   } do
  #     conn = delete(conn, ~p"/api/postgres_databases/#{other_database.id}")
  #     assert json_response(conn, 404)
  #   end
  # end

  describe "test_connection" do
    test "succeeds for a reachable database", %{conn: conn, database: database} do
      conn = post(conn, ~p"/api/postgres_databases/#{database.id}/test_connection")
      assert %{"success" => true} = json_response(conn, 200)
    end

    @tag capture_log: true
    test "fails for an unreachable database", %{conn: conn, account: account} do
      unreachable_database =
        DatabasesFactory.insert_postgres_database!(
          account_id: account.id,
          hostname: "unreachable.test.host",
          port: 1234
        )

      conn = post(conn, ~p"/api/postgres_databases/#{unreachable_database.id}/test_connection")
      assert %{"success" => false, "reason" => reason} = json_response(conn, 422)
      assert String.contains?(reason, "nxdomain")
    end

    test "fails for a missing publication", %{conn: conn, database: database, slot: slot} do
      {:ok, _} = Replication.update_pg_replication(slot, %{publication_name: "missing_publication"})
      conn = post(conn, ~p"/api/postgres_databases/#{database.id}/test_connection")
      assert %{"success" => false, "reason" => reason} = json_response(conn, 422)
      assert String.contains?(reason, "Not found: publication")
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = post(conn, ~p"/api/postgres_databases/#{other_database.id}/test_connection")
      assert json_response(conn, 404)
    end
  end

  describe "refresh_tables" do
    test "succeeds for an existing database", %{conn: conn, database: database} do
      conn = post(conn, ~p"/api/postgres_databases/#{database.id}/refresh_tables")
      assert %{"success" => true} = json_response(conn, 200)
    end

    @tag capture_log: true
    test "fails if database connection fails during refresh", %{conn: conn, account: account} do
      unreachable_database =
        DatabasesFactory.insert_configured_postgres_database!(
          account_id: account.id,
          queue_target: 10,
          queue_interval: 10,
          username: "bad-user"
        )

      conn = post(conn, ~p"/api/postgres_databases/#{unreachable_database.id}/refresh_tables")
      assert %{"success" => false} = json_response(conn, 422)
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = post(conn, ~p"/api/postgres_databases/#{other_database.id}/refresh_tables")
      assert json_response(conn, 404)
    end
  end
end
