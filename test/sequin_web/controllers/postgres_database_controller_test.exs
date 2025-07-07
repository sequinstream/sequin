defmodule SequinWeb.PostgresDatabaseControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Databases
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)

    database_with_primary =
      DatabasesFactory.insert_configured_postgres_database!(
        account_id: account.id,
        primary: DatabasesFactory.postgres_database_primary_attrs(hostname: "primary.example.com")
      )

    slot =
      ReplicationFactory.insert_configured_postgres_replication!(
        postgres_database_id: database.id,
        account_id: account.id
      )

    other_database =
      DatabasesFactory.insert_configured_postgres_database!(account_id: other_account.id)

    %{
      database: database,
      database_with_primary: database_with_primary,
      other_database: other_database,
      other_account: other_account,
      slot: slot
    }
  end

  describe "index" do
    test "lists databases in the given account", %{
      conn: conn,
      database: database,
      database_with_primary: database_with_primary,
      slot: slot
    } do
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

      second_db_json = Enum.find(databases_json, &(&1["id"] == database_with_primary.id))
      assert second_db_json["name"] == database_with_primary.name
      assert second_db_json["password"] == Sequin.String.obfuscate(database_with_primary.password)

      assert second_db_json["primary"] == %{
               "hostname" => database_with_primary.primary.hostname,
               "port" => database_with_primary.primary.port,
               "database" => database_with_primary.primary.database,
               "username" => database_with_primary.primary.username,
               "ssl" => database_with_primary.primary.ssl,
               "ipv6" => database_with_primary.primary.ipv6
             }
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

  describe "create" do
    setup do
      database_attrs = DatabasesFactory.configured_postgres_database_attrs()

      slot_attrs =
        Map.take(ReplicationFactory.configured_postgres_replication_attrs(), [:publication_name, :slot_name, :status])

      %{database_attrs: database_attrs, slot_attrs: slot_attrs}
    end

    test "creates a database and slot under the authenticated account", %{
      conn: conn,
      account: account,
      database_attrs: database_attrs,
      slot_attrs: slot_attrs
    } do
      payload = Map.put(database_attrs, "replication_slots", [slot_attrs])

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert response = json_response(conn, 201)
      assert response["id"]
      assert response["name"] == database_attrs.name
      assert is_list(response["replication_slots"])
      assert length(response["replication_slots"]) == 1
      [slot_response] = response["replication_slots"]
      assert slot_response["slot_name"] == slot_attrs.slot_name

      {:ok, database} = Databases.get_db_for_account(account.id, response["id"])
      assert database.account_id == account.id
      database = Sequin.Repo.preload(database, :replication_slot)
      assert database.replication_slot.slot_name == slot_attrs.slot_name
      assert database.replication_slot.publication_name == slot_attrs.publication_name
    end

    test "returns validation error for invalid database attributes", %{conn: conn, slot_attrs: slot_attrs} do
      payload = %{
        "hostname" => nil,
        "replication_slots" => [slot_attrs]
      }

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert is_map(json_response(conn, 422)["validation_errors"])
    end

    test "returns validation error if replication_slots key is missing", %{conn: conn, database_attrs: database_attrs} do
      conn = post(conn, ~p"/api/postgres_databases", database_attrs)
      assert json_response(conn, 422)["summary"] =~ "`replication_slots`"
    end

    test "returns validation error if replication_slots is not a list", %{
      conn: conn,
      database_attrs: database_attrs,
      slot_attrs: slot_attrs
    } do
      payload = Map.put(database_attrs, "replication_slots", slot_attrs)

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert json_response(conn, 422)["summary"] =~ "`replication_slots` field with exactly one slot is required"
    end

    test "returns validation error if replication_slots list has more than one element", %{
      conn: conn,
      database_attrs: database_attrs,
      slot_attrs: slot_attrs
    } do
      payload = Map.put(database_attrs, "replication_slots", [slot_attrs, slot_attrs])

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert json_response(conn, 422)["summary"] =~ "`replication_slots` field with exactly one slot is required"
    end

    test "returns validation error if replication_slots list is empty", %{conn: conn, database_attrs: database_attrs} do
      payload = Map.put(database_attrs, "replication_slots", [])

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert json_response(conn, 422)["summary"] =~ "`replication_slots` field with exactly one slot is required"
    end
  end

  describe "update" do
    test "updates only the database attributes when replication_slots are not provided", %{conn: conn, database: database} do
      new_hostname = "new-db.example.com"
      payload = %{"hostname" => new_hostname}

      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)
      assert response = json_response(conn, 200)
      assert response["id"] == database.id
      assert response["hostname"] == new_hostname

      {:ok, updated_database} = Databases.get_db(database.id)
      assert updated_database.hostname == new_hostname
    end

    test "updates only the database attributes when replication_slots list is empty", %{
      conn: conn,
      database: database,
      slot: slot
    } do
      new_hostname = "new-db-empty-slots.example.com"

      payload = %{
        "hostname" => new_hostname,
        "replication_slots" => []
      }

      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)
      assert response = json_response(conn, 200)
      assert response["id"] == database.id
      assert response["hostname"] == new_hostname

      {:ok, updated_database} = Databases.get_db(database.id)
      assert updated_database.hostname == new_hostname
      reloaded_slot = Repo.reload(slot)
      assert reloaded_slot.publication_name == slot.publication_name
      assert reloaded_slot.slot_name == slot.slot_name
    end

    test "updates both database and slot attributes", %{conn: conn, database: database, slot: slot} do
      new_hostname = "updated-db-and-slot.example.com"
      new_slot_name = "updated_sequin_slot"
      new_pub_name = "updated_sequin_pub"

      payload = %{
        "hostname" => new_hostname,
        "replication_slots" => [%{"id" => slot.id, "slot_name" => new_slot_name, "publication_name" => new_pub_name}]
      }

      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)
      assert response = json_response(conn, 200)
      assert response["id"] == database.id
      assert response["hostname"] == new_hostname
      assert is_list(response["replication_slots"])
      assert length(response["replication_slots"]) == 1
      [slot_response] = response["replication_slots"]
      assert slot_response["id"] == slot.id
      assert slot_response["slot_name"] == new_slot_name
      assert slot_response["publication_name"] == new_pub_name

      {:ok, updated_database} = Databases.get_db(database.id)
      assert updated_database.hostname == new_hostname
      updated_database = Sequin.Repo.preload(updated_database, :replication_slot)
      assert updated_database.replication_slot.slot_name == new_slot_name
      assert updated_database.replication_slot.publication_name == new_pub_name
    end

    test "returns validation error for invalid database attributes", %{conn: conn, database: database} do
      payload = %{"database" => %{"port" => "invalid"}}
      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)
      assert is_map(json_response(conn, 422)["validation_errors"])
    end

    test "returns validation error if replication_slots list contains more than one slot", %{
      conn: conn,
      database: database,
      slot: slot
    } do
      payload = %{
        "name" => "new-name",
        "replication_slots" => [
          %{"id" => slot.id, "slot_name" => "new_slot"},
          %{"id" => "fake-id", "slot_name" => "another_slot"}
        ]
      }

      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)

      assert json_response(conn, 422)["summary"] =~ "`replication_slots`"
    end

    test "returns validation error if slot in list is missing id", %{conn: conn, database: database} do
      payload = %{
        "name" => "new-name",
        "replication_slots" => [%{"slot_name" => "new_slot"}]
      }

      conn = put(conn, ~p"/api/postgres_databases/#{database.id}", payload)

      assert json_response(conn, 422)["summary"] =~ "must have an `id` field"
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn =
        put(conn, ~p"/api/postgres_databases/#{other_database.id}", %{"database" => %{"hostname" => "new.hostname.com"}})

      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "deletes the database and its replication slot", %{conn: conn, database: database} do
      conn = delete(conn, ~p"/api/postgres_databases/#{database.id}")
      assert %{"success" => true, "id" => id} = json_response(conn, 200)
      assert id == database.id

      assert {:error, _} = Databases.get_db_for_account(database.account_id, id)
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = delete(conn, ~p"/api/postgres_databases/#{other_database.id}")
      assert json_response(conn, 404)
    end

    @tag capture_log: true
    test "returns validation error if database has associated sink consumers", %{conn: conn, database: database} do
      database = Repo.preload(database, :replication_slot)

      ConsumersFactory.insert_sink_consumer!(
        account_id: database.account_id,
        replication_slot_id: database.replication_slot.id
      )

      conn = delete(conn, ~p"/api/postgres_databases/#{database.id}")
      assert response = json_response(conn, 422)
      assert response["error"] =~ "Cannot delete database that's used by sink consumers"
    end
  end

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

  describe "create with connection URL" do
    setup %{account: _} do
      slot_attrs =
        Map.take(
          ReplicationFactory.configured_postgres_replication_attrs(),
          [:publication_name, :slot_name, :status]
        )

      database_attrs = DatabasesFactory.configured_postgres_database_attrs()

      %{slot_attrs: slot_attrs, database_attrs: database_attrs}
    end

    test "creates a database from a valid connection URL", %{
      conn: conn,
      account: account,
      slot_attrs: slot_attrs,
      database_attrs: database_attrs
    } do
      url =
        "postgresql://#{database_attrs.username}:#{database_attrs.password}@#{database_attrs.hostname}:#{database_attrs.port}/#{database_attrs.database}"

      payload = %{
        "url" => url,
        "ssl" => false,
        "name" => "URLTestDB",
        "replication_slots" => [slot_attrs]
      }

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert response = json_response(conn, 201)
      assert response["id"]
      assert response["name"] == "URLTestDB"
      assert response["hostname"] == database_attrs.hostname
      assert response["port"] == database_attrs.port
      assert response["database"] == database_attrs.database
      assert Sequin.String.obfuscate(response["password"]) == response["password"]

      # Verify the database was created properly
      {:ok, database} = Databases.get_db_for_account(account.id, response["id"])
      assert database.hostname == database_attrs.hostname
      assert database.port == database_attrs.port
      assert database.database == database_attrs.database
      assert database.username == database_attrs.username
      assert database.password == database_attrs.password
    end

    test "returns error when URL is missing required components", %{conn: conn, slot_attrs: slot_attrs} do
      # Missing path (database name)
      url = "postgresql://user:pass@localhost:5432"

      payload = %{
        "url" => url,
        "name" => "Bad URL DB",
        "replication_slots" => [slot_attrs]
      }

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert response = json_response(conn, 422)
      assert response["summary"] =~ "Parameters missing from `url`"
      assert response["summary"] =~ "database"
    end

    test "returns error when URL has query parameters", %{conn: conn, slot_attrs: slot_attrs} do
      url = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"

      payload = %{
        "url" => url,
        "name" => "Query Params DB",
        "replication_slots" => [slot_attrs]
      }

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert response = json_response(conn, 422)
      assert response["summary"] =~ "Query parameters not allowed in `url`"
    end

    test "returns error when URL is provided with other connection params", %{
      conn: conn,
      slot_attrs: slot_attrs,
      database_attrs: database_attrs
    } do
      url =
        "postgresql://#{database_attrs.username}:#{database_attrs.password}@#{database_attrs.hostname}:#{database_attrs.port}/#{database_attrs.database}"

      payload = %{
        "url" => url,
        "hostname" => "another-host.example.com",
        "name" => "Conflicting Params DB",
        "replication_slots" => [slot_attrs]
      }

      conn = post(conn, ~p"/api/postgres_databases", payload)
      assert response = json_response(conn, 422)
      assert response["summary"] =~ "If `url` is specified, no other connection params are allowed"
    end
  end
end
