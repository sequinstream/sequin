defmodule SequinWeb.PostgresReplicationControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.TestSupport.ReplicationSlots

  setup :authenticated_conn

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
    other_database = DatabasesFactory.insert_configured_postgres_database!(account_id: other_account.id)

    postgres_replication =
      ReplicationFactory.insert_postgres_replication!(
        account_id: account.id,
        postgres_database_id: database.id
      )

    other_postgres_replication =
      ReplicationFactory.insert_postgres_replication!(
        account_id: other_account.id,
        postgres_database_id: other_database.id
      )

    %{
      postgres_replication: postgres_replication,
      other_postgres_replication: other_postgres_replication,
      database: database,
      other_database: other_database,
      other_account: other_account
    }
  end

  describe "index" do
    test "lists postgres replications for the given database", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      conn = get(conn, ~p"/api/databases/#{database.id}/replication_slots")
      assert %{"data" => prs} = json_response(conn, 200)
      assert length(prs) == 1
      [pr] = prs

      assert pr["id"] == postgres_replication.id
    end

    test "returns empty list for database with no replication slots", %{
      conn: conn,
      account: account
    } do
      db_with_no_slots = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
      conn = get(conn, ~p"/api/databases/#{db_with_no_slots.id}/replication_slots")
      assert %{"data" => prs} = json_response(conn, 200)
      assert Enum.empty?(prs)
    end

    test "returns 404 for database in another account", %{
      conn: conn,
      other_database: other_database
    } do
      conn = get(conn, ~p"/api/databases/#{other_database.id}/replication_slots")
      assert json_response(conn, 404)
    end
  end

  describe "show" do
    test "shows postgres replication details when accessed by ID", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      conn = get(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.id}")
      assert %{"postgres_replication" => json_response} = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(postgres_replication, atomized_response, [
        :id,
        :slot_name,
        :publication_name,
        :postgres_database_id
      ])
    end

    test "shows postgres replication details when accessed by slot_name", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      conn = get(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.slot_name}")
      assert %{"postgres_replication" => json_response} = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(postgres_replication, atomized_response, [
        :id,
        :slot_name,
        :publication_name,
        :postgres_database_id
      ])
    end

    test "returns 404 if postgres replication belongs to another database", %{
      conn: conn,
      postgres_replication: postgres_replication,
      other_database: other_database
    } do
      conn = get(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{postgres_replication.id}")
      assert json_response(conn, 404)
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication,
      other_database: other_database
    } do
      conn = get(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{other_postgres_replication.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    setup do
      postgres_replication_attrs =
        [
          slot_name: replication_slot(),
          publication_name: @publication
        ]
        |> ReplicationFactory.postgres_replication_attrs()
        |> Map.put(:backfill_existing_rows, true)

      %{postgres_replication_attrs: postgres_replication_attrs}
    end

    test "creates a postgres replication under the authenticated account", %{
      conn: conn,
      account: account,
      database: database,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      conn = post(conn, ~p"/api/databases/#{database.id}/replication_slots", postgres_replication_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, postgres_replication} = Replication.get_pg_replication_for_account(account.id, id)
      assert postgres_replication.account_id == account.id
      assert postgres_replication.postgres_database_id == database.id
    end

    test "returns validation error for invalid attributes", %{
      conn: conn,
      database: database,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      invalid_attrs = Map.put(postgres_replication_attrs, :slot_name, nil)
      conn = post(conn, ~p"/api/databases/#{database.id}/replication_slots", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "cannot create a postgres replication for a database in another account", %{
      conn: conn,
      other_database: other_database,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      conn = post(conn, ~p"/api/databases/#{other_database.id}/replication_slots", postgres_replication_attrs)
      assert json_response(conn, 404)
    end

    test "creates a postgres replication with backfill_existing_rows set to false", %{
      conn: conn,
      account: account,
      database: database,
      postgres_replication_attrs: postgres_replication_attrs
    } do
      attrs =
        postgres_replication_attrs
        |> Map.put(:backfill_existing_rows, false)
        |> Map.put(:status, :active)

      conn = post(conn, ~p"/api/databases/#{database.id}/replication_slots", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, postgres_replication} = Replication.get_pg_replication_for_account(account.id, id)
      assert postgres_replication.account_id == account.id
      assert postgres_replication.postgres_database_id == database.id
    end
  end

  describe "update" do
    test "updates the postgres replication with valid attributes when accessed by ID", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      update_attrs = %{publication_name: "new_publication"}
      conn = put(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_postgres_replication} = Replication.get_pg_replication(id)
      assert updated_postgres_replication.publication_name == "new_publication"
    end

    test "updates the postgres replication with valid attributes when accessed by slot_name", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      update_attrs = %{publication_name: "updated_via_name"}

      conn =
        put(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.slot_name}", update_attrs)

      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_postgres_replication} = Replication.get_pg_replication(id)
      assert updated_postgres_replication.publication_name == "updated_via_name"
    end

    test "returns validation error for invalid attributes", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      invalid_attrs = %{slot_name: nil}
      conn = put(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if postgres replication belongs to another database", %{
      conn: conn,
      postgres_replication: postgres_replication,
      other_database: other_database
    } do
      conn =
        put(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{postgres_replication.id}", %{
          status: "disabled"
        })

      assert json_response(conn, 404)
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication,
      other_database: other_database
    } do
      conn =
        put(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{other_postgres_replication.id}", %{
          status: "disabled"
        })

      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "deletes the postgres replication when accessed by ID", %{
      conn: conn,
      postgres_replication: postgres_replication,
      database: database
    } do
      conn = delete(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)
      assert id == postgres_replication.id

      assert {:error, _} =
               Replication.get_pg_replication_for_account(postgres_replication.account_id, postgres_replication.id)
    end

    test "deletes the postgres replication when accessed by slot_name", %{
      conn: conn,
      account: account,
      database: database
    } do
      # Create a new replication slot specifically for this test
      postgres_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: database.id,
          slot_name: "test_deletion_by_name"
        )

      conn = delete(conn, ~p"/api/databases/#{database.id}/replication_slots/#{postgres_replication.slot_name}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)
      assert id == postgres_replication.id

      assert {:error, _} =
               Replication.get_pg_replication_for_account(postgres_replication.account_id, postgres_replication.id)
    end

    test "returns 404 if postgres replication belongs to another database", %{
      conn: conn,
      postgres_replication: postgres_replication,
      other_database: other_database
    } do
      conn = delete(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{postgres_replication.id}")
      assert json_response(conn, 404)
    end

    test "returns 404 if database belongs to another account", %{
      conn: conn,
      other_postgres_replication: other_postgres_replication,
      other_database: other_database
    } do
      conn = delete(conn, ~p"/api/databases/#{other_database.id}/replication_slots/#{other_postgres_replication.id}")
      assert json_response(conn, 404)
    end
  end
end
