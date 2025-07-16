defmodule SequinWeb.BackfillControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory

  setup :authenticated_conn

  setup %{account: account} do
    column_attrs = DatabasesFactory.column_attrs(type: "text", is_pk?: true)
    table_attrs = DatabasesFactory.table_attrs(columns: [column_attrs])
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id, tables: [table_attrs])
    [table] = database.tables

    sink_consumer =
      ConsumersFactory.insert_sink_consumer!(
        account_id: account.id,
        postgres_database_id: database.id
      )

    backfill =
      ConsumersFactory.insert_backfill!(account_id: account.id, sink_consumer_id: sink_consumer.id, state: :active)

    other_account = AccountsFactory.insert_account!()
    other_sink_consumer = ConsumersFactory.insert_sink_consumer!(account_id: other_account.id)

    other_backfill =
      ConsumersFactory.insert_backfill!(account_id: other_account.id, sink_consumer_id: other_sink_consumer.id)

    %{
      sink_consumer: sink_consumer,
      backfill: backfill,
      other_sink_consumer: other_sink_consumer,
      other_backfill: other_backfill,
      other_account: other_account,
      table: table,
      postgres_database: database
    }
  end

  describe "index" do
    test "lists backfills for the given sink consumer", %{
      conn: conn,
      sink_consumer: sink_consumer,
      backfill: backfill
    } do
      another_backfill =
        ConsumersFactory.insert_backfill!(
          account_id: sink_consumer.account_id,
          sink_consumer_id: sink_consumer.id,
          state: :completed
        )

      sink_identifier = Enum.random([sink_consumer.id, sink_consumer.name])
      conn = get(conn, ~p"/api/sinks/#{sink_identifier}/backfills")
      assert %{"data" => backfills} = json_response(conn, 200)
      assert length(backfills) == 2
      atomized_backfills = Enum.map(backfills, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([backfill, another_backfill], atomized_backfills, &(&1.id == &2.id))
    end

    test "does not list backfills from another account's sink consumer", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      sink_identifier = Enum.random([other_sink_consumer.id, other_sink_consumer.name])
      conn = get(conn, ~p"/api/sinks/#{sink_identifier}/backfills")
      assert json_response(conn, 404)
    end
  end

  describe "show" do
    test "shows backfill details", %{conn: conn, sink_consumer: sink_consumer, backfill: backfill} do
      sink_identifier = Enum.random([sink_consumer.id, sink_consumer.name])
      conn = get(conn, ~p"/api/sinks/#{sink_identifier}/backfills/#{backfill.id}")
      assert backfill_json = json_response(conn, 200)

      assert backfill.id == backfill_json["id"]
      assert to_string(backfill.state) == backfill_json["state"]
    end

    test "returns 404 if backfill belongs to another account's sink consumer", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer,
      other_backfill: other_backfill
    } do
      sink_identifier = Enum.random([other_sink_consumer.id, other_sink_consumer.name])
      conn = get(conn, ~p"/api/sinks/#{sink_identifier}/backfills/#{other_backfill.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a backfill for the given sink consumer", %{
      conn: conn,
      sink_consumer: sink_consumer,
      table: table
    } do
      sink_identifier = Enum.random([sink_consumer.id, sink_consumer.name])
      conn = post(conn, ~p"/api/sinks/#{sink_identifier}/backfills", %{table: "#{table.schema}.#{table.name}"})
      assert backfill_json = json_response(conn, 200)

      assert backfill_json["sink_consumer"] == sink_consumer.name
      assert backfill_json["state"] == "active"
    end

    test "returns error when table is not found", %{
      conn: conn,
      table: table,
      sink_consumer: sink_consumer
    } do
      sink_identifier = Enum.random([sink_consumer.id, sink_consumer.name])
      conn = post(conn, ~p"/api/sinks/#{sink_identifier}/backfills", %{table: "public.#{table.name}"})
      assert json_response(conn, 422)
    end

    test "returns 404 if sink consumer belongs to another account", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      conn = post(conn, ~p"/api/sinks/#{other_sink_consumer.id}/backfills")
      assert json_response(conn, 404)
    end
  end

  describe "create with exactly one included_table_oid" do
    setup %{account: account} do
      column_attrs = DatabasesFactory.column_attrs(type: "text", is_pk?: true)
      table_attrs = DatabasesFactory.table_attrs(columns: [column_attrs])
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id, tables: [table_attrs])
      [table] = database.tables

      sink_consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          postgres_database_id: database.id,
          source: ConsumersFactory.source_attrs(include_table_oids: [table.oid])
        )

      %{
        sink_consumer: sink_consumer,
        table: table
      }
    end

    test "creates a backfill for a sink consumer without specifying a table", %{
      conn: conn,
      sink_consumer: sink_consumer
    } do
      sink_identifier = Enum.random([sink_consumer.id, sink_consumer.name])

      conn = post(conn, ~p"/api/sinks/#{sink_identifier}/backfills")
      assert backfill_json = json_response(conn, 200)
      assert backfill_json["sink_consumer"] == sink_consumer.name
    end
  end

  describe "update" do
    test "updates the backfill with valid attributes", %{
      conn: conn,
      sink_consumer: sink_consumer,
      backfill: backfill
    } do
      update_attrs = %{state: "cancelled"}
      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}/backfills/#{backfill.id}", update_attrs)
      assert updated_backfill_json = json_response(conn, 200)

      assert updated_backfill_json["id"] == backfill.id
      assert updated_backfill_json["state"] == "cancelled"
    end

    test "returns 404 if backfill belongs to another account's sink consumer", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer,
      other_backfill: other_backfill
    } do
      update_attrs = %{state: "cancelled"}
      conn = put(conn, ~p"/api/sinks/#{other_sink_consumer.id}/backfills/#{other_backfill.id}", update_attrs)
      assert json_response(conn, 404)
    end
  end
end
