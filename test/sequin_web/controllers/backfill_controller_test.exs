defmodule SequinWeb.BackfillControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.Backfill
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()

    consumer = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
    other_consumer = ConsumersFactory.insert_sink_consumer!(account_id: other_account.id)

    backfill = create_backfill!(consumer.id, account.id)
    other_backfill = create_backfill!(other_consumer.id, other_account.id)

    %{
      consumer: consumer,
      other_consumer: other_consumer,
      backfill: backfill,
      other_backfill: other_backfill,
      other_account: other_account
    }
  end

  describe "index" do
    test "lists backfills for the given consumer", %{
      conn: conn,
      consumer: consumer,
      backfill: backfill
    } do
      conn = get(conn, ~p"/api/consumers/#{consumer.id}/backfills")
      assert %{"data" => backfills} = json_response(conn, 200)
      assert length(backfills) == 1
      [bf] = backfills

      assert bf["id"] == backfill.id
    end

    test "returns empty list for consumer with no backfills", %{
      conn: conn,
      account: account
    } do
      consumer_with_no_backfills = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
      conn = get(conn, ~p"/api/consumers/#{consumer_with_no_backfills.id}/backfills")
      assert %{"data" => backfills} = json_response(conn, 200)
      assert Enum.empty?(backfills)
    end

    test "returns 404 for consumer in another account", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      conn = get(conn, ~p"/api/consumers/#{other_consumer.id}/backfills")
      assert json_response(conn, 404)
    end
  end

  describe "show" do
    test "shows backfill details", %{
      conn: conn,
      consumer: consumer,
      backfill: backfill
    } do
      conn = get(conn, ~p"/api/consumers/#{consumer.id}/backfills/#{backfill.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(backfill, atomized_response, [
        :id,
        :account_id,
        :sink_consumer_id,
        :state,
        :rows_initial_count,
        :sort_column_attnum
      ])
    end

    test "returns 404 if backfill belongs to another consumer", %{
      conn: conn,
      backfill: backfill,
      account: account
    } do
      other_consumer_same_account = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
      conn = get(conn, ~p"/api/consumers/#{other_consumer_same_account.id}/backfills/#{backfill.id}")
      assert json_response(conn, 404)
    end

    test "returns 404 if consumer belongs to another account", %{
      conn: conn,
      other_backfill: other_backfill,
      other_consumer: other_consumer
    } do
      conn = get(conn, ~p"/api/consumers/#{other_consumer.id}/backfills/#{other_backfill.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a backfill for the given consumer", %{
      conn: conn,
      consumer: consumer
    } do
      backfill_attrs = %{
        "initial_min_cursor" => %{"1" => 0},
        "sort_column_attnum" => 1
      }

      conn = post(conn, ~p"/api/consumers/#{consumer.id}/backfills", backfill_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, backfill} = Consumers.get_backfill(id)
      assert backfill.sink_consumer_id == consumer.id
      assert backfill.account_id == consumer.account_id
      assert backfill.state == :active
    end

    test "returns 404 if consumer belongs to another account", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      backfill_attrs = %{
        "initial_min_cursor" => %{"1" => 0},
        "sort_column_attnum" => 1
      }

      conn = post(conn, ~p"/api/consumers/#{other_consumer.id}/backfills", backfill_attrs)
      assert json_response(conn, 404)
    end
  end

  describe "update" do
    test "updates the backfill with valid attributes", %{
      conn: conn,
      consumer: consumer,
      backfill: backfill
    } do
      update_attrs = %{
        "rows_processed_count" => 100,
        "rows_ingested_count" => 50
      }

      conn = put(conn, ~p"/api/consumers/#{consumer.id}/backfills/#{backfill.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_backfill} = Consumers.get_backfill(id)
      assert updated_backfill.rows_processed_count == 100
      assert updated_backfill.rows_ingested_count == 50
    end

    test "returns 404 if backfill belongs to another consumer", %{
      conn: conn,
      backfill: backfill,
      account: account
    } do
      other_consumer_same_account = ConsumersFactory.insert_sink_consumer!(account_id: account.id)

      conn =
        put(conn, ~p"/api/consumers/#{other_consumer_same_account.id}/backfills/#{backfill.id}", %{
          "rows_processed_count" => 100
        })

      assert json_response(conn, 404)
    end

    test "returns 404 if consumer belongs to another account", %{
      conn: conn,
      other_backfill: other_backfill,
      other_consumer: other_consumer
    } do
      conn =
        put(conn, ~p"/api/consumers/#{other_consumer.id}/backfills/#{other_backfill.id}", %{
          "rows_processed_count" => 100
        })

      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "cancels the backfill", %{
      conn: conn,
      consumer: consumer,
      backfill: backfill
    } do
      conn = delete(conn, ~p"/api/consumers/#{consumer.id}/backfills/#{backfill.id}")
      assert %{"id" => id, "cancelled" => true} = json_response(conn, 200)
      assert id == backfill.id

      {:ok, updated_backfill} = Consumers.get_backfill(id)
      assert updated_backfill.state == :cancelled
    end

    test "returns 404 if backfill belongs to another consumer", %{
      conn: conn,
      backfill: backfill,
      account: account
    } do
      other_consumer_same_account = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
      conn = delete(conn, ~p"/api/consumers/#{other_consumer_same_account.id}/backfills/#{backfill.id}")
      assert json_response(conn, 404)
    end

    test "returns 404 if consumer belongs to another account", %{
      conn: conn,
      other_backfill: other_backfill,
      other_consumer: other_consumer
    } do
      conn = delete(conn, ~p"/api/consumers/#{other_consumer.id}/backfills/#{other_backfill.id}")
      assert json_response(conn, 404)
    end
  end

  defp create_backfill!(consumer_id, account_id) do
    {:ok, backfill} =
      Consumers.create_backfill(%{
        sink_consumer_id: consumer_id,
        account_id: account_id,
        state: :active,
        initial_min_cursor: %{"1" => 0},
        sort_column_attnum: 1
      })

    backfill
  end
end
