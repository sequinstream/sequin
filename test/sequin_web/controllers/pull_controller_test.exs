defmodule SequinWeb.PullControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory

  setup :authenticated_conn

  @one_day_ago DateTime.add(DateTime.utc_now(), -24, :hour)

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()

    consumer =
      ConsumersFactory.insert_http_pull_consumer!(
        message_kind: :event,
        account_id: account.id,
        backfill_completed_at: @one_day_ago
      )

    other_consumer = ConsumersFactory.insert_http_pull_consumer!(message_kind: :event, account_id: other_account.id)
    %{consumer: consumer, other_consumer: other_consumer}
  end

  describe "receive" do
    test "returns 404 if trying to pull for another account's consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      conn = get(conn, ~p"/api/http_pull_consumers/#{other_consumer.id}/receive")
      assert json_response(conn, 404)
    end

    test "returns empty list if no messages to return", %{conn: conn, consumer: consumer} do
      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.id}/receive")
      assert %{"data" => []} = json_response(conn, 200)
    end

    test "returns available messages if mix of available and delivered", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)

      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      )

      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.id}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["ack_id"] == event.ack_id
    end

    test "returns an available message by consumer name", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.name}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["ack_id"] == event.ack_id
    end

    test "respects batch_size parameter", %{conn: conn, consumer: consumer} do
      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)
      end

      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.id}/receive", batch_size: 1)
      assert %{"data" => messages} = json_response(conn, 200)
      assert length(messages) == 1
    end
  end

  describe "ack" do
    test "successfully acks a message", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/ack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      # Verify the message can't be pulled again
      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.id}/receive")
      assert %{"data" => []} = json_response(conn, 200)

      # Verify it's gone from consumer_events
      assert Consumers.list_consumer_events_for_consumer(consumer.id) == []
    end

    test "successfully acks a message by consumer name", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.name}/ack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
    end

    test "allows acking a message twice", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/ack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/ack", ack_ids: [event.ack_id])
      assert json_response(conn, 200) == %{"success" => true}
    end

    test "returns 404 when acking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id)

      conn = post(conn, ~p"/api/http_pull_consumers/#{other_consumer.id}/ack", ack_ids: [event.ack_id])
      assert json_response(conn, 404)
    end
  end

  describe "nack" do
    test "successfully nacks a message", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/nack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
      # Verify the message reappears
      conn = get(conn, ~p"/api/http_pull_consumers/#{consumer.id}/receive")
      assert %{"data" => [nacked_message]} = json_response(conn, 200)
      assert nacked_message["ack_id"] == event.ack_id
    end

    test "successfully nacks a message by consumer name", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.name}/nack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
    end

    test "allows nacking a message twice", %{conn: conn, consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/nack", ack_ids: [event.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      conn = post(conn, ~p"/api/http_pull_consumers/#{consumer.id}/nack", ack_ids: [event.ack_id])
      assert json_response(conn, 200) == %{"success" => true}
    end

    test "returns 404 when nacking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id)

      conn = post(conn, ~p"/api/http_pull_consumers/#{other_consumer.id}/nack", ack_ids: [event.ack_id])
      assert json_response(conn, 404)
    end
  end
end
