defmodule SequinWeb.PullControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  setup :authenticated_conn

  @one_day_ago DateTime.add(DateTime.utc_now(), -24, :hour)

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    stream = StreamsFactory.insert_stream!(account_id: account.id)
    other_stream = StreamsFactory.insert_stream!(account_id: other_account.id)

    consumer =
      StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id, backfill_completed_at: @one_day_ago)

    other_consumer = StreamsFactory.insert_consumer!(account_id: other_account.id, stream_id: other_stream.id)
    %{stream: stream, consumer: consumer, other_consumer: other_consumer}
  end

  describe "next" do
    test "returns 404 if trying to pull for another account's consumer", %{conn: conn, other_consumer: other_consumer} do
      conn = get(conn, ~p"/api/consumers/#{other_consumer.id}/next")
      assert json_response(conn, 404)
    end

    test "returns empty list if no ConsumerMessages to return", %{conn: conn, consumer: consumer} do
      conn = get(conn, ~p"/api/consumers/#{consumer.id}/next")
      assert %{"data" => []} = json_response(conn, 200)
    end

    test "returns available messages if mix of available and delivered", %{conn: conn, consumer: consumer, stream: stream} do
      available_message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      delivered_message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      cm =
        StreamsFactory.insert_consumer_message!(%{
          consumer_id: consumer.id,
          message: available_message,
          state: :available
        })

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: delivered_message,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      })

      conn = get(conn, ~p"/api/consumers/#{consumer.id}/next")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["ack_token"] == cm.ack_id
      assert message["message"]["subject"] == available_message.subject
    end

    test "respects batch_size parameter", %{conn: conn, consumer: consumer, stream: stream} do
      for _ <- 1..3 do
        message = StreamsFactory.insert_message!(%{stream_id: stream.id})
        StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message, state: :available})
      end

      conn = get(conn, ~p"/api/consumers/#{consumer.id}/next", batch_size: 1)
      assert %{"data" => messages} = json_response(conn, 200)
      assert length(messages) == 1
    end
  end

  describe "ack" do
    test "successfully acks a message", %{conn: conn, consumer: consumer, stream: stream} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      cm = StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message, state: :delivered})

      res_conn = post(conn, ~p"/api/consumers/#{consumer.id}/ack", ack_tokens: [cm.ack_id])
      assert response(res_conn, 204)

      # Verify the message can't be pulled again
      conn = get(conn, ~p"/api/consumers/#{consumer.id}/next")
      assert %{"data" => []} = json_response(conn, 200)

      # Verify it's gone from consumer_messages
      assert Streams.all_consumer_messages() == []
    end

    test "allows acking a message twice", %{conn: conn, consumer: consumer, stream: stream} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      cm = StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message, state: :delivered})

      res_conn = post(conn, ~p"/api/consumers/#{consumer.id}/ack", ack_tokens: [cm.ack_id])
      assert response(res_conn, 204)

      conn = post(conn, ~p"/api/consumers/#{consumer.id}/ack", ack_tokens: [cm.ack_id])
      assert response(conn, 204)
    end

    test "returns 404 when acking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer,
      stream: stream
    } do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      cm =
        StreamsFactory.insert_consumer_message!(%{consumer_id: other_consumer.id, message: message, state: :delivered})

      conn = post(conn, ~p"/api/consumers/#{other_consumer.id}/ack", ack_tokens: [cm.ack_id])
      assert json_response(conn, 404)
    end
  end

  describe "nack" do
    test "successfully nacks a message", %{conn: conn, consumer: consumer, stream: stream} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      cm = StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message, state: :delivered})

      res_conn = post(conn, ~p"/api/consumers/#{consumer.id}/nack", ack_tokens: [cm.ack_id])
      assert response(res_conn, 204)

      # Verify it's still in consumer_messages
      assert Streams.get_consumer_message!(consumer.id, cm.message_subject).state == :available

      # Verify the message reappears
      conn = get(conn, ~p"/api/consumers/#{consumer.id}/next")
      assert %{"data" => [nacked_message]} = json_response(conn, 200)
      assert nacked_message["message"]["subject"] == message.subject
    end

    test "allows nacking a message twice", %{conn: conn, consumer: consumer, stream: stream} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      cm = StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message, state: :delivered})

      res_conn = post(conn, ~p"/api/consumers/#{consumer.id}/nack", ack_tokens: [cm.ack_id])
      assert response(res_conn, 204)

      conn = post(conn, ~p"/api/consumers/#{consumer.id}/nack", ack_tokens: [cm.ack_id])
      assert response(conn, 204)
    end

    test "returns 404 when nacking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer,
      stream: stream
    } do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      cm =
        StreamsFactory.insert_consumer_message!(%{consumer_id: other_consumer.id, message: message, state: :delivered})

      conn = post(conn, ~p"/api/consumers/#{other_consumer.id}/nack", ack_tokens: [cm.ack_id])
      assert json_response(conn, 404)
    end
  end
end
