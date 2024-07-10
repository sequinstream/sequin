defmodule SequinWeb.ConsumerControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    stream = StreamsFactory.insert_stream!(account_id: account.id)
    other_stream = StreamsFactory.insert_stream!(account_id: other_account.id)
    %{stream: stream, other_stream: other_stream, other_account: other_account}
  end

  describe "index" do
    test "lists consumers for the given stream", %{conn: conn, account: account, stream: stream} do
      consumer1 = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)
      consumer2 = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers")
      assert %{"data" => consumers} = json_response(conn, 200)
      assert length(consumers) == 2
      atomized_consumers = Enum.map(consumers, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([consumer1, consumer2], atomized_consumers, &(&1.id == &2.id))
    end

    test "does not list consumers from another account's stream", %{conn: conn, other_stream: other_stream} do
      StreamsFactory.insert_consumer!(account_id: other_stream.account_id, stream_id: other_stream.id)

      conn = get(conn, ~p"/api/streams/#{other_stream.id}/consumers")
      assert json_response(conn, 404)
    end
  end

  describe "show" do
    test "shows consumer details", %{conn: conn, account: account, stream: stream} do
      consumer = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(consumer, atomized_response, [
        :id,
        :ack_wait_ms,
        :max_ack_pending,
        :max_deliver,
        :max_waiting,
        :stream_id
      ])
    end

    test "shows consumer details by slug", %{conn: conn, account: account, stream: stream} do
      consumer = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)

      conn = get(conn, ~p"/api/streams/#{stream.slug}/consumers/#{consumer.slug}")
      assert json_response = json_response(conn, 200)
      assert json_response["id"] == consumer.id
      assert json_response["slug"] == consumer.slug
    end

    test "returns 404 if consumer belongs to another account", %{conn: conn, other_stream: other_stream} do
      consumer = StreamsFactory.insert_consumer!(account_id: other_stream.account_id, stream_id: other_stream.id)

      conn = get(conn, ~p"/api/streams/#{other_stream.id}/consumers/#{consumer.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a consumer under the authenticated account", %{conn: conn, stream: stream} do
      attrs = StreamsFactory.consumer_attrs(stream_id: stream.id, max_ack_pending: 5000)

      conn = post(conn, ~p"/api/streams/#{stream.id}/consumers", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      consumer = Streams.get_consumer!(id)
      assert consumer.account_id == stream.account_id
      assert consumer.stream_id == stream.id
      assert consumer.max_ack_pending == 5000
    end

    test "returns validation error for invalid attributes", %{conn: conn, stream: stream} do
      invalid_attrs = %{max_ack_pending: "invalid"}

      conn = post(conn, ~p"/api/streams/#{stream.id}/consumers", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      stream: stream,
      other_account: other_account
    } do
      attrs = StreamsFactory.consumer_attrs(account_id: other_account.id, max_ack_pending: 5000)

      conn = post(conn, ~p"/api/streams/#{stream.id}/consumers", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      consumer = Streams.get_consumer!(id)
      assert consumer.account_id == stream.account_id
      assert consumer.account_id != other_account.id
    end

    test "returns error when trying to create consumer for stream in another account", %{
      conn: conn,
      other_stream: other_stream
    } do
      attrs = %{max_ack_pending: 5000}

      conn = post(conn, ~p"/api/streams/#{other_stream.id}/consumers", attrs)
      assert json_response(conn, 404)
    end
  end

  describe "update" do
    setup %{account: account, stream: stream} do
      consumer = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)
      %{consumer: consumer}
    end

    test "updates the consumer with valid attributes", %{conn: conn, consumer: consumer, stream: stream} do
      attrs = %{max_ack_pending: 8000}
      conn = put(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      updated_consumer = Streams.get_consumer!(id)
      assert updated_consumer.max_ack_pending == 8000
    end

    test "returns validation error for invalid attributes", %{conn: conn, consumer: consumer, stream: stream} do
      invalid_attrs = %{max_ack_pending: "invalid"}
      conn = put(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if consumer belongs to another account", %{conn: conn, other_stream: other_stream} do
      other_consumer = StreamsFactory.insert_consumer!(account_id: other_stream.account_id, stream_id: other_stream.id)

      conn = put(conn, ~p"/api/streams/#{other_stream.id}/consumers/#{other_consumer.id}", %{max_ack_pending: 8000})
      assert json_response(conn, 404)
    end

    test "ignores account_id if provided", %{conn: conn, consumer: consumer, other_account: other_account, stream: stream} do
      attrs = %{account_id: other_account.id, max_ack_pending: 8000}

      conn = put(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      updated_consumer = Streams.get_consumer!(id)
      assert updated_consumer.account_id == consumer.account_id
      assert updated_consumer.account_id != other_account.id
      assert updated_consumer.max_ack_pending == 8000
    end

    test "ignores attempt to change a stream", %{
      conn: conn,
      consumer: consumer,
      other_stream: other_stream,
      stream: stream
    } do
      attrs = %{stream_id: other_stream.id}

      conn = put(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}", attrs)
      assert json_response(conn, 200)
      assert Repo.reload(consumer).stream_id == consumer.stream_id
    end
  end

  describe "delete" do
    test "deletes the consumer", %{conn: conn, account: account, stream: stream} do
      consumer = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)

      conn = delete(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert_raise Ecto.NoResultsError, fn -> Streams.get_consumer!(id) end
    end

    test "returns 404 if consumer belongs to another account", %{conn: conn, other_stream: other_stream} do
      other_consumer = StreamsFactory.insert_consumer!(account_id: other_stream.account_id, stream_id: other_stream.id)

      conn = delete(conn, ~p"/api/streams/#{other_stream.id}/consumers/#{other_consumer.id}")
      assert json_response(conn, 404)
    end
  end
end
