defmodule SequinWeb.MessageControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.StreamsFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    stream = StreamsFactory.insert_stream!(account_id: account.id)
    other_stream = StreamsFactory.insert_stream!(account_id: other_account.id)

    consumer = StreamsFactory.insert_consumer!(account_id: account.id, stream_id: stream.id)

    other_consumer = StreamsFactory.insert_consumer!(account_id: other_account.id, stream_id: other_stream.id)
    %{stream: stream, other_stream: other_stream, consumer: consumer, other_consumer: other_consumer}
  end

  describe "publish" do
    test "publishes messages to a stream", %{conn: conn, stream: stream} do
      messages = [
        %{"subject" => "test.subject.1", "data" => "test data 1"},
        %{"subject" => "test.subject.2", "data" => "test data 2"}
      ]

      conn = post(conn, ~p"/api/streams/#{stream.id}/messages", %{messages: messages})
      assert %{"data" => %{"published" => 2}} = json_response(conn, 200)
      # Check the database to ensure the messages were published
      published_messages = Sequin.Streams.list_messages_for_stream(stream.id, order_by: [desc: :seq])
      assert length(published_messages) == 2

      [message1, message2] = published_messages
      assert message1.subject == "test.subject.1"
      assert message1.data == "test data 1"
      assert message2.subject == "test.subject.2"
      assert message2.data == "test data 2"
    end

    test "rejects invalid message format", %{conn: conn, stream: stream} do
      invalid_messages = [%{"invalid" => "format"}]

      conn = post(conn, ~p"/api/streams/#{stream.id}/messages", %{messages: invalid_messages})
      assert json_response(conn, 400)
    end

    test "cannot publish to a stream from another account", %{conn: conn, other_stream: other_stream} do
      messages = [%{"subject" => "test.subject", "data" => "test data"}]

      conn = post(conn, ~p"/api/streams/#{other_stream.id}/messages", %{messages: messages})
      assert json_response(conn, 404)
    end
  end

  describe "stream_list" do
    test "lists messages for a stream", %{conn: conn, stream: stream} do
      for _ <- 1..5, do: StreamsFactory.insert_message!(stream_id: stream.id)

      conn = get(conn, ~p"/api/streams/#{stream.id}/messages")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 5
    end

    test "cannot list messages from a stream in another account", %{conn: conn, other_stream: other_stream} do
      conn = get(conn, ~p"/api/streams/#{other_stream.id}/messages")
      assert json_response(conn, 404)
    end

    test "respects limit parameter", %{conn: conn, stream: stream} do
      for _ <- 1..10, do: StreamsFactory.insert_message!(stream_id: stream.id)

      conn = get(conn, ~p"/api/streams/#{stream.id}/messages?limit=3")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 3
    end

    test "respects sort parameter", %{conn: conn, stream: stream} do
      for _ <- 1..3, do: StreamsFactory.insert_message!(stream_id: stream.id)

      resp_conn = get(conn, ~p"/api/streams/#{stream.id}/messages?sort=seq_desc")
      assert %{"data" => listed_messages} = json_response(resp_conn, 200)
      seqs = Enum.map(listed_messages, & &1["seq"])
      assert seqs |> Enum.sort() |> Enum.reverse() == seqs

      conn = get(conn, ~p"/api/streams/#{stream.id}/messages?sort=seq_asc")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      seqs = Enum.map(listed_messages, & &1["seq"])
      assert Enum.sort(seqs) == seqs
    end

    test "filters by subject pattern", %{conn: conn, stream: stream} do
      StreamsFactory.insert_message!(stream_id: stream.id, subject: "orders.new")
      StreamsFactory.insert_message!(stream_id: stream.id, subject: "orders.update")
      StreamsFactory.insert_message!(stream_id: stream.id, subject: "users.new")

      conn = get(conn, ~p"/api/streams/#{stream.id}/messages?subject_pattern=orders.*")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 2
      assert Enum.all?(listed_messages, &String.starts_with?(&1["subject"], "orders."))

      conn = get(conn, ~p"/api/streams/#{stream.id}/messages?subject_pattern=*.new")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 2
      assert Enum.all?(listed_messages, &String.ends_with?(&1["subject"], ".new"))
    end
  end

  describe "consumer_list" do
    test "lists messages for a consumer", %{conn: conn, stream: stream, consumer: consumer} do
      messages = for _ <- 1..5, do: StreamsFactory.insert_message!(stream_id: consumer.stream_id)

      for message <- messages do
        StreamsFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message: message,
          state: :available
        )
      end

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 5
      assert Enum.all?(listed_messages, &(&1["message"] && &1["info"]))
    end

    test "cannot list messages from a consumer in another account", %{
      conn: conn,
      other_stream: other_stream,
      other_consumer: other_consumer
    } do
      conn = get(conn, ~p"/api/streams/#{other_stream.id}/consumers/#{other_consumer.id}/messages")
      assert json_response(conn, 404)
    end

    test "filters by visibility", %{conn: conn, stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(stream_id: consumer.stream_id)
      message2 = StreamsFactory.insert_message!(stream_id: consumer.stream_id)

      StreamsFactory.insert_consumer_message!(
        consumer_id: consumer.id,
        message: message1,
        state: :available
      )

      StreamsFactory.insert_consumer_message!(
        consumer_id: consumer.id,
        message: message2,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 60, :second)
      )

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?visible=true")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 1
      assert List.first(listed_messages)["info"]["state"] == "available"

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?visible=false")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 1
      assert List.first(listed_messages)["info"]["state"] == "delivered"
    end

    test "respects limit parameter", %{conn: conn, stream: stream, consumer: consumer} do
      for _ <- 1..10 do
        message = StreamsFactory.insert_message!(stream_id: consumer.stream_id)
        StreamsFactory.insert_consumer_message!(consumer_id: consumer.id, message: message)
      end

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?limit=3")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 3
    end

    test "respects sort parameter", %{conn: conn, stream: stream, consumer: consumer} do
      for _ <- 1..3, do: StreamsFactory.insert_message!(stream_id: consumer.stream_id)

      resp_conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?sort=seq_desc")
      assert %{"data" => listed_messages} = json_response(resp_conn, 200)
      seqs = Enum.map(listed_messages, & &1["message"]["seq"])
      assert seqs |> Enum.sort() |> Enum.reverse() == seqs

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?sort=seq_asc")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      seqs = Enum.map(listed_messages, & &1["message"]["seq"])
      assert Enum.sort(seqs) == seqs
    end

    test "filters by subject pattern", %{conn: conn, stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(stream_id: stream.id, subject: "orders.new")
      message2 = StreamsFactory.insert_message!(stream_id: stream.id, subject: "orders.update")
      message3 = StreamsFactory.insert_message!(stream_id: stream.id, subject: "users.new")

      StreamsFactory.insert_consumer_message!(consumer_id: consumer.id, message: message1, state: :available)
      StreamsFactory.insert_consumer_message!(consumer_id: consumer.id, message: message2, state: :available)
      StreamsFactory.insert_consumer_message!(consumer_id: consumer.id, message: message3, state: :available)

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?subject_pattern=orders.*")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 2
      assert Enum.all?(listed_messages, &String.starts_with?(&1["message"]["subject"], "orders."))

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages?subject_pattern=*.new")
      assert %{"data" => listed_messages} = json_response(conn, 200)
      assert length(listed_messages) == 2
      assert Enum.all?(listed_messages, &String.ends_with?(&1["message"]["subject"], ".new"))
    end

    test "correctly reports state for messages with expired visibility", %{conn: conn, stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(stream_id: stream.id)
      past_time = DateTime.add(DateTime.utc_now(), -60, :second)

      StreamsFactory.insert_consumer_message!(
        consumer_id: consumer.id,
        message: message,
        state: :delivered,
        not_visible_until: past_time
      )

      conn = get(conn, ~p"/api/streams/#{stream.id}/consumers/#{consumer.id}/messages")
      assert %{"data" => [listed_message]} = json_response(conn, 200)

      assert listed_message["info"]["state"] == "available"
      assert listed_message["info"]["not_visible_until"] == nil
    end
  end
end
