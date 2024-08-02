defmodule SequinWeb.WebhookIngestionControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.SourcesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  setup :authenticated_conn

  describe "ingest" do
    setup %{account: account} do
      stream = StreamsFactory.insert_stream!(account_id: account.id)
      webhook = SourcesFactory.insert_webhook!(account_id: account.id, stream_id: stream.id)
      %{stream: stream, webhook: webhook}
    end

    test "successfully ingests a webhook payload", %{conn: conn, webhook: webhook, stream: stream} do
      payload = %{
        "id" => 1,
        "name" => "Paul Atreides",
        "house" => "Atreides",
        "planet" => "Arrakis"
      }

      conn = post(conn, ~p"/api/webhook/#{webhook.name}", payload)
      assert json_response(conn, 200) == %{"success" => true}

      # Verify the message was added to the stream
      [message] = Streams.list_messages_for_stream(stream.id)
      assert message.key =~ "#{webhook.name}."
      assert Jason.decode!(message.data) == payload
    end

    test "returns 404 when webhook is not found", %{conn: conn} do
      payload = %{"id" => 1, "name" => "Paul Atreides"}

      conn = post(conn, ~p"/api/webhook/non_existent_webhook", payload)
      assert json_response(conn, 404) == %{"error" => "Webhook not found"}
    end

    # test "returns 422 when message ingestion fails", %{conn: conn, webhook: webhook} do
    #   payload = %{"id" => 1, "name" => "Paul Atreides"}

    #   # Mock Streams.send_messages to return an error
    #   Mox.expect(Sequin.StreamsMock, :send_messages, fn _, _ -> {:error, "Some error"} end)

    #   conn = post(conn, ~p"/api/webhook/#{webhook.name}", payload)
    #   assert json_response(conn, 422) == %{"error" => "Failed to ingest message"}
    # end

    test "generates correct key with SHA256 hash", %{conn: conn, webhook: webhook, stream: stream} do
      payload = %{"id" => 1, "name" => "Paul Atreides"}

      conn = post(conn, ~p"/api/webhook/#{webhook.name}", payload)
      assert json_response(conn, 200) == %{"success" => true}

      [message] = Streams.list_messages_for_stream(stream.id)
      expected_hash = :sha256 |> :crypto.hash(Jason.encode!(payload)) |> Base.encode16()
      assert message.key == "#{webhook.name}.#{expected_hash}"
    end
  end
end
