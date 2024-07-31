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

    #   # Mock Streams.upsert_messages to return an error
    #   Mox.expect(Sequin.StreamsMock, :upsert_messages, fn _, _ -> {:error, "Some error"} end)

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

    test "validates HMAC auth strategy", %{conn: conn, account: account, stream: stream} do
      secret = 32 |> :crypto.strong_rand_bytes() |> Base.encode64()

      webhook =
        SourcesFactory.insert_webhook!(
          account_id: account.id,
          stream_id: stream.id,
          auth_strategy: %{"type" => "hmac", "header_name" => "X-HMAC-Signature", "secret" => secret}
        )

      payload = %{"id" => 1, "name" => "Paul Atreides"}
      body = Jason.encode!(payload)

      expected_hmac =
        "hmac-sha256=" <> (:hmac |> :crypto.mac(:sha256, Base.decode64!(secret), body) |> Base.encode16(case: :lower))

      conn =
        conn
        |> put_req_header("x-hmac-signature", expected_hmac)
        |> post(~p"/api/webhook/#{webhook.name}", payload)

      assert json_response(conn, 200) == %{"success" => true}
    end

    test "returns 401 for invalid HMAC", %{conn: conn, account: account, stream: stream} do
      secret = 32 |> :crypto.strong_rand_bytes() |> Base.encode64()

      webhook =
        SourcesFactory.insert_webhook!(
          account_id: account.id,
          stream_id: stream.id,
          auth_strategy: %{"type" => "hmac", "header_name" => "X-HMAC-Signature", "secret" => secret}
        )

      payload = %{"id" => 1, "name" => "Paul Atreides"}

      conn =
        conn
        |> put_req_header("x-hmac-signature", "invalid-hmac")
        |> post(~p"/api/webhook/#{webhook.name}", payload)

      assert json_response(conn, 401) == %{"error" => "Unauthorized"}
    end
  end
end
