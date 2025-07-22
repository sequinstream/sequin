defmodule Sequin.Sinks.Gcp.PubSubTest do
  use Sequin.Case, async: true

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Sinks.Gcp.PubSub

  @project_id "test-project"
  @topic_id "test-topic"

  setup do
    credentials = ConsumersFactory.gcp_credential_attrs()
    client = PubSub.new(@project_id, credentials)
    {:ok, client: client, credentials: credentials}
  end

  describe "authentication" do
    test "generates and caches auth token", %{client: client} do
      # First request should generate new token
      Req.Test.expect(PubSub, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "oauth2.googleapis.com"
        assert conn.request_path == "/token"
        # assert conn.req_body =~ "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer"
        {:ok, body, _} = Plug.Conn.read_body(conn)
        body =~ "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer"

        Req.Test.json(conn, %{"access_token" => "test_token"})
      end)

      # Next two requests make requests to pusub
      Req.Test.expect(PubSub, 2, fn conn ->
        assert conn.method == "GET"
        assert conn.host == "pubsub.googleapis.com"

        Req.Test.json(conn, topic_meta())
      end)

      # First request should generate new token
      assert {:ok, _} = PubSub.topic_metadata(client, @topic_id)

      # Second request should reuse cached token
      assert {:ok, _} = PubSub.topic_metadata(client, @topic_id)
    end

    test "handles auth failures gracefully", %{client: client} do
      Req.Test.expect(PubSub, fn conn ->
        conn
        |> Plug.Conn.put_status(400)
        |> Req.Test.json(%{"error" => "invalid_grant"})
      end)

      assert {:error, error} = PubSub.topic_metadata(client, @topic_id)
      assert error.service == :gcp_pubsub
      assert error.message =~ "Failed to exchange JWT for access token"
    end
  end

  describe "topic_metadata" do
    setup :expect_jwt_request

    test "successfully fetches and parses topic metadata", %{client: client} do
      topic = topic_meta()

      Req.Test.expect(PubSub, fn conn ->
        assert conn.host == "pubsub.googleapis.com"
        assert conn.request_path == "/v1/projects/#{@project_id}/topics/#{@topic_id}"

        Req.Test.json(conn, topic)
      end)

      assert {:ok, metadata} = PubSub.topic_metadata(client, @topic_id)
      assert metadata.name == topic["name"]
      assert metadata.labels == topic["labels"]
    end

    test "handles non-existent topics", %{client: client} do
      Req.Test.expect(PubSub, fn conn ->
        conn
        |> Plug.Conn.put_status(400)
        |> Req.Test.json(%{"error" => "Topic not found"})
      end)

      assert {:error, error} = PubSub.topic_metadata(client, "non-existent")
      assert error.message =~ "Topic not found"
    end
  end

  describe "publish_messages" do
    setup :expect_jwt_request

    test "successfully publishes single message", %{client: client} do
      message = %{data: %{key: "value"}, attributes: %{type: "test"}}

      Req.Test.expect(PubSub, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "pubsub.googleapis.com"
        assert conn.request_path == "/v1/projects/#{@project_id}/topics/#{@topic_id}:publish"

        {:ok, body, _} = Plug.Conn.read_body(conn)
        body = Jason.decode!(body)

        data = get_in(body, ["messages", Access.at(0), "data"])
        assert data == %{"key" => "value"}
        assert get_in(body, ["messages", Access.at(0), "attributes"]) == %{"type" => "test"}

        Req.Test.json(conn, %{})
      end)

      assert :ok = PubSub.publish_messages(client, @topic_id, [message])
    end

    test "handles publish failures", %{client: client} do
      Req.Test.expect(PubSub, fn conn ->
        conn
        |> Plug.Conn.put_status(500)
        |> Req.Test.json(%{"error" => "GCP internal error"})
      end)

      assert {:error, error} = PubSub.publish_messages(client, @topic_id, [%{data: "test"}])
      assert error.service == :gcp_pubsub
      assert error.message =~ "publish messages"
      assert error.message =~ "GCP internal error"
    end
  end

  describe "cache_key" do
    test "generates consistent keys for same credentials" do
      creds = ConsumersFactory.gcp_credential()
      key1 = PubSub.cache_key(creds)
      key2 = PubSub.cache_key(creds)
      assert key1 == key2
    end

    test "generates different keys for different credentials" do
      key1 = PubSub.cache_key(ConsumersFactory.gcp_credential())
      key2 = PubSub.cache_key(ConsumersFactory.gcp_credential())
      refute key1 == key2
    end
  end

  defp expect_jwt_request(_) do
    Req.Test.expect(PubSub, fn conn ->
      Req.Test.json(conn, %{"access_token" => "test_token"})
    end)

    :ok
  end

  defp topic_meta do
    %{
      "name" => Sequin.Factory.word(),
      "labels" => %{"env" => Sequin.Factory.word()},
      "messageStoragePolicy" => %{},
      "kmsKeyName" => Sequin.Factory.unique_word(),
      "schemaSettings" => %{},
      "messageRetentionDuration" => "#{Sequin.Factory.duration()}s"
    }
  end
end
