defmodule Sequin.Sinks.Meilisearch.ClientTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Factory.SinkFactory
  alias Sequin.Sinks.Meilisearch.Client

  @sink %MeilisearchSink{
    type: :meilisearch,
    endpoint_url: "http://127.0.0.1:7700",
    index_name: "test",
    primary_key: "id",
    api_key: "token"
  }

  describe "test_connection/1" do
    test "returns ok on 200" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/health"
        Req.Test.json(conn, %{})
      end)

      assert :ok = Client.test_connection(@sink)
    end
  end

  describe "import_documents/2" do
    test "successfully sends batch" do
      records = [SinkFactory.meilisearch_record(), SinkFactory.meilisearch_record()]

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents"

        {:ok, body, _} = Plug.Conn.read_body(conn)
        decompressed_body = :zlib.gunzip(body)

        body_records =
          decompressed_body
          |> String.split("\n", trim: true)
          |> Enum.map(&Jason.decode!/1)

        assert length(body_records) == length(records)

        Req.Test.json(conn, %{
          "taskUid" => 1
        })
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/1"

        Req.Test.json(conn, %{
          "status" => "success"
        })
      end)

      assert {:ok} = Client.import_documents(@sink, "test", records)
    end
  end

  describe "delete_documents/2" do
    test "successfully delete batch" do
      records = [SinkFactory.meilisearch_record(), SinkFactory.meilisearch_record()]

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents/delete-batch"

        Req.Test.json(conn, %{
          "taskUid" => 1
        })
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/1"

        Req.Test.json(conn, %{
          "status" => "success"
        })
      end)

      ids = Enum.map(records, & &1["id"])
      assert {:ok} = Client.delete_documents(@sink, "test", ids)
    end
  end

  describe "maybe_verify_index/3" do
    test "returns :ok when index exists with matching primary key" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/indexes/test"

        Req.Test.json(conn, %{
          "primaryKey" => "id"
        })
      end)

      assert :ok = Client.maybe_verify_index(@sink, "test", "id")
    end

    test "returns error when index exists with different primary key" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/indexes/test"

        Req.Test.json(conn, %{
          "primaryKey" => "different_id"
        })
      end)

      assert {:error, error} = Client.maybe_verify_index(@sink, "test", "id")
      assert error.message =~ ~s(Expected primary key "id", got "different_id")
    end

    test "returns error when index verification fails" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/indexes/test"

        conn
        |> Plug.Conn.put_status(404)
        |> Req.Test.json(%{"message" => "Index not found"})
      end)

      assert {:error, error} = Client.maybe_verify_index(@sink, "test", "id")
      assert error.message == "[meilisearch]: Index verification failed"
    end
  end
end
