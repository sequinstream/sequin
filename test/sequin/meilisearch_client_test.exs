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
        assert conn.method == "PUT"
        assert conn.request_path == "/indexes/test/documents"

        {:ok, body, _} = Plug.Conn.read_body(conn)

        body_records =
          body
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

      assert :ok = Client.import_documents(@sink, "test", records)
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
      assert :ok = Client.delete_documents(@sink, "test", ids)
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

  describe "update_documents_with_function/5" do
    test "successfully sends function update request" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents/edit"

        {:ok, body, _} = Plug.Conn.read_body(conn)
        # Handle compressed body
        body =
          try do
            :zlib.gunzip(body)
          rescue
            _ -> body
          end

        body = Jason.decode!(body)

        assert body["filter"] == "id = 5"
        assert body["function"] == "doc.content[1] = context.new_block"
        assert body["context"]["new_block"] == "block2 - v2"

        Req.Test.json(conn, %{"taskUid" => 123})
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/123"

        Req.Test.json(conn, %{"status" => "succeeded"})
      end)

      assert :ok =
               Client.update_documents_with_function(
                 @sink,
                 "test",
                 "id = 5",
                 "doc.content[1] = context.new_block",
                 %{"new_block" => "block2 - v2"}
               )
    end

    test "handles empty context by omitting it from request" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents/edit"

        {:ok, body, _} = Plug.Conn.read_body(conn)
        # Handle compressed body
        body =
          try do
            :zlib.gunzip(body)
          rescue
            _ -> body
          end

        body = Jason.decode!(body)

        assert body["filter"] == "id = 1"
        assert body["function"] == "doc.field = 'value'"
        refute Map.has_key?(body, "context")

        Req.Test.json(conn, %{"taskUid" => 456})
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/456"

        Req.Test.json(conn, %{"status" => "succeeded"})
      end)

      assert :ok =
               Client.update_documents_with_function(
                 @sink,
                 "test",
                 "id = 1",
                 "doc.field = 'value'"
               )
    end

    test "returns error when function update fails" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents/edit"

        Req.Test.json(conn, %{"taskUid" => 789})
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/789"

        Req.Test.json(conn, %{
          "status" => "failed",
          "error" => %{
            "message" => "Invalid filter expression",
            "code" => "invalid_search_filter"
          }
        })
      end)

      assert {:error, error} =
               Client.update_documents_with_function(
                 @sink,
                 "test",
                 "invalid filter",
                 "doc.field = 'value'",
                 %{}
               )

      assert error.service == :meilisearch
      assert error.message == "[meilisearch]: Invalid filter expression"
    end
  end
end
