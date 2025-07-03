defmodule Sequin.Aws.KinesisTest do
  use Sequin.Case, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.Kinesis
  alias Sequin.Factory.SinkFactory

  @stream "test-stream"

  setup do
    client =
      "test"
      |> AWS.Client.create("test", "us-east-1")
      |> HttpClient.put_client()

    {:ok, client: client}
  end

  describe "put_records/3" do
    test "successfully sends batch of records", %{client: client} do
      records = [SinkFactory.kinesis_record(), SinkFactory.kinesis_record()]

      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.host == "kinesis.us-east-1.amazonaws.com"
        assert conn.method == "POST"

        Req.Test.json(conn, %{"FailedRecordCount" => 0, "Records" => []})
      end)

      assert :ok = Kinesis.put_records(client, @stream, records)
    end

    test "returns error when request fails", %{client: client} do
      records = [SinkFactory.kinesis_record()]

      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        Req.Test.json(conn, %{"FailedRecordCount" => 1, "Records" => []})
      end)

      assert {:error, _} = Kinesis.put_records(client, @stream, records)
    end
  end

  describe "test_credentials_and_permissions/1" do
    test "successfully tests credentials with list streams", %{client: client} do
      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.method == "POST"
        assert String.contains?(conn.host, "kinesis.us-east-1.amazonaws.com")
        Req.Test.json(conn, %{"StreamNames" => ["stream1", "stream2"]})
      end)

      assert :ok = Kinesis.test_credentials_and_permissions(client)
    end

    test "returns error when credentials are invalid", %{client: client} do
      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        conn
        |> Plug.Conn.put_status(400)
        |> Req.Test.json(%{"__type" => "UnrecognizedClientException", "message" => "Invalid credentials"})
      end)

      assert {:error, _} = Kinesis.test_credentials_and_permissions(client)
    end
  end
end
