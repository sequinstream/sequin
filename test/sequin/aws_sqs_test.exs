defmodule Sequin.Aws.SQSTest do
  use ExUnit.Case, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SQS
  alias Sequin.Factory.DestinationFactory

  @queue_url "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

  setup do
    client =
      "test"
      |> AWS.Client.create("test", "us-east-1")
      |> HttpClient.put_client()

    {:ok, client: client}
  end

  describe "send_messages/3" do
    test "successfully sends batch of messages", %{client: client} do
      messages = [
        DestinationFactory.sqs_message(),
        DestinationFactory.sqs_message()
      ]

      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.host == "sqs.us-east-1.amazonaws.com"
        assert conn.method == "POST"

        Req.Test.json(conn, %{
          "Successful" => [
            %{"Id" => "1", "MessageId" => "msg1", "MD5OfMessageBody" => "test"},
            %{"Id" => "2", "MessageId" => "msg2", "MD5OfMessageBody" => "test"}
          ],
          "Failed" => []
        })
      end)

      assert :ok = SQS.send_messages(client, @queue_url, messages)
    end

    test "returns error when batch send fails", %{client: client} do
      messages = [DestinationFactory.sqs_message()]

      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        Req.Test.json(conn, %{
          "Failed" => [
            %{
              "Id" => "1",
              "Code" => "InternalError",
              "Message" => "Internal Error occurred"
            }
          ],
          "Successful" => []
        })
      end)

      assert {:error, %{"Failed" => [_failed_message]}, _} = SQS.send_messages(client, @queue_url, messages)
    end
  end

  describe "queue_meta/2" do
    test "successfully retrieves queue metadata", %{client: client} do
      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.method == "POST"
        assert String.contains?(conn.host, "sqs.us-east-1.amazonaws.com")

        Req.Test.json(conn, %{
          "Attributes" => %{
            "ApproximateNumberOfMessages" => "10",
            "ApproximateNumberOfMessagesNotVisible" => "5",
            "QueueArn" => "arn:aws:sqs:us-east-1:123456789012:test-queue"
          }
        })
      end)

      assert {:ok, meta} = SQS.queue_meta(client, @queue_url)
      assert meta.messages == 10
      assert meta.not_visible == 5
      assert meta.arn == "arn:aws:sqs:us-east-1:123456789012:test-queue"
    end
  end
end
