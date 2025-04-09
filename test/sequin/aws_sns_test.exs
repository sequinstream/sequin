defmodule Sequin.Aws.SNSTest do
  use Sequin.Case, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SNS
  alias Sequin.Factory.SinkFactory

  @topic_arn "arn:aws:sns:us-east-1:123456789012:test-topic"

  setup do
    client =
      "test"
      |> AWS.Client.create("test", "us-east-1")
      |> HttpClient.put_client()

    {:ok, client: client}
  end

  describe "publish_messages/3" do
    test "successfully publishes batch of messages", %{client: client} do
      messages = [
        SinkFactory.sns_message(),
        SinkFactory.sns_message()
      ]

      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.host == "sns.us-east-1.amazonaws.com"
        assert conn.method == "POST"

        Req.Test.json(conn, %{
          "Successful" => [
            %{"Id" => "1", "MessageId" => "msg1"},
            %{"Id" => "2", "MessageId" => "msg2"}
          ],
          "Failed" => []
        })
      end)

      assert :ok = SNS.publish_messages(client, @topic_arn, messages)
    end

    test "returns error when batch publish fails", %{client: client} do
      messages = [SinkFactory.sns_message()]

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

      assert {:error, %{"Failed" => [_failed_message]}, _} = SNS.publish_messages(client, @topic_arn, messages)
    end
  end

  describe "topic_meta/2" do
    test "successfully retrieves topic metadata", %{client: client} do
      Req.Test.stub(Sequin.Aws.HttpClient, fn conn ->
        assert conn.method == "POST"
        assert String.contains?(conn.host, "sns.us-east-1.amazonaws.com")

        Req.Test.json(conn, %{
          "Attributes" => %{
            "TopicArn" => "arn:aws:sns:us-east-1:123456789012:test-topic",
            "DisplayName" => "Test Topic",
            "SubscriptionsConfirmed" => "5"
          }
        })
      end)

      assert {:ok, meta} = SNS.topic_meta(client, @topic_arn)
      assert meta.topic_arn == "arn:aws:sns:us-east-1:123456789012:test-topic"
      assert meta.display_name == "Test Topic"
      assert meta.subscriptions_confirmed == 5
    end
  end
end
