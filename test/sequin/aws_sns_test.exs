defmodule Sequin.Aws.SNSTest do
  use Sequin.Case, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SNS
  alias Sequin.Factory.AwsFactory
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

      Req.Test.stub(HttpClient, fn conn ->
        assert conn.host == "sns.us-east-1.amazonaws.com"
        assert conn.method == "POST"
        body = Sequin.Factory.AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      assert :ok = SNS.publish_messages(client, @topic_arn, messages)
    end

    test "returns error when batch publish fails", %{client: client} do
      messages = [SinkFactory.sns_message()]

      Req.Test.stub(HttpClient, fn conn ->
        body = AwsFactory.sns_publish_batch_response_failure()
        Req.Test.text(conn, body)
      end)

      assert {:error, resp, _} = SNS.publish_messages(client, @topic_arn, messages)
      assert is_map(resp["PublishBatchResponse"]["PublishBatchResult"]["Failed"])
    end
  end

  describe "topic_meta/2" do
    test "successfully retrieves topic metadata", %{client: client} do
      Req.Test.stub(HttpClient, fn conn ->
        assert conn.method == "POST"
        assert String.contains?(conn.host, "sns.us-east-1.amazonaws.com")
        body = AwsFactory.sns_get_topic_attributes_response_success()
        Req.Test.text(conn, body)
      end)

      assert :ok = SNS.topic_meta(client, @topic_arn)
    end
  end

  describe "test_credentials_and_permissions/1" do
    test "successfully tests credentials with list topics", %{client: client} do
      Req.Test.stub(HttpClient, fn conn ->
        assert conn.method == "POST"
        assert String.contains?(conn.host, "sns.us-east-1.amazonaws.com")
        body = AwsFactory.sns_list_topics_response_success()
        Req.Test.text(conn, body)
      end)

      assert :ok = SNS.test_credentials_and_permissions(client)
    end
  end
end
