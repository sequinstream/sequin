defmodule Sequin.Consumers.SnsSinkTest do
  use Sequin.Case, async: true

  alias Sequin.AwsMock
  alias Sequin.Consumers.SnsSink
  alias Sequin.Test.AwsTestHelpers

  describe "changeset/2" do
    test "validates explicit credentials when use_task_role is false" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic",
          region: "us-east-1",
          use_task_role: false,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:access_key_id] == {"can't be blank", [validation: :required]}
      assert changeset.errors[:secret_access_key] == {"can't be blank", [validation: :required]}
    end

    test "only requires region when use_task_role is true" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic",
          region: "us-east-1",
          use_task_role: true,
          routing_mode: :static
        })

      assert changeset.valid?
      refute changeset.errors[:access_key_id]
      refute changeset.errors[:secret_access_key]
    end

    test "validates region is required when cannot be inferred" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          use_task_role: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:region] == {"can't be blank", [validation: :required]}
    end

    test "infers region from topic ARN when not provided" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          topic_arn: "arn:aws:sns:us-west-2:123456789012:test-topic",
          access_key_id: "test_key",
          secret_access_key: "test_secret",
          routing_mode: :static
        })

      assert changeset.valid?
      assert changeset.changes.region == "us-west-2"
    end

    test "sets topic_arn to nil when routing_mode is dynamic" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic",
          region: "us-east-1",
          access_key_id: "test_key",
          secret_access_key: "test_secret",
          routing_mode: :dynamic
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :topic_arn) == nil
    end

    test "detects FIFO topic from ARN" do
      changeset =
        SnsSink.changeset(%SnsSink{}, %{
          topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic.fifo",
          region: "us-east-1",
          access_key_id: "test_key",
          secret_access_key: "test_secret",
          routing_mode: :static
        })

      assert changeset.valid?
      assert changeset.changes.is_fifo == true
    end
  end

  describe "aws_client/1" do
    test "creates client with explicit credentials when use_task_role is false" do
      sink = %SnsSink{
        region: "us-east-1",
        access_key_id: "test_key",
        secret_access_key: "test_secret",
        use_task_role: false
      }

      assert {:ok, client} = SnsSink.aws_client(sink)
      assert client.access_key_id == "test_key"
      assert client.secret_access_key == "test_secret"
      assert client.region == "us-east-1"
    end

    test "creates client with task role credentials when use_task_role is true" do
      # Mock the task role credentials
      mock_credentials = %{
        access_key_id: "ASIA123456789",
        secret_access_key: "mock_secret",
        token: "mock_token"
      }

      expect(AwsMock, :get_client, fn "us-east-1" ->
        {:ok,
         mock_credentials.access_key_id
         |> AWS.Client.create(mock_credentials.secret_access_key, "us-east-1")
         |> Map.put(:session_token, mock_credentials.token)}
      end)

      sink = %SnsSink{
        region: "us-east-1",
        use_task_role: true
      }

      assert {:ok, client} = SnsSink.aws_client(sink)
      assert client.access_key_id == "ASIA123456789"
      assert client.secret_access_key == "mock_secret"
      assert client.session_token == "mock_token"
      assert client.region == "us-east-1"
    end

    test "returns error when task role credentials are unavailable" do
      AwsTestHelpers.setup_failed_task_role_stub()

      sink = %SnsSink{
        region: "us-east-1",
        use_task_role: true
      }

      expected_error = Sequin.Error.service(service: :aws, message: "No credentials available")
      assert {:error, ^expected_error} = SnsSink.aws_client(sink)
    end
  end

  describe "region_from_arn/1" do
    test "extracts region from valid SNS topic ARN" do
      assert "us-west-2" = SnsSink.region_from_arn("arn:aws:sns:us-west-2:123456789012:MyTopic")
      assert "eu-central-1" = SnsSink.region_from_arn("arn:aws:sns:eu-central-1:123456789012:AnotherTopic")
      assert "us-east-1" = SnsSink.region_from_arn("arn:aws:sns:us-east-1:123456789012:test-topic.fifo")
    end

    test "returns error for invalid ARN format" do
      assert {:error, _} = SnsSink.region_from_arn("invalid_arn")
      assert {:error, _} = SnsSink.region_from_arn("arn:aws:sqs:us-east-1:123456789012:queue")
    end
  end
end
