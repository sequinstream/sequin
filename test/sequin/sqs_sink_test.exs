defmodule Sequin.Consumers.SqsSinkTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.SqsSink
  alias Sequin.Test.AwsTestHelpers

  describe "changeset/2" do
    test "sets queue_url to blank when routing_mode is dynamic" do
      changeset =
        SqsSink.changeset(%SqsSink{}, %{
          queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test",
          region: "us-east-1",
          access_key_id: "key",
          secret_access_key: "secret",
          routing_mode: :dynamic
        })

      refute :queue_url in changeset.changes
    end

    test "validates explicit credentials when use_task_role is false" do
      changeset =
        SqsSink.changeset(%SqsSink{}, %{
          queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test",
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
        SqsSink.changeset(%SqsSink{}, %{
          queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test",
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
        SqsSink.changeset(%SqsSink{}, %{
          use_task_role: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:region] == {"can't be blank", [validation: :required]}
    end

    test "infers region from queue_url when not provided" do
      changeset =
        SqsSink.changeset(%SqsSink{}, %{
          queue_url: "https://sqs.us-west-2.amazonaws.com/123456789012/test",
          use_task_role: true,
          routing_mode: :static
        })

      assert changeset.valid?
      assert changeset.changes.region == "us-west-2"
    end
  end

  describe "aws_client/1" do
    test "creates client with explicit credentials when use_task_role is false" do
      sink = %SqsSink{
        region: "us-east-1",
        access_key_id: "test_key",
        secret_access_key: "test_secret",
        use_task_role: false
      }

      assert {:ok, client} = SqsSink.aws_client(sink)
      assert client.access_key_id == "test_key"
      assert client.secret_access_key == "test_secret"
      assert client.region == "us-east-1"
    end

    test "creates client with task role credentials when use_task_role is true" do
      AwsTestHelpers.setup_task_role_stub(
        access_key_id: "ASIA123456789",
        secret_access_key: "mock_secret",
        session_token: "mock_token"
      )

      sink = %SqsSink{
        region: "us-east-1",
        use_task_role: true
      }

      assert {:ok, client} = SqsSink.aws_client(sink)
      assert client.access_key_id == "ASIA123456789"
      assert client.secret_access_key == "mock_secret"
      assert client.session_token == "mock_token"
      assert client.region == "us-east-1"
    end

    test "returns error when task role credentials are unavailable" do
      AwsTestHelpers.setup_failed_task_role_stub()

      sink = %SqsSink{
        region: "us-east-1",
        use_task_role: true
      }

      expected_error = Sequin.Error.service(service: :aws, message: "No credentials available")
      assert {:error, ^expected_error} = SqsSink.aws_client(sink)
    end
  end
end
