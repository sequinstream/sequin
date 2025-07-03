defmodule Sequin.Consumers.KinesisSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.KinesisSink

  describe "changeset/2" do
    test "validates required fields" do
      changeset = KinesisSink.changeset(%KinesisSink{}, %{})
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset).region
      assert "can't be blank" in errors_on(changeset).access_key_id
      assert "can't be blank" in errors_on(changeset).secret_access_key
      assert "is required" in errors_on(changeset).routing_mode
    end

    test "validates with valid static routing" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          stream_arn: "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :static
        })

      assert changeset.valid?
      assert changeset.changes[:region] == "us-east-1"
    end

    test "validates with valid dynamic routing" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          region: "us-west-2",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :dynamic
        })

      assert changeset.valid?
    end

    test "requires stream_arn when routing_mode is static" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          region: "us-east-1",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :static
        })

      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset).stream_arn
    end

    test "sets stream_arn to nil when routing_mode is dynamic" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          stream_arn: "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
          region: "us-east-1",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :dynamic
        })

      assert changeset.valid?
      assert is_nil(Ecto.Changeset.get_change(changeset, :stream_arn))
    end

    test "validates stream ARN format" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          stream_arn: "invalid-arn",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :static
        })

      refute changeset.valid?

      assert "must be a valid AWS Kinesis Stream ARN (arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>)" in errors_on(
               changeset
             ).stream_arn
    end

    test "extracts region from stream ARN" do
      changeset =
        KinesisSink.changeset(%KinesisSink{}, %{
          stream_arn: "arn:aws:kinesis:eu-west-1:123456789012:stream/my-stream",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          routing_mode: :static
        })

      assert changeset.valid?
      assert changeset.changes[:region] == "eu-west-1"
    end
  end

  describe "region_from_arn/1" do
    test "extracts region from valid ARN" do
      arn = "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
      assert KinesisSink.region_from_arn(arn) == "us-west-2"
    end

    test "returns error for invalid ARN" do
      assert {:error, _} = KinesisSink.region_from_arn("invalid-arn")
    end
  end

  describe "aws_client/1" do
    test "creates AWS client with correct region" do
      sink = %KinesisSink{
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region: "us-west-2"
      }

      client = KinesisSink.aws_client(sink)
      assert client.region == "us-west-2"
    end

    test "creates AWS client using region from stream ARN" do
      sink = %KinesisSink{
        stream_arn: "arn:aws:kinesis:eu-central-1:123456789012:stream/test",
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
      }

      client = KinesisSink.aws_client(sink)
      assert client.region == "eu-central-1"
    end
  end

  # Helper function to extract error messages
  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
