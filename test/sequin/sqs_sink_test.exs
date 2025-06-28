defmodule Sequin.Consumers.SqsSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.SqsSink

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
  end
end
