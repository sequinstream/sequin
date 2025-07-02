defmodule Sequin.Consumers.AzureEventHubSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.AzureEventHubSink

  describe "changeset/2" do
    test "requires event_hub_name when routing_mode is static" do
      params = %{
        namespace: "test-ns",
        event_hub_name: "hub",
        shared_access_key_name: "key",
        shared_access_key: "secret",
        routing_mode: :static
      }

      changeset = AzureEventHubSink.changeset(%AzureEventHubSink{}, params)
      assert changeset.valid?
    end

    test "clears event_hub_name when routing_mode is dynamic" do
      params = %{
        namespace: "test-ns",
        event_hub_name: "hub",
        shared_access_key_name: "key",
        shared_access_key: "secret",
        routing_mode: :dynamic
      }

      changeset = AzureEventHubSink.changeset(%AzureEventHubSink{}, params)
      refute Map.has_key?(changeset.changes, :event_hub_name)
    end
  end
end
