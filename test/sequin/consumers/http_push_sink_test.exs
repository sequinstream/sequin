defmodule Sequin.Consumers.HttpPushSinkTest do
  use Sequin.DataCase, async: true

  import Sequin.TestSupport

  alias Ecto.Changeset
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Factory

  describe "via settings in HttpPushSink" do
    test "creating a sink without via settings" do
      # Stub the application environment to ensure nil return for via config
      expect_application_get_env(1, fn
        :sequin, HttpPushSink ->
          :sequin |> Application.get_env(HttpPushSink) |> Keyword.put(:via, nil)
      end)

      changes = HttpPushSink.changeset(%HttpPushSink{}, attrs())
      assert changes.valid?
      refute Changeset.get_field(changes, :via)
    end

    test "creating a sink with via settings" do
      # Stub the application environment to return via config
      via_config = %{
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        region: "us-east-1",
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key"
      }

      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via: via_config]
        app, key -> Application.get_env(app, key)
      end)

      # Create a sink and validate changeset
      changes = HttpPushSink.changeset(%HttpPushSink{}, attrs())

      # Assert that the changeset is valid and via field is populated with expected values
      assert changes.valid?
      via = Changeset.get_field(changes, :via)
      assert via
      assert via.queue_url == via_config.queue_url
      assert via.region == via_config.region
      assert via.access_key_id == via_config.access_key_id
      assert via.is_fifo == false
    end

    test "creating a sink with FIFO queue via settings" do
      # Stub the application environment to return via config with FIFO queue
      via_config = %{
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo",
        region: "us-east-1",
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key"
      }

      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via: via_config]
        app, key -> Application.get_env(app, key)
      end)

      # Create a sink and validate changeset
      changes = HttpPushSink.changeset(%HttpPushSink{}, attrs(http_endpoint_path: "/webhook"))

      # Assert that the changeset is valid and via field has is_fifo set to true
      assert changes.valid?
      via = Changeset.get_field(changes, :via)
      assert via != nil
      assert via.is_fifo == true
    end

    test "updating a sink should preserve via settings" do
      # Set up initial via config
      via_config = %{
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        region: "us-east-1",
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key"
      }

      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via: via_config]
        app, key -> Application.get_env(app, key)
      end)

      # Create initial sink with via settings
      initial_changeset = HttpPushSink.changeset(%HttpPushSink{}, attrs(http_endpoint_path: "/webhook"))
      assert initial_changeset.valid?

      # Apply the changeset to get a struct with the via data
      sink_with_via = Changeset.apply_changes(initial_changeset)

      # Update the sink with new path
      update_changeset = HttpPushSink.changeset(sink_with_via, %{http_endpoint_path: "/updated-webhook"})

      # Assert that the updated changeset is valid and still has via settings
      assert update_changeset.valid?
      via = Changeset.get_field(update_changeset, :via)
      assert via != nil
      assert via.queue_url == via_config.queue_url
      assert via.region == via_config.region
      assert via.access_key_id == via_config.access_key_id
    end

    test "updating a sink after the env settings change changes the via" do
      # Initial via config
      initial_via_config = %{
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/initial-queue",
        region: "us-east-1",
        access_key_id: "initial-access-key",
        secret_access_key: "initial-secret-key"
      }

      # Stub the application environment to return initial via config
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via: initial_via_config]
        app, key -> Application.get_env(app, key)
      end)

      # Create initial sink with via settings
      initial_changeset = HttpPushSink.changeset(%HttpPushSink{}, attrs(http_endpoint_path: "/webhook"))
      assert initial_changeset.valid?

      # Apply the changeset to get a struct with the via data
      sink_with_via = Changeset.apply_changes(initial_changeset)

      # Change the application environment for via config
      updated_via_config = %{
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/updated-queue",
        region: "us-west-2",
        access_key_id: "updated-access-key",
        secret_access_key: "updated-secret-key"
      }

      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via: updated_via_config]
        app, key -> Application.get_env(app, key)
      end)

      # Update the sink with new path
      update_changeset = HttpPushSink.changeset(sink_with_via, %{http_endpoint_path: "/updated-webhook"})

      # Assert that the updated changeset still has the ORIGINAL via settings
      assert update_changeset.valid?
      via = Changeset.get_field(update_changeset, :via)
      assert via != nil
      assert via.queue_url == updated_via_config.queue_url
      assert via.region == updated_via_config.region
      assert via.access_key_id == updated_via_config.access_key_id
    end
  end

  defp attrs(attrs \\ []) do
    defaults = [
      type: :http_push,
      http_endpoint_path: "/",
      mode: :static,
      batch: true,
      http_endpoint_id: Factory.uuid()
    ]

    defaults
    |> Keyword.merge(attrs)
    |> Map.new()
  end
end
