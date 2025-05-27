defmodule Sequin.Consumers.HttpPushSinkTest do
  use Sequin.DataCase, async: true

  import Sequin.TestSupport

  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Factory

  describe "via_sqs settings in HttpPushSink" do
    test "creating a sink without via_sqs enabled" do
      # Stub the application environment to ensure via_sqs_for_new_sinks? is false
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: false]
        app, key -> Application.get_env(app, key)
      end)

      changes = HttpPushSink.changeset(%HttpPushSink{}, attrs())
      assert {:ok, sink} = Ecto.Changeset.apply_action(changes, :insert)
      assert sink.via_sqs == false
    end

    test "creating a sink with via_sqs_for_new_sinks? enabled" do
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: true]
        app, key -> Application.get_env(app, key)
      end)

      changes = HttpPushSink.changeset(%HttpPushSink{}, attrs())
      assert {:ok, sink} = Ecto.Changeset.apply_action(changes, :insert)
      assert sink.via_sqs == true
    end

    test "creating a sink with explicit via_sqs: true parameter" do
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: false]
        app, key -> Application.get_env(app, key)
      end)

      changes = HttpPushSink.changeset(%HttpPushSink{}, Map.put(attrs(), :via_sqs, true))
      assert {:ok, sink} = Ecto.Changeset.apply_action(changes, :insert)
      assert sink.via_sqs == true
    end

    test "updating a sink should preserve via_sqs setting" do
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: true]
        app, key -> Application.get_env(app, key)
      end)

      initial_changeset = HttpPushSink.changeset(%HttpPushSink{}, attrs())
      assert {:ok, sink} = Ecto.Changeset.apply_action(initial_changeset, :insert)
      assert sink.via_sqs == true

      update_changeset = HttpPushSink.changeset(sink, %{http_endpoint_path: "/updated-webhook"})

      assert {:ok, updated_sink} = Ecto.Changeset.apply_action(update_changeset, :update)
      assert updated_sink.via_sqs == true
    end

    test "explicitly update via_sqs during an update" do
      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: false]
        app, key -> Application.get_env(app, key)
      end)

      initial_changeset = HttpPushSink.changeset(%HttpPushSink{}, attrs())
      assert {:ok, sink} = Ecto.Changeset.apply_action(initial_changeset, :insert)
      assert sink.via_sqs == false

      update_changeset = HttpPushSink.changeset(sink, %{via_sqs: true})

      assert {:ok, updated_sink} = Ecto.Changeset.apply_action(update_changeset, :update)
      assert updated_sink.via_sqs == true
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
