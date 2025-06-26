defmodule SequinWeb.TransformEditTest do
  use SequinWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias Sequin.Consumers.Function
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir

  describe "runs functions" do
    setup %{conn: conn} do
      conn = log_in_user(conn, AccountsFactory.insert_user!())
      {:ok, conn: conn}
    end

    test "of type transform", %{conn: conn} do
      {:ok, lv, _} = live(conn, ~p"/functions/new")

      p1 = %{
        "description" => nil,
        "id" => nil,
        "name" => "test",
        "function" => %{
          "type" => "transform",
          "code" => "def transform(action, record, changes, metadata) do\n  record\nend\n"
        }
      }

      render_hook(lv, "validate", %{"function" => p1})

      assert [test_message] = assigns(lv).encoded_synthetic_test_message
      assert is_map(test_message.transformed)
      refute test_message[:error]
    end

    test "of type filter", %{conn: conn} do
      {:ok, lv, _} = live(conn, ~p"/functions/new")

      p1 = %{
        "description" => nil,
        "id" => nil,
        "name" => "test",
        "function" => %{
          "type" => "filter",
          "code" => "def filter(action, record, changes, metadata) do\n  true\nend\n"
        }
      }

      render_hook(lv, "validate", %{"function" => p1})

      assert [test_message] = assigns(lv).encoded_synthetic_test_message
      assert test_message.transformed == true
      refute test_message[:error]
    end

    test "of type routing", %{conn: conn} do
      {:ok, lv, _} = live(conn, ~p"/functions/new")

      p1 = %{
        "description" => nil,
        "id" => nil,
        "name" => "test",
        "function" => %{
          "type" => "routing",
          "sink_type" => "redis_string",
          "code" => "def route(action, record, changes, metadata) do\n  %{key: record[\"id\"]}\nend\n"
        }
      }

      render_hook(lv, "validate", %{"function" => p1})

      assert [test_message] = assigns(lv).encoded_synthetic_test_message
      assert %{key: key} = test_message.transformed
      assert key
      refute test_message[:error]
    end
  end

  test "Do not try to run obviously-invalid function transforms", %{conn: conn} do
    telref =
      :telemetry_test.attach_event_handlers(self(), [
        [:minielixir, :compile, :exception],
        [:minielixir, :interpret, :exception]
      ])

    conn = log_in_user(conn, AccountsFactory.insert_user!())
    {:ok, lv, _} = live(conn, ~p"/functions/new")

    p1 = %{"description" => nil, "id" => nil, "name" => nil, "function" => %{"type" => "transform"}}

    render_hook(lv, "validate", %{"function" => p1})

    refute_receive {[:minielixir, _, :exception], ^telref, _, _}, 10

    :telemetry.detach(telref)
  end

  @tag :capture_log
  test "Failure telemetry is actually emitted" do
    telref =
      :telemetry_test.attach_event_handlers(self(), [
        [:minielixir, :compile, :exception],
        [:minielixir, :interpret, :exception]
      ])

    MiniElixir.run_interpreted_inner(
      %Function{id: "fake", function: %TransformFunction{code: "{"}},
      ConsumersFactory.consumer_event_data(
        action: :insert,
        record: %{"id" => "xyz"}
      )
    )

    assert_receive {[:minielixir, _, :exception], ^telref, _, _}, 10
    :telemetry.detach(telref)
  end

  defp assigns(lv) do
    :sys.get_state(lv.pid).socket.assigns
  end
end
