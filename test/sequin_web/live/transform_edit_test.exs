defmodule SequinWeb.TransformEditTest do
  use SequinWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias Sequin.Consumers.Function
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir

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

    # assigns = :sys.get_state(lv.pid).socket.assigns

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

  # TEST `end` to prvide proper validation error for ecto
end
