defmodule SequinWeb.TransformEditTest do
  use SequinWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias Sequin.Consumers.FunctionTransform
  alias Sequin.Consumers.Transform
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Transforms.MiniElixir

  test "Do not try to run obviously-invalid function transforms", %{conn: conn} do
    telref =
      :telemetry_test.attach_event_handlers(self(), [
        [:minielixir, :compile, :exception],
        [:minielixir, :interpret, :exception]
      ])

    conn = log_in_user(conn, AccountsFactory.insert_user!())
    {:ok, lv, _} = live(conn, ~p"/transforms/new")

    p1 = %{"description" => nil, "id" => nil, "name" => nil, "transform" => %{"type" => "function"}}

    render_hook(lv, "validate", %{"transform" => p1})

    # assigns = :sys.get_state(lv.pid).socket.assigns

    refute_receive {[:minielixir, _, :exception], ^telref, _, _}, 10

    :telemetry.detach(telref)
  end

  test "Failure telemetry is actually emitted" do
    telref =
      :telemetry_test.attach_event_handlers(self(), [
        [:minielixir, :compile, :exception],
        [:minielixir, :interpret, :exception]
      ])

    MiniElixir.run_interpreted_inner(
      %Transform{id: "fake", transform: %FunctionTransform{code: "{"}},
      %{action: 1, record: 2, changes: 3, metadata: 4}
    )

    assert_receive {[:minielixir, _, :exception], ^telref, _, _}, 10
    :telemetry.detach(telref)
  end

  # TEST `end` to prvide proper validation error for ecto 
end
