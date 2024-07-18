defmodule SequinWeb.ObserveChannelTest do
  use SequinWeb.ChannelCase

  setup do
    {:ok, _, socket} =
      SequinWeb.UserSocket
      |> socket("user_id", %{some: :assign})
      |> subscribe_and_join(SequinWeb.ObserveChannel, "observe")

    %{socket: socket}
  end

  test "broadcasts are pushed to the client", %{socket: socket} do
    broadcast_from!(socket, "broadcast", %{"some" => "data"})
    assert_push "broadcast", %{"some" => "data"}
  end
end
