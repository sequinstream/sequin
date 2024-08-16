defmodule SequinWeb.Presence do
  @moduledoc false
  use Phoenix.Presence,
    otp_app: :sequin,
    pubsub_server: Sequin.PubSub
end
