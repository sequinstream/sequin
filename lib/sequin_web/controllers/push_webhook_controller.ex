defmodule SequinWeb.PushWebhookController do
  use SequinWeb, :controller

  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def ack(conn, _params) do
    delay = :rand.uniform(500) + 1000
    Process.sleep(delay)

    send_resp(conn, 200, "ACK")
  end

  def maybe_ack(conn, _params) do
    delay = :rand.uniform(500) + 1000
    Process.sleep(delay)

    ack? = :rand.uniform(10) > 2

    if ack? do
      send_resp(conn, 200, "ACK")
    else
      send_resp(conn, 400, "NACK")
    end
  end

  def timeout(conn, _params) do
    Process.sleep(:infinity)
    send_resp(conn, 200, "TIMEOUT")
  end
end
