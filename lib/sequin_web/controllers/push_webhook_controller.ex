defmodule SequinWeb.PushWebhookController do
  use SequinWeb, :controller

  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def ack(conn, params) do
    if wait_ms = params["wait_ms"] do
      wait_ms = String.to_integer(wait_ms)
      Process.sleep(wait_ms)
    end

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

  def nack(conn, _params) do
    delay = :rand.uniform(500) + 1000
    Process.sleep(delay)

    send_resp(conn, 400, "NACK")
  end

  def timeout(conn, _params) do
    Process.sleep(:infinity)
    send_resp(conn, 200, "TIMEOUT")
  end
end
