defmodule SequinWeb.PushWebhookController do
  use SequinWeb, :controller

  alias Sequin.Redis
  alias SequinWeb.ApiFallbackPlug

  require Logger

  action_fallback ApiFallbackPlug

  def ack(conn, params) do
    [message] = params["data"]
    action = message["action"]
    id = message["record"]["id"]

    # Create a unique key for this action:id combination
    lsn = message["metadata"]["commit_lsn"]
    key = "#{action}:#{id}:2"

    # Check if this key exists in Redis
    case Redis.command(["EXISTS", key]) do
      {:ok, "1"} ->
        # Key exists, this is a duplicate
        Logger.error("Duplicate webhook received for #{key} #{lsn}")
        send_resp(conn, 200, "ACK")

      {:ok, "0"} ->
        # Key doesn't exist, store it and ack
        # Store for 24 hours
        Redis.command(["SET", key, "1", "EX", 86_400])
        send_resp(conn, 200, "ACK")

      {:error, error} ->
        Logger.error("Redis error checking for duplicate: #{inspect(error)}")
        send_resp(conn, 200, "ACK")
    end
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
