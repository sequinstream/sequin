defmodule SequinWeb.HealthCheckController do
  use SequinWeb, :controller

  alias Sequin.CheckSystemHealth
  alias Sequin.Error.ServiceError
  alias SequinWeb.ApiFallbackPlug

  require Logger

  action_fallback ApiFallbackPlug

  def check_cloud(conn, _params) do
    case CheckSystemHealth.check() do
      :ok ->
        send_resp(conn, 200, Jason.encode!(%{ok: true, rev: System.get_env("RELEASE_VERSION")}))

      {:error, error} ->
        Logger.error("Error while checking health: #{inspect(error)}")

        send_resp(conn, 500, Jason.encode!(%{ok: false, error: "failed to reach Redis or Postgres"}))
    end
  end

  def check(conn, _params) do
    case CheckSystemHealth.check() do
      :ok ->
        send_resp(conn, 200, Jason.encode!(%{ok: true, rev: System.get_env("RELEASE_VERSION")}))

      {:error, %ServiceError{} = error} ->
        error_msg = "Error with service: #{error.message}"
        send_resp(conn, 500, Jason.encode!(%{ok: false, error: error_msg, details: error.details}))
    end
  end
end
