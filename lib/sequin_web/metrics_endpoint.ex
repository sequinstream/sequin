defmodule SequinWeb.MetricsEndpoint do
  use Phoenix.Endpoint, otp_app: :sequin

  plug :metrics_auth
  plug Sequin.PrometheusExporter

  defp metrics_auth(conn, _opts) do
    case Application.get_env(:sequin, :metrics_basic_auth) do
      nil -> conn
      conf -> Plug.BasicAuth.basic_auth(conn, conf)
    end
  end
end
