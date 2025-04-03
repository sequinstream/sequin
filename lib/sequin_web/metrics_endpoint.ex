defmodule SequinWeb.MetricsEndpoint do
  use Phoenix.Endpoint, otp_app: :sequin

  plug Sequin.PrometheusExporter
end
