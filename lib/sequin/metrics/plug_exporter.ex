### https://github.com/prometheus-erl/prometheus-plugs/blob/3a2b5f6fdb359a4b3488a834b091cfa742fd82bb/lib/prometheus/plug_exporter.ex
defmodule Sequin.Prometheus.PlugExporter do
  @moduledoc """
  Exports Prometheus metrics via configurable endpoint:

  ``` elixir
  # define plug
  defmodule MetricsPlugExporter do
    use Prometheus.PlugExporter
  end

  # on app startup (e.g. supervisor setup)
  MetricsPlugExporter.setup()

  # in your plugs pipeline
  plug MetricsPlugExporter
  ```

  **Do not add to Phoenix Router pipelines!** You will be getting 404!
  ```
  Note that router pipelines are only invoked after a route is found.
  No plug is invoked in case no matches were found.
  ```

  ### Metrics

  Also maintains telemetry metrics:

    * `telemetry_scrape_duration_seconds`
    * `telemetry_scrape_size_bytes`

  Do not forget to call `setup/0` before using plug, for example on application start!

  ### Configuration

  Plugs exporter can be configured via PlugsExporter key of `:prometheus` app env.

  Default configuration:

  ```elixir
  config :prometheus, MetricsPlugExporter, # (you should replace this with the name of your plug)
    path: "/metrics",
    format: :auto, ## or :protobuf, or :text
    registry: :default,
    auth: false
  ```

  Export endpoint can be optionally secured using HTTP Basic Authentication:

  ```elixir
    auth: {:basic, "username", "password"}
  ```
  """

  require Logger

  use Prometheus.Metric

  use Prometheus.Config,
    path: "/metrics",
    format: :auto,
    registry: :default,
    auth: false

  ## TODO: support multiple endpoints [for example separate endpoint for each registry]
  ##  endpoints: [[registry: :qwe,
  ##               path: "/metrics1"],
  ##              [registry: :default,
  ##               path: "/metrics",
  ##               format: :protobuf]]
  defmacro __using__(_opts) do
    module_name = __CALLER__.module

    registry = Config.registry(module_name)
    path = Plug.Router.Utils.split(Config.path(module_name))
    auth = Config.auth(module_name)
    format = normalize_format(Config.format(module_name))
    Code.ensure_loaded!(:accept_header)

    quote do
      @behaviour Plug
      import Plug.Conn
      use Prometheus.Metric

      def setup() do
        Summary.declare(
          name: :telemetry_scrape_duration_seconds,
          help: "Scrape duration",
          labels: ["registry", "content_type"],
          registry: unquote(registry)
        )

        Summary.declare(
          name: :telemetry_scrape_size_bytes,
          help: "Scrape size, uncompressed",
          labels: ["registry", "content_type"],
          registry: unquote(registry)
        )
      end

      def init(_opts) do
      end

      def call(conn, _opts) do
        case conn.path_info do
          unquote(path) ->
            unquote(handle_auth(auth))

          _ ->
            conn
        end
      end

      defp scrape_data(conn) do
        {content_type, format} = negotiate(conn)
        labels = [unquote(registry), content_type]

        scrape =
          Summary.observe_duration(
            [
              registry: unquote(registry),
              name: :telemetry_scrape_duration_seconds,
              labels: labels
            ],
            fn ->
              format.format(unquote(registry))
            end
          )

        Summary.observe(
          [registry: unquote(registry), name: :telemetry_scrape_size_bytes, labels: labels],
          :erlang.iolist_size(scrape)
        )

        {content_type, scrape}
      end

      defp negotiate(conn) do
        unquote(
          if format == :auto do
            quote do
              try do
                [accept] = Plug.Conn.get_req_header(conn, "accept")

                format =
                  :accept_header.negotiate(
                    accept,
                    [
                      {:prometheus_text_format.content_type(), :prometheus_text_format},
                      {:prometheus_protobuf_format.content_type(), :prometheus_protobuf_format}
                    ]
                  )

                {format.content_type(), format}
              rescue
                ErlangError ->
                  {unquote(:prometheus_text_format.content_type()), :prometheus_text_format}
              end
            end
          else
            {format.content_type(), format}
          end
        )
      end

      unquote(
        case auth do
          {:basic, _, _} ->
            quote do
              ## borrowed from https://github.com/CultivateHQ/basic_auth
              ## The MIT License (MIT)
              ## Copyright (c) 2015 Mark Connell

              defp valid_basic_credentials?(["Basic " <> encoded_string], username, password) do
                Base.decode64!(encoded_string) == "#{username}:#{password}"
              end

              defp valid_basic_credentials?(_, _, _) do
                false
              end

              ## end of basic_auth code
            end

          _ ->
            :ok
        end
      )
    end
  end
  

  defp handle_auth(auth) do
    case auth do
      false ->
        send_metrics()

      {:basic, username, password} ->
        quote do
          if valid_basic_credentials?(
               Plug.Conn.get_req_header(conn, "authorization"),
               unquote(username),
               unquote(password)
             ) do
            unquote(send_metrics())
          else
            unquote(send_unauthorized("metrics"))
          end
        end
    end
  end

  defp send_metrics() do
    quote do
      {content_type, scrape} = scrape_data(conn)

      conn
      |> put_resp_content_type(content_type, nil)
      |> send_resp(200, scrape)
      |> halt
    end
  end

  defp send_unauthorized(realm) do
    quote do
      Plug.Conn.put_resp_header(conn, "www-authenticate", "Basic realm=\"#{unquote(realm)}\"")
      |> Plug.Conn.send_resp(401, "401 Unauthorized")
      |> Plug.Conn.halt()
    end
  end

  defp normalize_format(:auto), do: :auto
  defp normalize_format(:text), do: :prometheus_text_format
  defp normalize_format(:protobuf), do: :prometheus_protobuf_format
  defp normalize_format(Prometheus.Format.Text), do: :prometheus_text_format
  defp normalize_format(Prometheus.Format.Protobuf), do: :prometheus_protobuf_format
  defp normalize_format(format), do: format
end
