defmodule Sequin.Sinks.S2.HttpClient do
  @moduledoc false

  alias Sequin.Consumers.S2Sink

  @type req_opts :: Keyword.t()
  @type req_response :: {:ok, Req.Response.t()} | {:error, term()}

  @spec get(S2Sink.t(), req_opts()) :: req_response()
  def get(%S2Sink{} = sink, opts) do
    req = base_request(sink)
    Req.get(req, Keyword.merge(default_req_opts(), opts))
  end

  @spec post(S2Sink.t(), req_opts()) :: req_response()
  def post(%S2Sink{} = sink, opts) do
    req = base_request(sink)
    Req.post(req, Keyword.merge(default_req_opts(), opts))
  end

  defp base_request(%S2Sink{} = sink) do
    base_url = S2Sink.endpoint_url(sink)

    Req.new(
      base_url: String.trim_trailing(base_url, "/"),
      headers: [
        {"authorization", "Bearer #{sink.access_token}"},
        {"content-type", "application/json"}
      ],
      receive_timeout: :timer.seconds(60),
      retry: false,
      compressed: true
    )
  end

  defp default_req_opts do
    Application.get_env(:sequin, :s2, [])[:req_opts] || []
  end
end
