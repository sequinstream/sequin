defmodule Sequin.Aws.HttpClient do
  @moduledoc false
  @behaviour AWS.HTTPClient

  # @callback request( method :: atom(), url :: binary(), body :: iodata(), headers :: list(), options :: keyword()) ::
  # {:ok, %{status_code: integer(), headers: [{binary(), binary()}], body: binary()}} | {:error, term()}
  @impl AWS.HTTPClient
  def request(method, url, body, headers, options) do
    # Don't decode body, AWS Request lib will do it
    res =
      [method: method, url: url, body: body, headers: headers, decode_body: false]
      |> Req.new()
      |> Req.merge(options)
      |> Req.merge(default_req_opts())
      |> dbg()
      |> Req.request()

    case res do
      {:ok, %{status: status, headers: headers, body: body}} ->
        {:ok, %{status_code: status, headers: headers, body: body}}

      error ->
        error
    end
  end

  def put_client(%AWS.Client{} = client, req_opts \\ []) do
    Map.put(client, :http_client, {__MODULE__, req_opts})
  end

  defp default_req_opts do
    Application.get_env(:sequin, :aws_sqs, [])[:req_opts] || []
  end
end
