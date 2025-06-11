defmodule Sequin.Sinks.S2.Client do
  @moduledoc false
  alias Sequin.Consumers.S2Sink
  alias Sequin.Error

  require Logger

  @spec test_connection(S2Sink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%S2Sink{} = sink) do
    req = base_request(sink)

    case Req.get(req, url: "/streams/#{sink.stream}") do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: status, body: body}} ->
        {:error,
         Error.service(
           service: :s2,
           message: "Failed to connect to S2: [#{status}]",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[S2] Transport error: #{Exception.message(error)}")
        {:error, Error.service(service: :s2, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :s2, message: "Unknown error", details: reason)}
    end
  end

  @spec append_records(S2Sink.t(), [map()]) :: :ok | {:error, Error.t()}
  def append_records(%S2Sink{} = sink, records) when is_list(records) do
    req = base_request(sink)

    case Req.post(req, url: "/streams/#{sink.stream}/records", json: %{records: records}) do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: status, body: body}} ->
        {:error,
         Error.service(
           service: :s2,
           message: "Failed to append records",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[S2] Transport error: #{Exception.message(error)}")
        {:error, Error.service(service: :s2, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :s2, message: "Unknown error", details: reason)}
    end
  end

  defp base_request(%S2Sink{} = sink) do
    Req.new(
      base_url: String.trim_trailing(sink.endpoint_url, "/"),
      headers: [
        {"authorization", "Bearer #{sink.access_token}"},
        {"content-type", "application/json"}
      ],
      receive_timeout: :timer.seconds(60),
      retry: false
    )
  end
end
