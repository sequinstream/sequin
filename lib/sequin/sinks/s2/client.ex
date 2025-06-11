defmodule Sequin.Sinks.S2.Client do
  @moduledoc false
  alias Sequin.Consumers.S2Sink
  alias Sequin.Error

  require Logger

  @spec test_connection(S2Sink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%S2Sink{} = sink) do
    req = base_request(sink)
    stream_url = "/streams/#{sink.stream}"
    basin_url = "/basins/#{sink.basin}"

    # First try to access the stream
    case Req.get(req, url: stream_url) do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: 404}} ->
        # Stream doesn't exist, check basin configuration
        case Req.get(req, url: basin_url) do
          {:ok, %{status: status, body: body}} when status in 200..299 ->
            case body do
              %{"create_stream_on_append" => true} ->
                :ok

              _ ->
                {:error,
                 Error.service(
                   service: :s2,
                   message: "Stream does not exist and basin does not have create_stream_on_append enabled",
                   details: %{stream: sink.stream, basin: sink.basin}
                 )}
            end

          {:ok, %{status: 404, body: body}} ->
            {:error,
             Error.service(
               service: :s2,
               message: "Basin does not exist",
               details: %{status: 404, body: body}
             )}

          {:ok, %{status: status, body: body}} ->
            {:error,
             Error.service(
               service: :s2,
               message: "Failed to check basin configuration",
               details: %{status: status, body: body}
             )}

          {:error, %Req.TransportError{} = error} ->
            Logger.error("[S2] Transport error: #{Exception.message(error)}")
            {:error, Error.service(service: :s2, message: "Transport error: #{Exception.message(error)}")}

          {:error, reason} ->
            {:error, Error.service(service: :s2, message: "Unknown error", details: reason)}
        end

      {:ok, %{status: status, body: body}} ->
        {:error,
         Error.service(
           service: :s2,
           message: "Failed to check stream",
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
    records_stream_url = "/streams/#{sink.stream}/records"

    case Req.post(req, url: records_stream_url, json: %{records: records}) do
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
end
