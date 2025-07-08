defmodule Sequin.Sinks.Elasticsearch.Client do
  @moduledoc """
  Client for interacting with the  API.
  """

  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Error

  require Logger

  @type index_result :: %{
          :id => String.t(),
          :status => :ok | :error,
          optional(:error_message) => String.t()
        }

  @doc """
  Import multiple documents in NDJSON format for bulk indexing.
  """
  @spec import_documents(
          sink :: ElasticsearchSink.t(),
          index_name :: String.t(),
          ndjson :: IO.chardata(),
          req_opts :: keyword()
        ) ::
          {:ok, [index_result()]} | {:error, Error.t()}
  def import_documents(%ElasticsearchSink{} = sink, index_name, ndjson, req_opts \\ []) do
    req = base_request(sink, req_opts)

    case Req.post(req,
           url: "/#{index_name}/_bulk",
           headers: [{"Content-Type", "application/x-ndjson"}],
           body: ndjson
         ) do
      {:ok, %{status: 200, body: %{"items" => items}}} ->
        {:ok, operation_results(items)}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :elasticsearch,
           message: "Batch import failed: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Elasticsearch] Transport error: #{Exception.message(error)}")
        {:error, Error.service(service: :elasticsearch, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :elasticsearch, message: "Unknown error", details: reason)}
    end
  end

  defp operation_results(items) when is_list(items) do
    Enum.map(items, &operation_result/1)
  end

  defp operation_result(item) do
    [operation] = Map.keys(item)
    [result] = Map.values(item)

    case {operation, result} do
      # 200 for update, 201 for create
      {"index", %{"_id" => id, "status" => status}} when status in [200, 201] ->
        %{id: id, status: :ok}

      # 200 for delete, 404 for missing so does not exist
      {"delete", %{"_id" => id, "status" => status}} when status in [200, 404] ->
        %{id: id, status: :ok}

      {_, %{"_id" => id, "error" => error}} ->
        %{"type" => type, "reason" => reason} = error
        error_message = "failed to #{operation} document #{id}: #{type}: #{reason}"
        %{id: id, status: :error, error_message: error_message}
    end
  end

  @doc """
  Test the connection to the Elasticsearch server using a search request.
  """
  @spec test_connection(sink :: ElasticsearchSink.t(), req_opts :: keyword()) :: :ok | {:error, Error.t()}
  def test_connection(%ElasticsearchSink{} = sink, req_opts \\ []) do
    req = base_request(sink, req_opts)

    case Req.get(req, url: "/") do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :elasticsearch,
           message: "Failed to connect to Elasticsearch: [#{status}] #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Elasticsearch] Failed to connect: #{Exception.message(error)}")
        {:error, Error.service(service: :elasticsearch, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :elasticsearch, message: "Unknown error", details: reason)}
    end
  end

  # Private helpers

  defp base_request(%ElasticsearchSink{} = sink, req_opts) do
    [
      base_url: sink.endpoint_url,
      headers: auth_header(sink),
      receive_timeout: to_timeout(minute: 1),
      retry: false,
      compress_body: true
    ]
    |> Req.new()
    |> Req.merge(req_opts)
  end

  defp auth_header(%ElasticsearchSink{} = sink) do
    case sink.auth_type do
      :none -> []
      :api_key -> [{"Authorization", "ApiKey #{sink.auth_value}"}]
      :basic -> [{"Authorization", "Basic #{Base.encode64(sink.auth_value)}"}]
      :bearer -> [{"Authorization", "Bearer #{sink.auth_value}"}]
    end
  end

  defp extract_error_message(%{"error" => error}) when is_map(error) do
    case Jason.encode(error, pretty: true) do
      {:ok, encoded} -> encoded
      {:error, _} -> inspect(error, pretty: true)
    end
  end

  defp extract_error_message(body) when is_map(body) do
    case Jason.encode(body, pretty: true) do
      {:ok, encoded} -> encoded
      {:error, _} -> inspect(body, pretty: true)
    end
  end

  defp extract_error_message(body) when is_binary(body), do: body

  defp extract_error_message(body) do
    inspect(body, pretty: true)
  end
end
