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

  defstruct [:url, :timeout_seconds, req_client: Req, auth_type: nil, auth_value: nil, index_name: nil]

  def new(%ElasticsearchSink{} = sink) do
    %__MODULE__{
      url: sink.endpoint_url,
      timeout_seconds: :timer.seconds(60),
      auth_type: sink.auth_type,
      auth_value: sink.auth_value,
      index_name: sink.index_name
    }
  end

  @doc """
  Import multiple documents in NDJSON format for bulk indexing.
  """
  @spec import_documents(client :: struct(), index_name :: String.t(), ndjson :: String.t()) ::
          {:ok, [index_result()]} | {:error, Error.t()}
  def import_documents(%__MODULE__{} = client, index_name, ndjson) do
    req = base_request(client)

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
  @spec test_connection(client :: struct()) :: :ok | {:error, Error.t()}
  def test_connection(%__MODULE__{} = client) do
    req = base_request(client)
    query = %{query: %{match_all: %{}}}

    case Req.post(req,
           url: "/#{client.index_name}/_search",
           headers: [{"Content-Type", "application/json"}],
           json: query
         ) do
      {:ok, %{status: st}} when st in 200..299 ->
        :ok

      {:ok, %{status: 404}} ->
        {:error, Error.not_found(entity: :elasticsearch_index, params: %{index_name: client.index_name})}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :elasticsearch,
           message: "Failed to connect to Elasticsearch: #{error_message}",
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

  defp base_request(%__MODULE__{} = client) do
    Req.new(
      base_url: client.url,
      headers: auth_header(client),
      receive_timeout: :timer.seconds(client.timeout_seconds),
      retry: false
    )
  end

  defp auth_header(%__MODULE__{} = client) do
    case client.auth_type do
      :api_key -> [{"Authorization", "ApiKey #{client.auth_value}"}]
      :basic -> [{"Authorization", "Basic #{Base.encode64(client.auth_value)}"}]
      :bearer -> [{"Authorization", "Bearer #{client.auth_value}"}]
    end
  end

  defp extract_error_message(body) when is_map(body) do
    cond do
      is_binary(body["message"]) -> body["message"]
      is_binary(body["error"]) -> body["error"]
      true -> Jason.encode!(body)
    end
  end

  defp extract_error_message(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> extract_error_message(decoded)
      {:error, _} -> body
    end
  end

  defp extract_error_message(body) when is_list(body) do
    body
    |> Enum.map(&extract_error_message/1)
    |> Enum.frequencies()
    |> Enum.map_join("\n", fn {msg, n} ->
      "#{n}x #{msg}"
    end)
  end

  defp extract_error_message(body) do
    inspect(body)
  end
end
