defmodule Sequin.Sinks.Typesense.Client do
  @moduledoc """
  Client for interacting with the Typesense API.
  """

  alias Sequin.Error

  require Logger

  defstruct [:api_key, :url, :timeout_seconds, req_client: Req]

  def new(opts) do
    %__MODULE__{
      url: Keyword.fetch!(opts, :url) |> String.trim_trailing("/"),
      api_key: Keyword.fetch!(opts, :api_key),
      timeout_seconds: Keyword.get(opts, :timeout_seconds, 30)
    }
  end

  @doc """
  Index a single document in a collection.
  """
  def index_document(%__MODULE__{} = client, collection_name, document, opts \\ []) do
    action = Keyword.get(opts, :action, "create")
    req = base_request(client)

    url = "/collections/#{collection_name}/documents"
    case Req.post(req, url: url, json: document, params: %{action: action}) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to index document: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to index document: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Import multiple documents in JSONL format.
  """
  def import_documents(%__MODULE__{} = client, collection_name, jsonl, opts \\ []) do
    action = Keyword.get(opts, :action, "create")
    batch_size = Keyword.get(opts, :batch_size, 40)
    req = base_request(client)

    query_params = %{
      "action" => action,
      "batch_size" => batch_size
    }

    case Req.post(req,
           url: "/collections/#{collection_name}/documents/import",
           headers: [{"Content-Type", "text/plain"}],
           body: jsonl,
           params: query_params
         ) do
      {:ok, %{status: 200, body: body}} ->
        responses =
          body
          |> String.split("\n", trim: true)
          |> Enum.map(&Jason.decode!/1)
        # :::warning NOTE
        # The import endpoint will always return a `HTTP 200 OK` code, regardless of the import results of the individual documents.

        # We do this because there might be some documents which succeeded on import and others that failed, and we don't want to return an HTTP error code in those partial scenarios.
        # To keep it consistent, we just return HTTP 200 in all cases.

        # So always be sure to check the API response for any `{success: false, ...}` records to see if there are any documents that failed import.
        # :::
        {:ok, responses}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to import documents: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to import documents: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Test the connection to the Typesense server.
  """
  def test_connection(%__MODULE__{} = client) do
    req = base_request(client)

    case Req.get(req, url: "/health") do
      {:ok, %{status: 200, body: %{"ok" => true}}} ->
        :ok

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to connect to Typesense: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to connect: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Create a new collection in Typesense.
  """
  def create_collection(%__MODULE__{} = client, schema) do
    req = base_request(client)

    case Req.post(req, url: "/collections", json: schema) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to create collection: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to create collection: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Get information about a collection.
  """
  def retrieve_collection(%__MODULE__{} = client, collection_name) do
    req = base_request(client)

    case Req.get(req, url: "/collections/#{collection_name}") do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}

      {:ok, %{status: 404}} ->
        {:error, Error.service(service: :typesense, message: "Collection '#{collection_name}' not found")}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to retrieve collection: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to retrieve collection: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  # Private helpers

  defp base_request(%__MODULE__{} = client) do
    Req.new(
      base_url: client.url,
      headers: [
        {"X-TYPESENSE-API-KEY", client.api_key}
      ],
      receive_timeout: :timer.seconds(client.timeout_seconds),
      retry: false
    )
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

  defp extract_error_message(body) do
    inspect(body)
  end
end
