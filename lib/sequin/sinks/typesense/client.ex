defmodule Sequin.Sinks.Typesense.Client do
  @moduledoc """
  Client for interacting with the Typesense API.
  """

  alias Sequin.Error

  require Logger

  defstruct [:api_key, :url, :timeout_seconds, req_client: Req]

  def new(opts) do
    %__MODULE__{
      url: opts |> Keyword.fetch!(:url) |> String.trim_trailing("/"),
      api_key: Keyword.fetch!(opts, :api_key),
      timeout_seconds: Keyword.fetch!(opts, :timeout_seconds)
    }
  end

  @doc """
  Index a single document in a collection.
  """
  def index_document(%__MODULE__{} = client, collection_name, document, opts \\ []) do
    action = Keyword.get(opts, :action, "emplace")
    req = base_request(client)

    url = "/collections/#{collection_name}/documents"

    case Req.post(req, url: url, json: document, params: %{action: action}) do
      {:ok, %{status: st, body: body}} when st in 200..299 ->
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
    action = Keyword.get(opts, :action, "emplace")
    req = base_request(client)

    query_params = %{
      "action" => action
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

        if Enum.all?(responses, &Map.get(&1, "success", false)) do
          {:ok, responses}
        else
          msg = extract_error_message(responses)
          {:error, Error.service(service: :typesense, message: "Batch import failed: #{msg}")}
        end

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Batch import failed: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Transport error: #{Exception.message(error)}")
        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        {:error, Error.service(service: :typesense, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Delete a document from a collection.
  """
  def delete_document(%__MODULE__{} = client, collection_name, document_id, opts \\ []) do
    ignore_not_found = Keyword.get(opts, :ignore_not_found, true)
    req = base_request(client)

    url = "/collections/#{collection_name}/documents/#{document_id}"

    case Req.delete(req, url: url, params: %{ignore_not_found: ignore_not_found}) do
      {:ok, %{status: st, body: body}} when st in 200..299 ->
        {:ok, body}

      {:ok, %{status: 404}} ->
        {:error, Error.service(service: :typesense, message: "Can't delete: document '#{document_id}' not found")}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to delete document: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to delete document: #{Exception.message(error)}")
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
      {:ok, %{status: st, body: %{"ok" => true}}} when st in 200..299 ->
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
      {:ok, %{status: st, body: body}} when st in 200..299 ->
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
  def get_collection(%__MODULE__{} = client, collection_name) do
    req = base_request(client)

    case Req.get(req, url: "/collections/#{collection_name}") do
      {:ok, %{status: st, body: body}} when st in 200..299 ->
        {:ok, body}

      {:ok, %{status: 404}} ->
        {:error, Error.service(service: :typesense, message: "Collection '#{collection_name}' not found")}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to get collection: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to get collection: #{Exception.message(error)}")
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
