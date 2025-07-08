defmodule Sequin.Sinks.Typesense.Client do
  @moduledoc """
  Client for interacting with the Typesense API.
  """

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Trace

  require Logger

  defstruct [:api_key, :url, :timeout_seconds, :req_opts]

  def new(opts) do
    %__MODULE__{
      url: opts |> Keyword.fetch!(:url) |> String.trim_trailing("/"),
      api_key: Keyword.fetch!(opts, :api_key),
      timeout_seconds: Keyword.fetch!(opts, :timeout_seconds),
      req_opts: Keyword.get(opts, :req_opts, [])
    }
  end

  @doc """
  Index a single document in a collection.
  """
  def index_document(%SinkConsumer{} = consumer, %__MODULE__{} = client, collection_name, document) do
    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/collections/#{collection_name}/documents",
        json: document,
        params: %{action: "emplace"}
      )

    case Req.post(req) do
      {:ok, %{status: status, body: body} = resp} when status in 200..299 ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Indexed document to #{collection_name}",
          req_request: req,
          req_response: resp
        })

        {:ok, body}

      {:ok, %{status: status, body: body} = resp} ->
        error_message = extract_error_message(body)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to index document: #{error_message}",
          req_request: req,
          req_response: resp
        })

        {:error,
         Error.service(
           service: :typesense,
           message: "Failed to index document: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to index document: #{Exception.message(error)}")

        error = Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to index document",
          req_request: req,
          error: error
        })

        {:error, error}

      {:error, reason} ->
        error = Error.service(service: :typesense, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to index document",
          req_request: req,
          error: error
        })

        {:error, error}
    end
  end

  @doc """
  Import multiple documents in JSONL format.
  """
  def import_documents(%SinkConsumer{} = consumer, %__MODULE__{} = client, collection_name, jsonl) do
    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/collections/#{collection_name}/documents/import",
        headers: [{"Content-Type", "text/plain"}],
        body: jsonl,
        params: %{action: "emplace"}
      )

    case Req.post(req) do
      {:ok, %{status: 200, body: body} = resp} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Imported documents to #{collection_name}",
          req_request: req,
          req_response: resp
        })

        responses =
          body
          |> String.split("\n", trim: true)
          |> Enum.map(&parse_batch_item_result/1)

        {:ok, responses}

      {:ok, %{status: status, body: body} = resp} ->
        error_message = extract_error_message(body)

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed: #{error_message}",
          req_request: req,
          req_response: resp
        })

        {:error,
         Error.service(
           service: :typesense,
           message: "Batch import failed: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Transport error: #{Exception.message(error)}")
        error = Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed",
          req_request: req,
          error: error
        })

        {:error, Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")}

      {:error, reason} ->
        error = Error.service(service: :typesense, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed",
          req_request: req,
          error: error
        })

        {:error, error}
    end
  end

  @doc """
  Delete a document from a collection.
  """
  def delete_document(%SinkConsumer{} = consumer, %__MODULE__{} = client, collection_name, document_id, opts \\ []) do
    ignore_not_found = Keyword.get(opts, :ignore_not_found, true)

    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/collections/#{collection_name}/documents/#{document_id}",
        params: %{ignore_not_found: ignore_not_found}
      )

    case Req.delete(req) do
      {:ok, %{status: st, body: body} = resp} when st in 200..299 ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Deleted document from #{collection_name}",
          req_request: req,
          req_response: resp
        })

        {:ok, body}

      {:ok, %{status: 404} = resp} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Can't delete: document '#{document_id}' not found",
          req_request: req,
          req_response: resp
        })

        {:error, Error.service(service: :typesense, message: "Can't delete: document '#{document_id}' not found")}

      {:ok, %{status: status, body: body} = resp} ->
        error_message = extract_error_message(body)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete document: [#{status}] #{error_message}",
          req_request: req,
          req_response: resp
        })

        {:error, Error.service(service: :typesense, message: "Failed to delete document: [#{status}] #{error_message}")}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Typesense] Failed to delete document: #{Exception.message(error)}")

        error = Error.service(service: :typesense, message: "Transport error: #{Exception.message(error)}")

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete document",
          req_request: req,
          error: error
        })

        {:error, error}

      {:error, reason} ->
        error = Error.service(service: :typesense, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete document",
          req_request: req,
          error: error
        })

        {:error, error}
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
    [
      base_url: client.url,
      headers: [
        {"X-TYPESENSE-API-KEY", client.api_key}
      ],
      receive_timeout: to_timeout(second: client.timeout_seconds),
      retry: false
    ]
    |> Keyword.merge(client.req_opts)
    |> Req.new()
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

  defp parse_batch_item_result(json_line) do
    res = Jason.decode!(json_line)

    if Map.get(res, "success", false) do
      :ok
    else
      {:error, extract_error_message(res)}
    end
  end
end
