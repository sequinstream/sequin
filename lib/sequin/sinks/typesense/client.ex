defmodule Sequin.Sinks.Typesense.Client do
  @moduledoc """
  Client for interacting with the Typesense API.
  """

  alias Sequin.Error

  require Logger

  @type t :: %__MODULE__{
          api_key: String.t(),
          url: String.t(),
          req_client: module()
        }

  defstruct [:api_key, :url, req_client: Req]

  @doc """
  Creates a new Typesense client.
  """
  @spec new(String.t(), String.t()) :: t()
  def new(url, api_key) do
    %__MODULE__{
      url: String.trim_trailing(url, "/"),
      api_key: api_key
    }
  end

  @doc """
  Index a single document in a collection.
  """
  @spec index_document(t(), String.t(), map()) :: {:ok, map()} | {:error, Error.t()}
  def index_document(%__MODULE__{} = client, collection_name, document) do
    req = base_request(client)

    case Req.post(req, url: "/collections/#{collection_name}/documents", json: document) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
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
  @spec import_documents(t(), String.t(), String.t(), keyword()) :: {:ok, list(map())} | {:error, Error.t()}
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
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        # The response is one JSON object per line, parse each line separately
        responses =
          body
          |> String.split("\n", trim: true)
          |> Enum.map(&Jason.decode!/1)

        # TODO: we can get 200 from the HTTP request but be told about errors here...        
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
  @spec test_connection(t()) :: :ok | {:error, Error.t()}
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
  @spec create_collection(t(), map()) :: {:ok, map()} | {:error, Error.t()}
  def create_collection(%__MODULE__{} = client, schema) do
    req = base_request(client)

    case Req.post(req, url: "/collections", json: schema) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
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
  @spec retrieve_collection(t(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def retrieve_collection(%__MODULE__{} = client, collection_name) do
    req = base_request(client)

    case Req.get(req, url: "/collections/#{collection_name}") do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
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
      receive_timeout: :timer.seconds(30),
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
