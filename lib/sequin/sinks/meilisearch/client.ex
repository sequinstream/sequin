defmodule Sequin.Sinks.Meilisearch.Client do
  @moduledoc """
  Client for interacting with the Meilisearch API.
  """

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Trace

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
  Import multiple documents in JSONL format.
  """
  def import_documents(%SinkConsumer{} = consumer, %__MODULE__{} = client, index_name, jsonl) do
    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents",
        headers: [{"Content-Type", "application/x-ndjson"}],
        body: jsonl,
        params: %{primaryKey: "id"}
      )

    case Req.put(req) do
      {:ok, %{status: st } = resp} when st in 200..299 ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Imported documents to #{index_name}",
          req_request: req,
          req_response: resp
        })

        {:ok}

      {:ok, %{status: status, body: body} = resp} ->
        error_message = extract_error_message(body)

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed: #{error_message}",
          req_request: req,
          req_response: resp
        })

        {:error,
         Error.service(
           service: :meilisearch,
           message: "Batch import failed: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Meilisearch] Transport error: #{Exception.message(error)}")

        err =
          Error.service(
            service: :meilisearch,
            message: "Transport error: #{Exception.message(error)}"
          )

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed",
          req_request: req,
          error: err
        })

        {:error, err}

      {:error, reason} ->
        err = Error.service(service: :meilisearch, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Batch import failed",
          req_request: req,
          error: err
        })

        {:error, err}
    end
  end

  @doc """
  Delete documents from an index.
  """
  def delete_documents(
        %SinkConsumer{} = consumer,
        %__MODULE__{} = client,
        index_name,
        document_ids
      ) do
    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/delete-batch",
        body: Jason.encode!(document_ids),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{status: st, body: body} = resp} when st in 200..299 ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Deleted documents from #{index_name}",
          req_request: req,
          req_response: resp
        })

        {:ok, body}

      {:ok, %{status: status, body: body} = resp} ->
        error_message = extract_error_message(body)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete documents: [#{status}] #{error_message}",
          req_request: req,
          req_response: resp
        })

        {:error,
         Error.service(
           service: :meilisearch,
           message: "Failed to delete document: [#{status}] #{error_message}"
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Meilisearch] Failed to delete documents: #{Exception.message(error)}")

        err =
          Error.service(
            service: :meilisearch,
            message: "Transport error: #{Exception.message(error)}"
          )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete document",
          req_request: req,
          error: err
        })

        {:error, err}

      {:error, reason} ->
        err = Error.service(service: :meilisearch, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete documents",
          req_request: req,
          error: err
        })

        {:error, err}
    end
  end

    @doc """
  Get information about an index.
  """
  def get_index(%__MODULE__{} = client, index_name) do
    req = base_request(client)

    case Req.get(req, url: "/indexes/#{index_name}") do
      {:ok, %{status: st, body: body}} when st in 200..299 ->
        {:ok, body}

      {:ok, %{status: 404}} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Index '#{index_name}' not found"
         )}

      {:ok, %{status: status, body: body}} ->
        error_message = extract_error_message(body)

        {:error,
         Error.service(
           service: :meilisearch,
           message: "Failed to get index: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Meilisearch] Failed to get index: #{Exception.message(error)}")

        {:error,
         Error.service(
           service: :meilisearch,
           message: "Transport error: #{Exception.message(error)}"
         )}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Test the connection to the Meilisearch server.
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
           service: :meilisearch,
           message: "Failed to connect to Meilisearch: #{error_message}",
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Meilisearch] Failed to connect: #{Exception.message(error)}")

        {:error,
         Error.service(
           service: :meilisearch,
           message: "Transport error: #{Exception.message(error)}"
         )}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  # Private helpers

  defp base_request(%__MODULE__{} = client) do
    Req.new(
      base_url: client.url,
      headers: [{"Authorization", "Bearer #{client.api_key}"}],
      receive_timeout: :timer.seconds(client.timeout_seconds),
      retry: false
    )
  end

  defp extract_error_message(body) when is_map(body) do
    cond do
      is_binary(body["message"]) -> body["message"]
      is_binary(body["code"]) -> body["code"]
      true -> Jason.encode!(body)
    end
  end

  defp extract_error_message(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> extract_error_message(decoded)
      {:error, _} -> body
    end
  end

  defp extract_error_message(body), do: inspect(body)
end
