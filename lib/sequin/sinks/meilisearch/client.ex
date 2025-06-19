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
  Verify the status of a task by its ID.
  """
  def verify_task_by_id(%__MODULE__{} = client, task_id, retries) do
    if retries > 10 do
      {:error,
       Error.service(
         service: :meilisearch,
         message: "Task verification timed out",
         details: %{task_id: task_id}
       )}
    else
      req = base_request(client)

      case Req.get(req, url: "/tasks/#{task_id}") do
        {:ok, %{body: body}} ->
          case body do
            %{"status" => status} when status in ["enqueued", "processing"] ->
              timeout = Sequin.Time.exponential_backoff(50, retries, 10_000)
              Logger.warning("[Meilisearch] Task #{task_id} is still in progress (#{retries}/10)")
              :timer.sleep(timeout)
              verify_task_by_id(client, task_id, retries + 1)

            %{"status" => "failed"} ->
              message = extract_error_message(body["error"])

              {:error,
               Error.service(
                 service: :meilisearch,
                 message: message,
                 details: body
               )}

            _ ->
              {:ok}
          end

        {:error, reason} ->
          {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
      end
    end
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
        compress_body: true
      )

    case Req.put(req) do
      {:ok, %{body: body}} ->
        case verify_task_by_id(client, body["taskUid"], 0) do
          {:ok} ->
            Trace.info(consumer.id, %Trace.Event{
              message: "Imported documents to #{index_name}"
            })

            {:ok}

          {:error, err} ->
            Trace.error(consumer.id, %Trace.Event{
              message: "Failed to import documents to #{index_name}",
              req_request: req,
              error: err
            })

            {:error, err}
        end

      {:error, %Req.TransportError{} = error} ->
        Logger.error("[Meilisearch] Failed to import documents: #{Exception.message(error)}")

        err =
          Error.service(
            service: :meilisearch,
            message: "Transport error: #{Exception.message(error)}"
          )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to import documents",
          req_request: req,
          error: err
        })

        {:error, err}

      {:error, reason} ->
        err = Error.service(service: :meilisearch, message: "Unknown error", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to import documents",
          req_request: req,
          error: err
        })

        {:error, err}
    end
  end

  @doc """
  Delete documents from an index.
  """
  def delete_documents(%SinkConsumer{} = consumer, %__MODULE__{} = client, index_name, document_ids) do
    req =
      client
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/delete-batch",
        body: Jason.encode!(document_ids),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{body: body}} ->
        case verify_task_by_id(client, body["taskUid"], 0) do
          {:ok} ->
            Trace.info(consumer.id, %Trace.Event{
              message: "Deleted documents #{document_ids} from #{index_name}"
            })

          {:error, err} ->
            Trace.error(consumer.id, %Trace.Event{
              message: "Failed to delete documents from #{index_name}",
              req_request: req,
              error: err
            })

            {:error, err}
        end

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
      {:ok, %{status: status, body: body}} when status == 200 ->
        {:ok, body["primaryKey"]}

      {:ok, %{body: body}} ->
        message = extract_error_message(body)
        {:error, Error.service(service: :meilisearch, message: message, details: body)}

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
      {:ok, %{status: status}} when status == 200 ->
        :ok

      {:error, reason} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Cannot connect to Meilisearch",
           details: reason
         )}
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

  defp extract_error_message(error) do
    cond do
      is_binary(error["message"]) -> error["message"]
      is_binary(error["code"]) -> error["code"]
      true -> nil
    end
  end
end
