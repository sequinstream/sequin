defmodule Sequin.Sinks.Meilisearch.Client do
  @moduledoc """
  Client for interacting with the Meilisearch API.
  """

  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Error

  require Logger

  defp verify_task_by_id(%MeilisearchSink{} = sink, task_id, retries) do
    if retries > 10 do
      {:error,
       Error.service(
         service: :meilisearch,
         message: "Task verification timed out",
         details: %{task_id: task_id}
       )}
    else
      req = base_request(sink)

      case Req.get(req, url: "/tasks/#{task_id}") do
        {:ok, %{status: status, body: body}} when status in 200..299 ->
          case body do
            %{"status" => status} when status in ["enqueued", "processing"] ->
              timeout = Sequin.Time.exponential_backoff(200, retries, 10_000)
              Logger.warning("[Meilisearch] Task #{task_id} is still in progress (#{retries}/10)")
              :timer.sleep(timeout)
              verify_task_by_id(sink, task_id, retries + 1)

            %{"status" => "failed"} ->
              message = extract_error_message(body["error"])

              {:error,
               Error.service(
                 service: :meilisearch,
                 message: message,
                 details: body
               )}

            _ ->
              :ok
          end

        {:ok, %{status: status, body: body}} ->
          message = extract_error_message(body) || "Request failed with status #{status}"

          {:error,
           Error.service(
             service: :meilisearch,
             message: message,
             details: %{status: status, body: body}
           )}

        {:error, reason} ->
          {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
      end
    end
  end

  @doc """
  Import multiple documents in JSONL format.
  """
  def import_documents(%MeilisearchSink{} = sink, index_name, records) do
    jsonl = Enum.map_join(records, "\n", &Jason.encode!/1)

    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents",
        headers: [{"Content-Type", "application/x-ndjson"}],
        body: jsonl
      )

    case Req.put(req) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        verify_task_by_id(sink, body["taskUid"], 0)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
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
  Delete documents from an index.
  """
  def delete_documents(%MeilisearchSink{} = sink, index_name, document_ids) do
    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/delete-batch",
        body: Jason.encode!(document_ids),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        verify_task_by_id(sink, body["taskUid"], 0)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
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
  Update documents using a function expression.
  """
  def update_documents_with_function(%MeilisearchSink{} = sink, index_name, filter, function, context \\ %{}) do
    body = %{
      "filter" => filter,
      "function" => function
    }

    body = if map_size(context) > 0, do: Map.put(body, "context", context), else: body

    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/edit",
        body: Jason.encode!(body),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        verify_task_by_id(sink, body["taskUid"], 0)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
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
  Get information about an index.
  """
  def get_index(%MeilisearchSink{} = sink, index_name) do
    req = base_request(sink)

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
  def test_connection(%MeilisearchSink{} = sink) do
    req = base_request(sink)

    case Req.get(req, url: "/health") do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: status, body: body}} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Health check failed with status #{status}",
           details: %{status: status, body: body}
         )}

      {:error, reason} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Cannot connect to Meilisearch",
           details: reason
         )}
    end
  end

  def maybe_verify_index(%MeilisearchSink{index_name: nil}, _index_name, _primary_key) do
    :ok
  end

  def maybe_verify_index(%MeilisearchSink{} = sink, index_name, primary_key) do
    case get_index(sink, index_name) do
      {:ok, ^primary_key} ->
        :ok

      {:ok, other_primary_key} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: ~s(Index verification failed. Expected primary key "#{primary_key}", got "#{other_primary_key}")
         )}

      {:error, error} ->
        {:error, Error.service(service: :meilisearch, message: "Index verification failed", details: error)}
    end
  end

  # Private helpers

  defp default_req_opts do
    Application.get_env(:sequin, :meilisearch, [])[:req_opts] || []
  end

  defp base_request(%MeilisearchSink{} = sink) do
    [
      base_url: String.trim_trailing(sink.endpoint_url, "/"),
      headers: [{"Authorization", "Bearer #{sink.api_key}"}],
      receive_timeout: to_timeout(second: sink.timeout_seconds),
      retry: false,
      compress_body: true
    ]
    |> Req.new()
    |> Req.merge(default_req_opts())
  end

  defp extract_error_message(error) do
    cond do
      is_binary(error["message"]) -> error["message"]
      is_binary(error["code"]) -> error["code"]
      true -> nil
    end
  end
end
