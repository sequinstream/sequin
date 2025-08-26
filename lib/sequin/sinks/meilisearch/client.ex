defmodule Sequin.Sinks.Meilisearch.Client do
  @moduledoc """
  Client for interacting with the Meilisearch API.
  """

  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Error

  require Logger

  defp decode_body(body) when is_map(body), do: body

  defp decode_body(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> %{}
    end
  end

  defp decode_body(_), do: %{}

  defp wait_for_task(%MeilisearchSink{} = sink, task_id) do
    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/tasks/#{task_id}",
        # We need to disable this if we use a custom retry that emits delays
        retry_delay: nil,
        max_retries: 5,
        retry: fn request, response_or_exception ->
          should_retry =
            case response_or_exception do
              %Req.Response{status: 200, body: encoded_body} ->
                # NOTE: Req does not automatically decode on retry functions
                case encoded_body |> :zlib.gunzip() |> Jason.decode() do
                  {:ok, %{"status" => status}} when status in ["enqueued", "processing"] ->
                    true

                  _ ->
                    false
                end

              %Req.Response{status: status} when status in [408, 429, 500, 502, 503, 504] ->
                true

              %Req.TransportError{reason: reason} when reason in [:timeout, :econnrefused, :closed] ->
                true

              _ ->
                false
            end

          if should_retry do
            # Req tracks retry count internally via request.private[:req_retry_count]
            count = request.private[:req_retry_count] || 0
            delay = Sequin.Time.exponential_backoff(200, count, 10_000)

            Logger.debug("[Meilisearch] Task #{task_id} has not succeeded, retrying in #{delay}ms (attempt #{count + 1})")

            {:delay, delay}
          else
            false
          end
        end
      )

    case Req.get(req) do
      {:ok, %{body: body}} ->
        decoded_body = decode_body(body)

        case decoded_body do
          %{"status" => status} when status in ["succeeded", "success"] ->
            :ok

          %{"status" => "failed", "error" => error} ->
            message = extract_error_message(error)
            {:error, Error.service(service: :meilisearch, message: message, details: error)}

          %{"status" => status} when status in ["enqueued", "processing"] ->
            # This means we exhausted retries
            {:error,
             Error.service(
               service: :meilisearch,
               message: "Task verification timed out",
               details: %{task_id: task_id, last_status: status}
             )}

          _ ->
            {:error, Error.service(service: :meilisearch, message: "Invalid response format")}
        end

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
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

    case Req.post(req) do
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

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
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

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
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

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
        decoded_body = decode_body(body)
        {:ok, decoded_body["primaryKey"]}

      {:ok, %{body: body}} ->
        decoded_body = decode_body(body)
        message = extract_error_message(decoded_body)
        {:error, Error.service(service: :meilisearch, message: message, details: decoded_body)}

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
        decoded_body = decode_body(body)
        message = extract_error_message(decoded_body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: decoded_body
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
      retry: :transient,
      retry_delay: fn retry_count ->
        Sequin.Time.exponential_backoff(500, retry_count, 5_000)
      end,
      max_retries: 1,
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
