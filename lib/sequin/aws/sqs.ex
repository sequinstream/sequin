defmodule Sequin.Aws.SQS do
  @moduledoc false

  alias AWS.Client
  alias Sequin.Aws.QueuePolicy
  alias Sequin.Error

  require Logger

  @typep message :: %{
           id: String.t(),
           receipt_handle: String.t(),
           message_body: String.t()
         }

  @doc """
  Call with the desired queue name and QueuePolicy. Returns {:ok, %{arn: queue_arn, url: queue_url}}.
  """
  @spec create_queue(Client.t(), String.t(), QueuePolicy.t(), keyword()) ::
          {:ok, map()} | {:error, any()}
  def create_queue(client, queue_name, %QueuePolicy{} = queue_policy, opts) do
    is_fifo? = Keyword.get(opts, :fifo, false)

    attributes =
      if is_fifo? do
        %{
          "FifoQueue" => "true",
          "Policy" => Jason.encode!(queue_policy),
          "ContentBasedDeduplication" => "true"
        }
      else
        %{
          "Policy" => Jason.encode!(queue_policy)
        }
      end

    request_body = %{
      "QueueName" => queue_name,
      "Attributes" => attributes
    }

    case AWS.SQS.create_queue(client, request_body) do
      {:ok, %{"QueueUrl" => queue_url}, %{status_code: 200}} ->
        {:ok, queue_url}

      err ->
        {:error, err}
    end
  end

  @spec get_queue_url(Client.t(), String.t(), String.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def get_queue_url(client, account_id, queue_name) do
    case AWS.SQS.get_queue_url(client, %{
           "QueueName" => queue_name,
           "QueueOwnerAWSAccountId" => account_id
         }) do
      {:ok, %{"QueueUrl" => queue_url}, _body} ->
        {:ok, queue_url}

      {:error, {:unexpected_response, %{body: body}}} ->
        if is_binary(body) and String.contains?(body, "The specified queue does not exist") do
          {:error, Error.not_found(entity: :sqs_queue)}
        else
          {:error, Error.service(service: :aws_sqs, message: "Failed to get queue URL", details: inspect(body))}
        end

      {:error, error} ->
        {:error, Error.service(service: :aws_sqs, message: "Failed to get queue URL", details: error)}
    end
  end

  @doc """
  Send a batch of messages to the queue. Returns :ok on success

  `messages` is a list of structs. Those structs should take the following form:
    - `id` (optional): String.t(),
    - `body` (required): map(),
    - `attributes` (optional): map(),
    - `message_group_id` (optional): String.t(),
    - `message_deduplication_id` (optional): String.t()
  """
  @spec send_messages(Client.t(), String.t(), list(map())) :: :ok | {:error, any()}
  def send_messages(%Client{} = client, queue_url, messages) do
    entries =
      Enum.map(messages, fn msg ->
        attributes =
          msg
          |> Map.get(:attributes, [])
          |> Map.new(fn {k, v} ->
            {to_string(k),
             %{
               "DataType" => "String",
               "StringValue" => to_string(v)
             }}
          end)

        %{
          "Id" => Map.fetch!(msg, :id),
          "MessageBody" => Jason.encode!(msg.message_body)
        }
        |> Sequin.Map.put_if_present("MessageAttributes", attributes)
        |> Sequin.Map.put_if_present("MessageGroupId", msg[:message_group_id])
        |> Sequin.Map.put_if_present("MessageDeduplicationId", msg[:message_deduplication_id])
      end)

    request_body = %{
      "QueueUrl" => queue_url,
      "Entries" => entries
    }

    case AWS.SQS.send_message_batch(client, request_body) do
      {:ok,
       %{
         "Failed" => failed_entries
       } = resp, %{body: body}}
      when failed_entries != [] ->
        {:error, resp, %{body: body}}

      {:ok, %{"Successful" => _successful}, %{body: _body}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        handle_unexpected_response(details)
    end
  end

  @spec receive_messages(Client.t(), String.t(), Keyword.t()) ::
          {:ok, list(message())} | {:error, Error.t()}
  def receive_messages(%Client{} = client, queue_url, opts \\ []) do
    res =
      AWS.SQS.receive_message(
        client,
        %{
          "QueueUrl" => queue_url,
          "WaitTimeSeconds" => Keyword.get(opts, :wait_time_s, 1),
          "VisibilityTimeout" => Keyword.get(opts, :visibility_timeout_s, 360),
          "MaxNumberOfMessages" => Keyword.get(opts, :max_messages, 3)
        }
      )

    case res do
      {:ok, %{"Messages" => messages}, _} ->
        {:ok, extract_messages(messages)}

      {:ok, response, _} when map_size(response) == 0 ->
        # No messages available
        {:ok, []}

      {:error,
       {:unexpected_response,
        %{
          body: body,
          status_code: 400
        }}} = err ->
        if String.contains?(body, "AWS.SimpleQueueService.NonExistentQueue") do
          {:error, Error.not_found(entity: :sqs_queue)}
        else
          {:error, Error.service(service: :aws_sqs, message: "Failed to fetch messages", details: err)}
        end

      err ->
        {:error, Error.service(service: :aws_sqs, message: "Failed to fetch messages", details: err)}
    end
  end

  defp extract_messages(messages) when is_list(messages) do
    Enum.map(messages, fn msg ->
      %{
        "MessageId" => id,
        "ReceiptHandle" => handle,
        "Body" => body
      } = msg

      %{
        id: id,
        receipt_handle: handle,
        message_body: Jason.decode!(body)
      }
    end)
  end

  @spec delete_queue(Client.t(), String.t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_queue(%Client{} = client, account_id, queue_name) do
    case get_queue_url(client, account_id, queue_name) do
      {:ok, queue_url} ->
        with {:ok, _, %{status_code: 200}} <- AWS.SQS.delete_queue(client, %{"QueueUrl" => queue_url}) do
          :ok
        end

      {:error, %Error.NotFoundError{} = error} ->
        {:error, error}

      {:error, error} ->
        {:error, Error.service(service: :aws_sqs, message: "Failed to delete queue", details: error)}
    end
  end

  def delete_messages(_client, _queue_url, []) do
    :ok
  end

  def delete_messages(%Client{} = client, queue_url, messages) do
    entries =
      Enum.map(messages, fn message -> %{"Id" => message.id, "ReceiptHandle" => message.receipt_handle} end)

    request_body = %{
      "QueueUrl" => queue_url,
      "Entries" => entries
    }

    case AWS.SQS.delete_message_batch(client, request_body) do
      {:ok, %{"Failed" => failed, "Successful" => successful}, _} ->
        if length(failed) > 0 do
          Logger.error("Failed to delete some messages: #{inspect(failed)}")
        end

        # AWS will return a list for multiple items but an object for single items
        successful = List.wrap(successful)

        if length(successful) != length(messages) do
          Logger.error("Did not delete messages for all handles")
        end

        :ok

      err ->
        Logger.error("Failed to delete messages: #{inspect(err)}")
        {:error, err}
    end
  end

  def get_queue_arn(client, queue_url) do
    case AWS.SQS.get_queue_attributes(
           client,
           %{
             "QueueUrl" => queue_url,
             "AttributeNames" => ["QueueArn"]
           }
         ) do
      {:ok, %{"Attributes" => %{"QueueArn" => queue_arn}}, _response} ->
        {:ok, queue_arn}

      err ->
        {:error, err}
    end
  end

  @doc """
  Test SQS credentials and basic permissions without requiring a specific queue.
  This is useful for dynamic routing scenarios where the queue URL is not known at configuration time.
  """
  @spec test_credentials_and_permissions(Client.t()) :: :ok | {:error, Error.t()}
  def test_credentials_and_permissions(%Client{} = client) do
    case AWS.SQS.list_queues(client, %{}) do
      {:ok, _response, %{status_code: 200}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        Logger.debug("[SQS] Failed to list queues for credential test: #{inspect(details)}")
        handle_unexpected_response(details)

      {:error, error} ->
        Logger.debug("[SQS] Failed to list queues for credential test: #{inspect(error)}")

        {:error,
         Error.service(service: :aws_sqs, message: "Failed to test SQS credentials and permissions", details: error)}
    end
  end

  @spec queue_meta(Client.t(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def queue_meta(%Client{} = client, queue_url) do
    case AWS.SQS.get_queue_attributes(
           client,
           %{
             "QueueUrl" => queue_url,
             "AttributeNames" => [
               "ApproximateNumberOfMessages",
               "ApproximateNumberOfMessagesNotVisible",
               "QueueArn"
             ]
           }
         ) do
      {:ok, %{"Attributes" => attributes}, %{status_code: 200}} ->
        {:ok,
         %{
           messages: String.to_integer(attributes["ApproximateNumberOfMessages"]),
           not_visible: String.to_integer(attributes["ApproximateNumberOfMessagesNotVisible"]),
           arn: attributes["QueueArn"]
         }}

      {:error, {:unexpected_response, details}} ->
        Logger.debug("[SQS] Failed to get queue attributes: #{inspect(details)}")
        handle_unexpected_response(details)

      {:error, error} ->
        Logger.debug("[SQS] Failed to get queue attributes: #{inspect(error)}")
        {:error, Error.service(service: :aws_sqs, message: "Failed to get queue attributes", details: error)}
    end
  end

  def has_more?(client, queue_url) do
    case queue_meta(client, queue_url) do
      {:ok, %{messages: 0, not_visible: 0}} -> {:ok, false}
      {:ok, _} -> {:ok, true}
      err -> err
    end
  end

  defp handle_unexpected_response(%{body: body, status_code: status_code}) do
    case Jason.decode(body) do
      # Handle standard AWS SQS error format with __type and message fields
      {:ok, %{"__type" => "com.amazonaws.sqs#BatchRequestTooLong", "message" => error_message}} ->
        {:error,
         Error.service(
           service: :aws_sqs,
           code: :batch_request_too_long,
           message: "SQS batch request exceeds the maximum allowed size: #{error_message}",
           details: %{status_code: status_code, body: body}
         )}

      {:ok, %{"__type" => "com.amazonaws.sqs#" <> error_type, "message" => error_message}} ->
        {:error,
         Error.service(
           service: :aws_sqs,
           code: error_type,
           message: "SQS error: #{error_message} (status=#{status_code})",
           details: %{status_code: status_code, body: body}
         )}

      # Fallback for other JSON formats with just a message field
      {:ok, %{"message" => message}} ->
        {:error,
         Error.service(
           service: :aws_sqs,
           code: :unknown_error,
           message: "Error from AWS SQS: #{message} (status=#{status_code})",
           details: %{status_code: status_code, body: body}
         )}

      # Fallback for non-parseable or unexpected formats
      _ ->
        message =
          if is_binary(body) do
            body
          else
            inspect(body)
          end

        {:error,
         Error.service(
           service: :aws_sqs,
           code: :unknown_error,
           message: "Error from AWS SQS: #{message} (status=#{status_code})",
           details: %{status_code: status_code, body: body}
         )}
    end
  end
end
