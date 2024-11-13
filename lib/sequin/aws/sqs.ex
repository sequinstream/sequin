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

    attrs =
      if is_fifo? do
        %{
          "QueueName" => queue_name,
          "Attribute.1.Name" => "FifoQueue",
          "Attribute.1.Value" => true,
          "Attribute.2.Name" => "Policy",
          "Attribute.2.Value" => Jason.encode!(queue_policy),
          "Attribute.3.Name" => "ContentBasedDeduplication",
          "Attribute.3.Value" => true
        }
      else
        # Fifo: false breaks AWS
        %{
          "QueueName" => queue_name,
          "Attribute.1.Name" => "Policy",
          "Attribute.1.Value" => Jason.encode!(queue_policy)
        }
      end

    res = AWS.SQS.create_queue(client, attrs)

    with {:ok, %{"CreateQueueResponse" => %{"CreateQueueResult" => %{"QueueUrl" => queue_url}}}, %{status_code: 200}} <-
           res do
      {:ok, queue_url}
    end
  end

  @spec get_queue_url(Client.t(), String.t(), String.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def get_queue_url(client, account_id, queue_name) do
    res =
      AWS.SQS.get_queue_url(client, %{
        "QueueName" => queue_name,
        "QueueOwnerAWSAccountId" => account_id
      })

    case res do
      {:ok, %{"GetQueueUrlResponse" => %{"GetQueueUrlResult" => %{"QueueUrl" => queue_url}}}, _body} ->
        {:ok, queue_url}

      err ->
        {:error, Error.service(service: :aws_sqs, message: "Failed to get queue URL", details: err)}
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
    wrapped_messages =
      Enum.map(messages, fn msg ->
        attributes =
          msg
          |> Map.get(:attributes, [])
          |> Enum.map(fn {k, v} ->
            %{
              "Name" => to_string(k),
              "Value.StringValue" => to_string(v),
              "Value.DataType" => "String"
            }
          end)

        attributes = to_aws_object_array("MessageAttribute", attributes)

        %{
          "Id" => Map.fetch!(msg, :id),
          "MessageBody" => Jason.encode!(msg.message_body)
        }
        |> Map.merge(attributes)
        |> Sequin.Map.put_if_present("MessageGroupId", msg[:message_group_id])
        |> Sequin.Map.put_if_present("MessageDeduplicationId", msg[:message_deduplication_id])
      end)

    wrapped_messages =
      "SendMessageBatchRequestEntry"
      |> to_aws_object_array(wrapped_messages)
      |> Map.put("QueueUrl", queue_url)

    case AWS.SQS.send_message_batch(client, wrapped_messages) do
      {:ok,
       %{
         "SendMessageBatchResponse" => %{
           "SendMessageBatchResult" => %{"BatchResultErrorEntry" => _}
         }
       } = resp, %{body: body, status_code: 200}} ->
        {:error, resp, %{body: body}}

      {:ok,
       %{
         "SendMessageBatchResponse" => %{
           "SendMessageBatchResult" => %{"SendMessageBatchResultEntry" => _}
         }
       }, %{body: _body, status_code: 200}} ->
        :ok

      err ->
        err
    end
  end

  @spec fetch_messages(Client.t(), String.t(), Keyword.t()) ::
          {:ok, list(message())} | {:error, Error.t()}
  def fetch_messages(%Client{} = client, queue_url, opts) do
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
      {:ok, %{"ReceiveMessageResponse" => %{"ReceiveMessageResult" => message_result}}, _} ->
        messages = extract_messages(message_result)
        {:ok, messages}

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

  @spec delete_queue(Client.t(), String.t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_queue(%Client{} = client, account_id, queue_name) do
    case get_queue_url(client, account_id, queue_name) do
      {:error, {:unexpected_response, %{body: body, status_code: 400}}} ->
        if String.contains?(body, "The specified queue does not exist") do
          {:error, Error.not_found(entity: :sqs_queue)}
        else
          {:error, Error.service(service: :aws_sqs, message: "Failed to delete queue", code: "400")}
        end

      {:ok, queue_url} ->
        with {:ok, _, %{status_code: 200}} <-
               AWS.SQS.delete_queue(client, %{"QueueUrl" => queue_url}) do
          :ok
        end
    end
  end

  defp extract_messages(:none), do: []

  defp extract_messages(%{"Message" => messages}) do
    # if there's only one message, then `messages` is an object, so wrap it
    messages
    |> List.wrap()
    |> Enum.map(fn msg ->
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

  def delete_messages(_client, _queue_url, []) do
    :ok
  end

  def delete_messages(%Client{} = client, queue_url, messages) do
    req_body =
      messages
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn {message, idx}, acc ->
        Map.merge(acc, %{
          # AWS starts index at 1 not 0
          "DeleteMessageBatchRequestEntry.#{idx + 1}.Id" => message.id,
          "DeleteMessageBatchRequestEntry.#{idx + 1}.ReceiptHandle" => message.receipt_handle
        })
      end)

    res = AWS.SQS.delete_message_batch(client, Map.put(req_body, "QueueUrl", queue_url))

    with {:ok,
          %{
            "DeleteMessageBatchResponse" => %{
              "DeleteMessageBatchResult" => %{
                "DeleteMessageBatchResultEntry" => results
              }
            }
          }, _} <- res do
      # AWS will not return a list if only one message was deleted!

      results = List.wrap(results)
      # Ghola.Statsd.increment("aws.sqs.messages_deleted", length(results))

      unless length(results) == length(messages) do
        Logger.error("Did not delete messages for all handles: #{inspect(res)}")
      end

      :ok
    end
  end

  def get_queue_arn(client, queue_url) do
    res =
      AWS.SQS.get_queue_attributes(
        client,
        %{
          "QueueUrl" => queue_url,
          "AttributeName.1" => "QueueArn"
        }
      )

    case res do
      {:ok,
       %{
         "GetQueueAttributesResponse" => %{
           "GetQueueAttributesResult" => %{
             "Attribute" => %{"Name" => "QueueArn", "Value" => queue_arn}
           }
         }
       }, _response} ->
        {:ok, queue_arn}

      err ->
        {:error, err}
    end
  end

  def queue_meta(%Client{} = client, queue_url) do
    res =
      AWS.SQS.get_queue_attributes(client, %{
        "QueueUrl" => queue_url,
        "AttributeName.1" => "ApproximateNumberOfMessages",
        "AttributeName.2" => "ApproximateNumberOfMessagesNotVisible",
        "AttributeName.3" => "QueueArn"
      })

    with {:ok, body, %{status_code: 200}} <- res do
      %{
        "GetQueueAttributesResponse" => %{
          "GetQueueAttributesResult" => %{
            "Attribute" => [
              %{"Name" => "ApproximateNumberOfMessages", "Value" => num_msgs},
              %{"Name" => "ApproximateNumberOfMessagesNotVisible", "Value" => num_not_vis},
              %{"Name" => "QueueArn", "Value" => arn}
            ]
          }
        }
      } = body

      {:ok,
       %{
         messages: String.to_integer(num_msgs),
         not_visible: String.to_integer(num_not_vis),
         arn: arn
       }}
    end
  end

  def has_more?(client, queue_url) do
    case queue_meta(client, queue_url) do
      {:ok, %{messages: 0, not_visible: 0}} -> {:ok, false}
      {:ok, _} -> {:ok, true}
      err -> err
    end
  end

  # Produces weird AWS object-based lists:
  # SendMessageBatchRequestEntry.1.Id: test_msg_no_message_timer
  # SendMessageBatchRequestEntry.1.MessageBody: test%20message%20body%201
  # SendMessageBatchRequestEntry.2.Id: test_msg_delay_45_seconds
  # SendMessageBatchRequestEntry.2.MessageBody: test%20message%20body%202
  # SendMessageBatchRequestEntry.2.DelaySeconds: 45
  # SendMessageBatchRequestEntry.3.Id: test_msg_delay_2_minutes
  # SendMessageBatchRequestEntry.3.MessageBody: test%20message%20body%203
  # SendMessageBatchRequestEntry.3.DelaySeconds: 120
  defp to_aws_object_array(prefix, objects) do
    objects
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {object, idx}, acc ->
      # Index starts at 1 not 0
      index = idx + 1

      object =
        Map.new(object, fn {k, v} -> {"#{prefix}.#{index}.#{k}", v} end)

      Map.merge(acc, object)
    end)
  end
end
