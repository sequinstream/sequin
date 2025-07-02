defmodule Sequin.Aws.SNS do
  @moduledoc false

  alias AWS.Client
  alias Sequin.Error

  require Logger

  def create_topic(client, topic_name, opts) do
    is_fifo? = Keyword.get(opts, :fifo, false)

    attributes =
      if is_fifo? do
        %{
          "FifoTopic" => "true",
          "ContentBasedDeduplication" => "true"
        }
      else
        %{}
      end

    request_body = %{
      "Name" => topic_name,
      "Attributes" => attributes
    }

    case AWS.SNS.create_topic(client, request_body) do
      {:ok, %{"TopicArn" => topic_arn}, %{status_code: 200}} ->
        {:ok, topic_arn}

      err ->
        {:error, err}
    end
  end

  def publish_messages(%Client{} = client, topic_arn, messages) do
    entries =
      if fifo?(topic_arn) do
        Enum.map(messages, fn msg ->
          %{
            "Id" => Map.fetch!(msg, :message_id),
            "Message" => Jason.encode!(msg.message)
          }
          |> Sequin.Map.put_if_present("MessageGroupId", msg[:message_group_id])
          |> Sequin.Map.put_if_present("MessageDeduplicationId", msg[:message_deduplication_id])
        end)
      else
        Enum.map(messages, fn msg ->
          %{
            "Id" => Map.fetch!(msg, :message_id),
            "Message" => Jason.encode!(msg.message)
          }
        end)
      end

    request_body = %{
      "TopicArn" => topic_arn,
      "PublishBatchRequestEntries" => %{"member" => entries}
    }

    case AWS.SNS.publish_batch(client, request_body) do
      {:ok, %{"PublishBatchResponse" => %{"PublishBatchResult" => %{"Failed" => :none}}}, _} ->
        :ok

      {:ok, resp, %{body: body}} ->
        {:error, resp, %{body: body}}

      {:error, {:unexpected_response, details}} ->
        handle_unexpected_response(details)
    end
  end

  def delete_topic(%Client{} = client, topic_arn) do
    case AWS.SNS.delete_topic(client, %{"TopicArn" => topic_arn}) do
      {:ok, _, %{status_code: 200}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        handle_unexpected_response(details)

      {:error, error} ->
        {:error, Error.service(service: :aws_sns, message: "Failed to delete topic", details: error)}
    end
  end

  @spec test_credentials_and_permissions(Client.t()) :: :ok | {:error, Error.t()}
  def test_credentials_and_permissions(%Client{} = client) do
    case AWS.SNS.list_topics(client, %{}) do
      {:ok, _response, %{status_code: 200}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        Logger.debug("[SNS] Failed to list topics for credential test: #{inspect(details)}")
        handle_unexpected_response(details)

      {:error, error} ->
        Logger.debug("[SNS] Failed to list topics for credential test: #{inspect(error)}")

        {:error,
         Error.service(service: :aws_sns, message: "Failed to test SNS credentials and permissions", details: error)}
    end
  end

  @spec topic_meta(Client.t(), String.t()) :: :ok | {:error, Error.t()}
  def topic_meta(%Client{} = client, topic_arn) do
    case AWS.SNS.get_topic_attributes(client, %{"TopicArn" => topic_arn}) do
      {:ok, _resp, %{status_code: 200}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        Logger.debug("[SNS] Failed to get topic attributes: #{inspect(details)}")
        handle_unexpected_response(details)

      {:error, error} ->
        Logger.debug("[SNS] Failed to get topic attributes: #{inspect(error)}")
        {:error, Error.service(service: :aws_sns, message: "Failed to get topic attributes", details: error)}
    end
  end

  defp handle_unexpected_response(%{body: body, status_code: status_code}) do
    message =
      case Jason.decode(body) do
        {:ok, %{"message" => message}} ->
          message

        _ ->
          if is_binary(body) do
            body
          else
            inspect(body)
          end
      end

    {:error,
     Error.service(service: :aws_sns, message: "Error from AWS: #{message} (status=#{status_code})", details: message)}
  end

  defp fifo?(topic_arn) do
    String.ends_with?(topic_arn, ".fifo")
  end
end
