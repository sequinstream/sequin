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
      Enum.map(messages, fn msg ->
        %{
          "Id" => Map.fetch!(msg, :message_id),
          "Message" => Jason.encode!(msg.message)
        }
        |> Sequin.Map.put_if_present("MessageGroupId", msg[:message_group_id])
        |> Sequin.Map.put_if_present("MessageDeduplicationId", msg[:message_deduplication_id])
      end)

    request_body = %{
      "TopicArn" => topic_arn,
      "PublishBatchRequestEntries" => entries
    }

    case AWS.SNS.publish_batch(client, request_body) do
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
end
