defmodule Sequin.Aws.Kinesis do
  @moduledoc false

  alias AWS.Client
  alias Sequin.Error

  def put_records(%Client{} = client, stream_arn, records) when is_list(records) do
    request_body = %{
      "StreamARN" => stream_arn,
      "Records" => records
    }

    case AWS.Kinesis.put_records(client, request_body) do
      {:ok, %{"FailedRecordCount" => 0}, _} ->
        :ok

      {:ok, resp, %{body: body}} ->
        {:error, resp, %{body: body}}

      {:error, {:unexpected_response, details}} ->
        handle_unexpected_response(details)

      {:error, error} ->
        {:error, Error.service(service: :aws_kinesis, message: "Failed to put records", details: error)}
    end
  end

  def describe_stream(%Client{} = client, stream_arn) do
    case AWS.Kinesis.describe_stream(client, %{"StreamARN" => stream_arn}) do
      {:ok, resp, _} ->
        {:ok, resp}

      {:error, {:unexpected_response, details}} ->
        handle_unexpected_response(details)

      {:error, error} ->
        {:error, Error.service(service: :aws_kinesis, message: "Failed to get stream info", details: error)}
    end
  end

  defp handle_unexpected_response(%{body: body, status_code: status_code}) do
    message =
      case Jason.decode(body) do
        {:ok, %{"message" => message}} ->
          message

        _ ->
          if is_binary(body), do: body, else: inspect(body)
      end

    {:error,
     Error.service(service: :aws_kinesis, message: "Error from AWS: #{message} (status=#{status_code})", details: message)}
  end
end
