defmodule Sequin.Aws.Kinesis do
  @moduledoc false

  alias AWS.Client
  alias Sequin.Error

  require Logger

  @spec test_credentials_and_permissions(Client.t()) :: :ok | {:error, Error.t()}
  def test_credentials_and_permissions(%Client{} = client) do
    case AWS.Kinesis.list_streams(client, %{}) do
      {:ok, _response, %{status_code: 200}} ->
        :ok

      {:error, {:unexpected_response, details}} ->
        Logger.debug("[Kinesis] Failed to list streams for credential test: #{inspect(details)}")
        handle_unexpected_response(details)

      {:error, error} ->
        Logger.debug("[Kinesis] Failed to list streams for credential test: #{inspect(error)}")

        {:error,
         Error.service(
           service: :aws_kinesis,
           message: "Failed to test Kinesis credentials and permissions",
           details: error
         )}
    end
  end

  def put_records(%Client{} = client, stream_arn, records) when is_list(records) do
    request_body = %{
      "StreamARN" => stream_arn,
      "Records" => records
    }

    case AWS.Kinesis.put_records(client, request_body) do
      {:ok, %{"FailedRecordCount" => 0}, _} ->
        :ok

      {:ok, resp, %{body: _body}} ->
        message =
          case Map.get(resp, "FailedRecordCount") do
            nil ->
              "Failed to put records to Kinesis stream"

            failed_count ->
              "Failed to put #{failed_count} records to Kinesis stream"
          end

        {:error, Error.service(service: :aws_kinesis, message: message, details: resp)}

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
