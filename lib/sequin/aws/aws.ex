defmodule Sequin.Aws do
  @moduledoc """
  Handles AWS related utilities and client creation.

  This module provides a callback-based interface for AWS operations,
  similar to Sequin.Sinks.Redis, allowing for easy mocking and testing.
  """

  @callback get_client(String.t()) :: {:ok, AWS.Client.t()} | {:error, Sequin.Error.t()}

  @doc """
  Gets an AWS client configured with task role credentials.

  Returns `{:ok, client}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> get_client("us-west-2")
      {:ok, %AWS.Client{}}

      iex> get_client("invalid-region")
      {:error, "No credentials available"}
  """
  @spec get_client(String.t()) :: {:ok, AWS.Client.t()} | {:error, Sequin.Error.t()}
  def get_client(region) when is_binary(region) do
    impl().get_client(region)
  end

  @doc """
  Generates a message group ID hash for AWS SQS FIFO queues.
  """
  def message_group_id(group_id), do: hash(group_id)

  @doc """
  Generates a message deduplication ID hash for AWS SQS FIFO queues.
  """
  def message_deduplication_id(idempotency_key), do: hash(idempotency_key)

  @doc """
  Gets an AWS client based on sink configuration.

  If sink uses task role credentials, it uses the task role.
  Otherwise, it creates a client with explicit credentials.

  ## Examples

      iex> get_aws_client(%{use_task_role: true, region: "us-west-2"})
      {:ok, %AWS.Client{}}

      iex> get_aws_client(%{use_task_role: false, access_key_id: "key", secret_access_key: "secret", region: "us-west-2"})
      {:ok, %AWS.Client{}}
  """
  @spec get_aws_client(map()) :: {:ok, AWS.Client.t()} | {:error, Error.t()}
  def get_aws_client(sink) when is_map(sink) do
    if sink.use_task_role do
      # Use task role credentials
      get_client(sink.region)
    else
      # Use explicit credentials
      {:ok, AWS.Client.create(sink.access_key_id, sink.secret_access_key, sink.region)}
    end
  end

  defp hash(value) do
    :md5
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower, padding: false)
  end

  defp impl do
    Application.get_env(:sequin, :aws_module, Sequin.Aws.Client)
  end
end
