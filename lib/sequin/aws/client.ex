defmodule Sequin.Aws.Client do
  @moduledoc """
  AWS client implementation with credential resolution using the aws_credentials library.

  This module provides functionality to automatically discover and use AWS credentials
  from the ECS task role environment, including:
  - OS environment variables
  - AWS credentials file
  - ECS task credentials
  - EC2 metadata
  """

  @behaviour Sequin.Aws

  alias Sequin.Error

  require Logger

  @impl Sequin.Aws
  def get_client(region) when is_binary(region) do
    case get_credentials() do
      {:ok, credentials} ->
        client = build_client(credentials, region)
        {:ok, client}

      {:error, reason} ->
        Logger.error("Failed to get task role credentials: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp get_credentials do
    case :aws_credentials.get_credentials() do
      :undefined ->
        {:error,
         Error.service(
           service: :aws,
           message:
             "Task role credentials not found. Ensure AWS credentials are available via environment variables, credentials file, ECS task role, web identity token, or EC2 metadata."
         )}

      credentials when is_map(credentials) ->
        {:ok, credentials}

      other ->
        {:error, Error.service(service: :aws, message: "Unexpected credential format: #{inspect(other)}")}
    end
  end

  defp build_client(credentials, region) do
    access_key_id = Map.get(credentials, :access_key_id)
    secret_access_key = Map.get(credentials, :secret_access_key)
    token = Map.get(credentials, :token)

    client = AWS.Client.create(access_key_id, secret_access_key, region)

    # Add session token if present (for temporary credentials)
    if token do
      Map.put(client, :session_token, token)
    else
      client
    end
  end
end
