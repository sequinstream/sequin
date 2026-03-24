defmodule Sequin.Aws.RepoIamAuth do
  @moduledoc """
  Ecto Repo `:configure` callback for RDS IAM authentication.

  When Sequin's internal database is hosted on AWS RDS and IAM auth is enabled
  via `PG_IAM_AUTH=true`, this module generates a fresh RDS IAM auth token
  before each new database connection.
  """

  require Logger

  def configure(region, opts) do
    hostname = Keyword.fetch!(opts, :hostname)
    port = Keyword.fetch!(opts, :port)
    username = Keyword.fetch!(opts, :username)

    with {:ok, client} <- Sequin.Aws.get_client(region),
         credentials = %{
           access_key_id: client.access_key_id,
           secret_access_key: client.secret_access_key,
           token: Map.get(client, :session_token)
         },
         {:ok, token} <- Sequin.Aws.RdsToken.generate(hostname, port, username, credentials, region) do
      {:ok, Keyword.put(opts, :password, token)}
    else
      {:error, reason} ->
        Logger.error("Failed to generate RDS IAM token for Sequin.Repo: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
