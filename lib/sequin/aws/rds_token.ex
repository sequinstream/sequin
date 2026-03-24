defmodule Sequin.Aws.RdsToken do
  @moduledoc """
  Generates RDS IAM authentication tokens.

  RDS IAM auth tokens are pre-signed URLs using AWS SigV4 that serve as
  short-lived passwords (valid for 15 minutes) for Postgres RDS connections.
  """

  @service "rds-db"
  # 15 minutes, the maximum for RDS IAM tokens
  @ttl 900

  @doc """
  Generates an RDS IAM authentication token for the given connection parameters.

  The token is a pre-signed URL that can be used as a Postgrex password.
  It is valid for 15 minutes.

  ## Parameters

    - `hostname` - The RDS instance hostname
    - `port` - The RDS instance port
    - `username` - The database username
    - `credentials` - Map with `:access_key_id`, `:secret_access_key`, and optionally `:token` (session token)
    - `region` - The AWS region

  ## Returns

    - `{:ok, token}` on success
    - `{:error, reason}` on failure
  """
  @spec generate(String.t(), integer(), String.t(), map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def generate(hostname, port, username, credentials, region) do
    url = "https://#{hostname}:#{port}/?Action=connect&DBUser=#{URI.encode_www_form(username)}"
    now = NaiveDateTime.to_erl(NaiveDateTime.utc_now())

    opts = maybe_add_session_token([ttl: @ttl], Map.get(credentials, :token))

    signed_url =
      :aws_signature.sign_v4_query_params(
        credentials.access_key_id,
        credentials.secret_access_key,
        region,
        @service,
        now,
        "GET",
        url,
        opts
      )

    # Strip the protocol prefix — RDS expects just the host:port/query portion
    token = String.replace_leading(signed_url, "https://", "")

    {:ok, token}
  rescue
    e ->
      {:error, Sequin.Error.service(service: :aws, message: "Failed to generate RDS IAM token: #{Exception.message(e)}")}
  end

  defp maybe_add_session_token(opts, nil), do: opts
  defp maybe_add_session_token(opts, token), do: Keyword.put(opts, :session_token, token)
end
