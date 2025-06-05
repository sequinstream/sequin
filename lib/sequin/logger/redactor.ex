defmodule Sequin.Logger.Redactor do
  @moduledoc """
  Redactor that's applied to message + metadata of JSON log entries.
  """

  @behaviour LoggerJSON.Redactor

  @sensitive [
    "access_key_id",
    "api_key",
    "auth_token",
    "auth_value",
    "aws_access_key_id",
    "aws_secret_access_key",
    "client_email",
    "client_secret",
    "contact_email",
    "credentials",
    "current_password",
    "email",
    "hashed_password",
    "hashed_token",
    "jwt",
    "nkey_seed",
    "password",
    "private_key",
    "private_key_id",
    "secret_access_key",
    "shared_access_key",
    "shared_access_key_name",
    "token"
  ]

  @impl true
  def redact(key, value, _opts) when key in @sensitive and is_binary(value), do: "[REDACTED]"
  def redact(_key, value, _opts), do: value
end
