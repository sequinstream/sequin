defmodule Sequin.Logger.StaticRedactor do
  @moduledoc false
  @behaviour LoggerJSON.Redactor

  @sensitive [
    :access_key_id,
    :api_key,
    :auth_token,
    :aws_access_key_id,
    :aws_secret_access_key,
    :client_secret,
    :credentials,
    :current_password,
    :hashed_password,
    :hashed_token,
    :nkey_seed,
    :password,
    :private_key,
    :private_key_id,
    :secret_access_key,
    :shared_access_key,
    :shared_access_key_name,
    :token
  ]

  @impl true
  def redact(key, _value, _opts) when key in @sensitive, do: "[REDACTED]"
  def redact(_key, value, _opts), do: value
end
