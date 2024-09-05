defmodule SequinWeb.Plugs.VerifyApiToken do
  @moduledoc false
  import Plug.Conn

  alias Sequin.ApiTokens
  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias SequinWeb.ApiFallbackPlug

  @header "authorization"

  @token_instructions """
  API tokens are generated in the Sequin console (console.sequin.io) and should be included in the Authorization header of your request. For example:

  Authorization: Bearer <your-api-token>
  """

  def header, do: @header

  def init(opts), do: opts

  def call(conn, _opts) do
    with ["Bearer " <> token] <- get_req_header(conn, "authorization"),
         {:ok, %ApiToken{name: token_name, account_id: account_id}} <- ApiTokens.find_by_token(token) do
      Logger.metadata(account_id: account_id, api_token_name: token_name)
      assign(conn, :account_id, account_id)
    else
      {:error, %NotFoundError{entity: :api_token}} ->
        error =
          Error.unauthorized(
            message: """
            The API token you provided is invalid or has expired. Your token may have been revoked or deleted in the Sequin console.
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [] ->
        error =
          Error.unauthorized(
            message: """
            Please provide a valid API token in the Authorization header.

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [_] ->
        error =
          Error.unauthorized(
            message: """
            Please provide a valid API token in the Authorization header. Ensure your Authorization value is prefixed with "Bearer".

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [_, _] ->
        error =
          Error.unauthorized(
            message: """
            Please provide exactly one Authorization header in your request.

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})
    end
  end
end
