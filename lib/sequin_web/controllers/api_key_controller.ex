defmodule SequinWeb.ApiKeyController do
  use SequinWeb, :controller

  alias Sequin.ApiTokens
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id
    api_keys = ApiTokens.list_tokens_for_account(account_id)
    render(conn, "index.json", api_keys: api_keys)
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, api_key} <- ApiTokens.create_for_account(account_id, params) do
      render(conn, "show.json", api_key: api_key)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, _} <- ApiTokens.delete_token_for_account(id, account_id) do
      json(conn, %{success: true})
    end
  end
end
