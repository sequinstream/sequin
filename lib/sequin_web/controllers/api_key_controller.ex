defmodule SequinWeb.ApiKeyController do
  use SequinWeb, :controller

  alias Sequin.Accounts
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id
    api_keys = Accounts.list_api_keys_for_account(account_id)
    render(conn, "index.json", api_keys: api_keys)
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, api_key} <- Accounts.create_api_key(account_id, params) do
      render(conn, "show.json", api_key: api_key)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, api_key} <- Accounts.get_api_key_for_account(account_id, id),
         {:ok, _api_key} <- Accounts.delete_api_key(api_key) do
      send_resp(conn, :no_content, "")
    end
  end
end
