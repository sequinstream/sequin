defmodule SequinWeb.FunctionController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", functions: Consumers.list_functions_for_account(account_id))
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, function} <- Consumers.find_function(account_id, id_or_name: id_or_name) do
      render(conn, "show.json", function: function)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, function} <- Consumers.find_function(account_id, id_or_name: id_or_name),
         {:ok, _function} <- Consumers.delete_function(account_id, function.id) do
      render(conn, "delete.json", function: function)
    end
  end
end
