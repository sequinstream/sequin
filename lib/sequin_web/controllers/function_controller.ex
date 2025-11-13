defmodule SequinWeb.FunctionController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", functions: Consumers.list_functions_for_account(account_id))
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, function} <- find_function_for_account(account_id, id_or_name) do
      render(conn, "show.json", function: function)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, cleaned_params} <- Transforms.from_external_function(params),
         {:ok, function} <- Consumers.create_function(account_id, cleaned_params) do
      render(conn, "show.json", function: function)
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    params = Map.delete(params, "id_or_name")
    account_id = conn.assigns.account_id

    with {:ok, existing_function} <- find_function_for_account(account_id, id_or_name),
         {:ok, cleaned_params} <- Transforms.from_external_function(params),
         {:ok, updated_function} <- Consumers.update_function(existing_function, cleaned_params) do
      render(conn, "show.json", function: updated_function)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, function} <- find_function_for_account(account_id, id_or_name),
         {:ok, _function} <- Consumers.delete_function(account_id, function.id) do
      render(conn, "delete.json", function: function)
    end
  end

  defp find_function_for_account(account_id, id_or_name) do
    if Sequin.String.uuid?(id_or_name) do
      Consumers.get_function_for_account(account_id, id_or_name)
    else
      Consumers.find_function(account_id, name: id_or_name)
    end
  end
end
