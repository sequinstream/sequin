defmodule SequinWeb.HttpEndpointController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", http_endpoints: Consumers.list_http_endpoints_for_account(account_id))
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, http_endpoint} <- Consumers.find_http_endpoint_for_account(account_id, id_or_name: id_or_name) do
      render(conn, "show.json", http_endpoint: http_endpoint)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, cleaned_params} <- Transforms.from_external_http_endpoint(params),
         {:ok, http_endpoint} <- Consumers.create_http_endpoint(account_id, cleaned_params) do
      render(conn, "show.json", http_endpoint: http_endpoint)
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, existing_endpoint} <- Consumers.find_http_endpoint_for_account(account_id, id_or_name: id_or_name),
         {:ok, cleaned_params} <- Transforms.from_external_http_endpoint(params),
         {:ok, updated_endpoint} <- Consumers.update_http_endpoint(existing_endpoint, cleaned_params) do
      render(conn, "show.json", http_endpoint: updated_endpoint)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, http_endpoint} <- Consumers.find_http_endpoint_for_account(account_id, id_or_name: id_or_name),
         {:ok, _http_endpoint} <- Consumers.delete_http_endpoint(http_endpoint) do
      render(conn, "delete.json", http_endpoint: http_endpoint)
    end
  end
end
