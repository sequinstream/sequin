defmodule SequinWeb.SinkConsumerController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Error.NotFoundError
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", sink_consumers: Consumers.list_sink_consumers_for_account(account_id))
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name) do
      render(conn, "show.json", sink_consumer: sink_consumer)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id
    databases = Databases.list_dbs_for_account(account_id)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)

    with {:ok, cleaned_params} <- Transforms.from_external_sink_consumer(account_id, params, databases, http_endpoints),
         {:ok, sink_consumer} <- Consumers.create_sink_consumer(account_id, cleaned_params) do
      render(conn, "show.json", sink_consumer: sink_consumer)
    else
      {:error, %NotFoundError{entity: :sequence}} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("error.json",
          error: %{
            "summary" =>
              "Reference to table that does not exist or Sequin does not have access to in the source database: `#{params["table"]}`"
          }
        )

      error ->
        error
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    params = Map.delete(params, "id_or_name")
    account_id = conn.assigns.account_id
    databases = Databases.list_dbs_for_account(account_id)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)

    with {:ok, existing_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name),
         {:ok, cleaned_params} <- Transforms.from_external_sink_consumer(account_id, params, databases, http_endpoints),
         {:ok, updated_consumer} <- Consumers.update_sink_consumer(existing_consumer, cleaned_params) do
      render(conn, "show.json", sink_consumer: updated_consumer)
    else
      {:error, %NotFoundError{entity: :sequence}} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("error.json",
          error: %{
            "summary" =>
              "Reference to table that does not exist or Sequin does not have access to in the source database: `#{params["table"]}`"
          }
        )

      error ->
        error
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name),
         {:ok, _sink_consumer} <- Consumers.delete_sink_consumer(sink_consumer) do
      render(conn, "delete.json", sink_consumer: sink_consumer)
    end
  end
end
