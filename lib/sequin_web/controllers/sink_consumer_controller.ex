defmodule SequinWeb.SinkConsumerController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  require Logger

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    sink_consumers =
      account_id
      |> Consumers.list_sink_consumers_for_account()
      |> Enum.map(&load_health/1)

    render(conn, "index.json", sink_consumers: sink_consumers)
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name) do
      render(conn, "show.json", sink_consumer: load_health(sink_consumer))
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id
    databases = Databases.list_dbs_for_account(account_id, [:replication_slot])
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)

    with {:ok, cleaned_params} <- Transforms.from_external_sink_consumer(account_id, params, databases, http_endpoints),
         cleaned_params = Map.put_new(cleaned_params, :source, %{}),
         {:ok, sink_consumer} <- Consumers.create_sink_consumer(account_id, cleaned_params) do
      render(conn, "show.json", sink_consumer: sink_consumer)
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    params = Map.delete(params, "id_or_name")
    account_id = conn.assigns.account_id
    databases = Databases.list_dbs_for_account(account_id, [:replication_slot])
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)

    with {:ok, existing_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name),
         {:ok, cleaned_params} <- Transforms.from_external_sink_consumer(account_id, params, databases, http_endpoints),
         {:ok, updated_consumer} <- Consumers.update_sink_consumer(existing_consumer, cleaned_params) do
      render(conn, "show.json", sink_consumer: updated_consumer)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name),
         {:ok, _sink_consumer} <- Consumers.delete_sink_consumer(sink_consumer) do
      render(conn, "delete.json", sink_consumer: sink_consumer)
    end
  end

  defp load_health(%SinkConsumer{} = sink_consumer) do
    case Health.health(sink_consumer) do
      {:ok, health} ->
        %{sink_consumer | health: health}

      {:error, error} ->
        Logger.warning("Failed to load health for sink consumer #{sink_consumer.id}", error: error)
        sink_consumer
    end
  end
end
