defmodule SequinWeb.Live.Consumers.HttpPushConsumerForm do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.Repo

  @impl Phoenix.LiveComponent
  def render(assigns) do
    encoded_errors =
      if assigns.show_errors? do
        encode_errors(assigns.changeset)
      else
        %{}
      end

    assigns =
      assigns
      |> assign(:encoded_http_push_consumer, encode_http_push_consumer(assigns.http_push_consumer))
      |> assign(:encoded_errors, encoded_errors)
      |> assign(:encoded_databases, Enum.map(assigns.databases, &encode_database/1))
      |> assign(:encoded_http_endpoints, Enum.map(assigns.http_endpoints, &encode_http_endpoint/1))

    ~H"""
    <div id={@id}>
      <.svelte
        name="consumers/HttpPushForm"
        ssr={false}
        props={
          %{
            http_push_consumer: @encoded_http_push_consumer,
            errors: @encoded_errors,
            submitError: @submit_error,
            parent: @id,
            databases: @encoded_databases,
            httpEndpoints: @encoded_http_endpoints
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveComponent
  def update(assigns, socket) do
    http_push_consumer =
      assigns[:http_push_consumer] || %HttpPushConsumer{account_id: current_account_id(assigns)}

    socket =
      socket
      |> assign(assigns)
      |> assign(
        http_push_consumer: http_push_consumer,
        show_errors?: false,
        submit_error: nil
      )
      |> assign_databases()
      |> assign_http_endpoints()
      |> reset_changeset()

    {:ok, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("form_updated", %{"form" => form}, socket) do
    params =
      form
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    socket = merge_changeset(socket, params)
    {:noreply, socket}
  end

  def handle_event("form_submitted", %{"form" => form}, socket) do
    socket = assign(socket, :submit_error, nil)

    params =
      form
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)
      # Remove nil values to allow defaults to be applied
      |> Sequin.Map.reject_nil_values()

    socket =
      if socket.assigns.http_push_consumer.id do
        update_http_push_consumer(socket, params)
      else
        create_http_push_consumer(socket, params)
      end

    socket = assign(socket, :show_errors?, true)
    {:noreply, socket}
  end

  def handle_event("delete_submitted", _params, socket) do
    consumer = socket.assigns.http_push_consumer

    case Consumers.delete_consumer_with_lifecycle(consumer) do
      {:ok, _} ->
        socket =
          socket
          |> put_flash(:info, "HTTP Push Consumer deleted successfully")
          |> push_navigate(to: ~p"/consumers")

        {:noreply, socket}

      {:error, _} ->
        socket = put_flash(socket, :error, "Failed to delete HTTP Push Consumer")
        {:noreply, socket}
    end
  end

  def handle_event("form_closed", _params, socket) do
    socket = push_navigate(socket, to: ~p"/consumers")
    {:noreply, socket}
  end

  def handle_event("refresh_databases", _params, socket) do
    {:noreply, assign_databases(socket)}
  end

  def handle_event("refresh_tables", %{"database_id" => database_id}, socket) do
    with {:ok, database} <- Databases.get_db(database_id),
         {:ok, _updated_database} <- Databases.update_tables(database) do
      {:noreply, assign_databases(socket)}
    else
      _ -> {:noreply, socket}
    end
  end

  defp decode_params(form) do
    params = %{
      "ack_wait_ms" => form["ackWaitMs"],
      "http_endpoint" => form["httpEndpoint"],
      "http_endpoint_id" => form["httpEndpointId"],
      "max_ack_pending" => form["maxAckPending"],
      "max_waiting" => form["maxWaiting"],
      "message_kind" => form["messageKind"],
      "name" => form["name"],
      "postgres_database_id" => form["postgresDatabaseId"],
      "source_tables" => [
        %{
          "oid" => form["tableOid"],
          "column_filters" =>
            Enum.map(form["sourceTableFilters"], fn filter ->
              %{
                "column" => filter["column"],
                "operator" => filter["operator"],
                "value" => %{value: filter["value"], __type__: "string"}
              }
            end),
          "actions" => form["sourceTableActions"] || []
        }
      ]
    }

    if params["http_endpoint_id"] do
      Map.delete(params, "http_endpoint")
    else
      params
    end
  end

  defp encode_http_push_consumer(%HttpPushConsumer{} = http_push_consumer) do
    postgres_database_id = if http_push_consumer.postgres_database, do: http_push_consumer.postgres_database.id
    source_table = List.first(http_push_consumer.source_tables)

    %{
      "id" => http_push_consumer.id,
      "name" => http_push_consumer.name,
      "ack_wait_ms" => http_push_consumer.ack_wait_ms,
      "max_ack_pending" => http_push_consumer.max_ack_pending,
      "max_deliver" => http_push_consumer.max_deliver,
      "max_waiting" => http_push_consumer.max_waiting,
      "message_kind" => http_push_consumer.message_kind,
      "status" => http_push_consumer.status,
      "http_endpoint_id" => http_push_consumer.http_endpoint_id,
      "postgres_database_id" => postgres_database_id,
      "table_oid" => source_table && source_table.oid,
      "source_table_actions" => (source_table && source_table.actions) || [:insert, :update, :delete]
    }
  end

  defp encode_errors(%Ecto.Changeset{} = changeset) do
    Sequin.Error.errors_on(changeset)
  end

  defp encode_database(database) do
    %{
      "id" => database.id,
      "name" => database.name,
      "tables" =>
        Enum.map(database.tables, fn %Table{} = table ->
          %{
            "oid" => table.oid,
            "schema" => table.schema,
            "name" => table.name,
            "columns" =>
              Enum.map(table.columns, fn %Table.Column{} = column ->
                %{
                  "attnum" => column.attnum,
                  "isPk?" => column.is_pk?,
                  "name" => column.name,
                  "type" => column.type
                }
              end)
          }
        end)
    }
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      "id" => http_endpoint.id,
      "name" => http_endpoint.name,
      "base_url" => http_endpoint.base_url
    }
  end

  defp update_http_push_consumer(socket, params) do
    consumer = socket.assigns.http_push_consumer

    case Consumers.update_consumer_with_lifecycle(consumer, params) do
      {:ok, http_push_consumer} ->
        socket.assigns.on_finish.(http_push_consumer)

        socket
        |> assign(:http_push_consumer, http_push_consumer)
        |> push_navigate(to: ~p"/consumers/#{http_push_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp create_http_push_consumer(socket, params) do
    account_id = current_account_id(socket)

    case Consumers.create_http_push_consumer_for_account_with_lifecycle(account_id, params) do
      {:ok, http_push_consumer} ->
        socket.assigns.on_finish.(http_push_consumer)
        push_navigate(socket, to: ~p"/consumers/#{http_push_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp reset_changeset(socket) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_push_consumer.id do
        HttpPushConsumer.update_changeset(socket.assigns.http_push_consumer, %{})
      else
        HttpPushConsumer.create_changeset(%HttpPushConsumer{account_id: account_id}, %{})
      end

    assign(socket, :changeset, changeset)
  end

  defp merge_changeset(socket, params) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_push_consumer.id do
        HttpPushConsumer.update_changeset(socket.assigns.http_push_consumer, params)
      else
        HttpPushConsumer.create_changeset(%HttpPushConsumer{account_id: account_id}, params)
      end

    assign(socket, :changeset, changeset)
  end

  defp assign_databases(socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)
    assign(socket, :databases, databases)
  end

  defp assign_http_endpoints(socket) do
    account_id = current_account_id(socket)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)
    assign(socket, :http_endpoints, http_endpoints)
  end

  defp maybe_put_replication_slot_id(%{"postgres_database_id" => nil} = params, _socket) do
    params
  end

  defp maybe_put_replication_slot_id(%{"postgres_database_id" => postgres_database_id} = params, socket) do
    with {:ok, database} <- Databases.get_db_for_account(current_account_id(socket), postgres_database_id) do
      database = Repo.preload(database, :replication_slot)
      Map.put(params, "replication_slot_id", database.replication_slot.id)
    end
  end
end
