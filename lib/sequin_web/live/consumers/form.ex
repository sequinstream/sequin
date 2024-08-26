defmodule SequinWeb.ConsumersLive.Form do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.Repo

  require Logger

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
      |> assign(:encoded_consumer, encode_consumer(assigns.consumer))
      |> assign(:encoded_errors, encoded_errors)
      |> assign(:encoded_databases, Enum.map(assigns.databases, &encode_database/1))
      |> assign(:encoded_http_endpoints, Enum.map(assigns.http_endpoints, &encode_http_endpoint/1))

    ~H"""
    <div id={@id}>
      <.svelte
        name={@component}
        ssr={false}
        props={
          %{
            consumer: @encoded_consumer,
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
    consumer = assigns[:consumer]

    component =
      cond do
        is_struct(consumer, HttpPullConsumer) -> "consumers/HttpPullForm"
        is_struct(consumer, HttpPushConsumer) -> "consumers/HttpPushForm"
        true -> "consumers/Wizard"
      end

    consumer =
      case consumer do
        %HttpPullConsumer{} -> Repo.preload(consumer, [:postgres_database])
        %HttpPushConsumer{} -> Repo.preload(consumer, [:http_endpoint, :postgres_database])
        _ -> consumer
      end

    socket =
      socket
      |> assign(assigns)
      |> assign(
        consumer: consumer,
        show_errors?: false,
        submit_error: nil,
        changeset: nil,
        component: component
      )
      |> assign_databases()
      |> assign_http_endpoints()
      |> reset_changeset()

    {:ok, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("validate", _params, socket) do
    {:noreply, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("form_updated", %{"form" => form}, socket) do
    params =
      form
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    socket =
      socket
      |> maybe_put_base_consumer(params)
      |> merge_changeset(params)

    {:noreply, socket}
  end

  def handle_event("form_submitted", %{"form" => form}, socket) do
    socket = assign(socket, :submit_error, nil)

    params =
      form
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)
      |> Sequin.Map.reject_nil_values()

    socket =
      if socket.assigns.consumer.id do
        update_consumer(socket, params)
      else
        create_consumer(socket, params)
      end

    socket = assign(socket, :show_errors?, true)
    {:noreply, socket}
  end

  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.consumer.id do
        push_patch(socket, to: ~p"/consumers/#{socket.assigns.consumer.id}")
      else
        push_navigate(socket, to: ~p"/consumers")
      end

    {:noreply, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("refresh_databases", _params, socket) do
    {:noreply, assign_databases(socket)}
  end

  @impl Phoenix.LiveComponent
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
          "column_filters" => Enum.map(form["sourceTableFilters"], &ColumnFilter.from_external/1),
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

  defp encode_consumer(%{__struct__: consumer_type} = consumer) do
    postgres_database_id =
      if is_struct(consumer.postgres_database, PostgresDatabase), do: consumer.postgres_database.id

    source_table = List.first(consumer.source_tables)

    base = %{
      "id" => consumer.id,
      "name" => consumer.name,
      "ack_wait_ms" => consumer.ack_wait_ms,
      "max_ack_pending" => consumer.max_ack_pending,
      "max_deliver" => consumer.max_deliver,
      "max_waiting" => consumer.max_waiting,
      "message_kind" => consumer.message_kind,
      "status" => consumer.status,
      "postgres_database_id" => postgres_database_id,
      "table_oid" => source_table && source_table.oid,
      "source_table_actions" => (source_table && source_table.actions) || [:insert, :update, :delete],
      "source_table_filters" => source_table && Enum.map(source_table.column_filters, &encode_column_filter/1)
    }

    case consumer_type do
      HttpPushConsumer -> Map.put(base, "http_endpoint_id", consumer.http_endpoint_id)
      HttpPullConsumer -> base
    end
  end

  defp encode_column_filter(column_filter) do
    %{
      "columnAttnum" => column_filter.column_attnum,
      "operator" => column_filter.operator,
      "value" => column_filter.value.value,
      # FIXME: Use the value type from column filter
      "valueType" => "string"
    }
  end

  defp encode_errors(%Ecto.Changeset{} = changeset) do
    Error.errors_on(changeset)
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
                  "type" => column.type,
                  "filterType" => Postgres.pg_simple_type_to_filter_type(column.type)
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

  defp update_consumer(socket, params) do
    consumer = socket.assigns.consumer

    case Consumers.update_consumer_with_lifecycle(consumer, params) do
      {:ok, updated_consumer} ->
        socket.assigns.on_finish.(updated_consumer)

        socket
        |> assign(:consumer, updated_consumer)
        |> push_navigate(to: ~p"/consumers/#{updated_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp create_consumer(socket, params) do
    account_id = current_account_id(socket)

    case_result =
      case socket.assigns.consumer do
        %HttpPullConsumer{} ->
          Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, params)

        %HttpPushConsumer{} ->
          Consumers.create_http_push_consumer_for_account_with_lifecycle(account_id, params)
      end

    case case_result do
      {:ok, consumer} ->
        socket.assigns.on_finish.(consumer)
        push_navigate(socket, to: ~p"/consumers/#{consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp reset_changeset(socket) do
    consumer = socket.assigns.consumer
    assign(socket, :changeset, changeset(socket, consumer, %{}))
  end

  defp merge_changeset(socket, params) do
    consumer = socket.assigns.consumer
    assign(socket, :changeset, changeset(socket, consumer, params))
  end

  defp changeset(socket, %HttpPullConsumer{id: nil}, params) do
    account_id = current_account_id(socket)
    HttpPullConsumer.create_changeset(%HttpPullConsumer{account_id: account_id}, params)
  end

  defp changeset(_socket, %HttpPullConsumer{} = consumer, params) do
    HttpPullConsumer.update_changeset(consumer, params)
  end

  defp changeset(socket, %HttpPushConsumer{id: nil}, params) do
    account_id = current_account_id(socket)
    HttpPushConsumer.create_changeset(%HttpPushConsumer{account_id: account_id}, params)
  end

  defp changeset(_socket, %HttpPushConsumer{} = consumer, params) do
    HttpPushConsumer.update_changeset(consumer, params)
  end

  # user is in wizard and hasn't selected a consumer_kind yet
  defp changeset(_socket, nil, _params) do
    nil
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

  defp maybe_put_base_consumer(socket, params) do
    consumer_kind = params["consumerKind"]
    consumer = socket.assigns.consumer

    if is_nil(consumer) and consumer_kind do
      case consumer_kind do
        "pull" ->
          assign(socket, :consumer, %HttpPullConsumer{})

        "push" ->
          assign(socket, :consumer, %HttpPushConsumer{})
      end
    else
      socket
    end
  end
end
