defmodule SequinWeb.Live.Consumers.HttpPullConsumerForm do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
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
      |> assign(:encoded_http_pull_consumer, encode_http_pull_consumer(assigns.http_pull_consumer))
      |> assign(:encoded_errors, encoded_errors)
      |> assign(:encoded_databases, Enum.map(assigns.databases, &encode_database/1))

    ~H"""
    <div id={@id}>
      <.svelte
        name="consumers/HttpPullForm"
        ssr={false}
        props={
          %{
            http_pull_consumer: @encoded_http_pull_consumer,
            errors: @encoded_errors,
            submitError: @submit_error,
            parent: @id,
            databases: @encoded_databases
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveComponent
  def update(assigns, socket) do
    http_pull_consumer =
      assigns[:http_pull_consumer] || %HttpPullConsumer{account_id: current_account_id(assigns)}

    socket =
      socket
      |> assign(assigns)
      |> assign(
        http_pull_consumer: http_pull_consumer,
        show_errors?: false,
        submit_error: nil
      )
      |> assign_databases()
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

    socket =
      if socket.assigns.http_pull_consumer.id do
        update_http_pull_consumer(socket, params)
      else
        create_http_pull_consumer(socket, params)
      end

    socket = assign(socket, :show_errors?, true)
    {:noreply, socket}
  end

  def handle_event("delete_submitted", _params, socket) do
    consumer = socket.assigns.http_pull_consumer

    case Consumers.delete_consumer_with_lifecycle(consumer) do
      {:ok, _} ->
        socket =
          socket
          |> put_flash(:info, "HTTP Pull Consumer deleted successfully")
          |> push_navigate(to: ~p"/consumers")

        {:noreply, socket}

      {:error, _} ->
        socket = put_flash(socket, :error, "Failed to delete HTTP Pull Consumer")
        {:noreply, socket}
    end
  end

  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.http_pull_consumer.id do
        push_patch(socket, to: ~p"/consumers/#{socket.assigns.http_pull_consumer.id}")
      else
        push_navigate(socket, to: ~p"/consumers")
      end

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
    %{
      "ack_wait_ms" => form["ackWaitMs"],
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
  end

  defp encode_http_pull_consumer(%HttpPullConsumer{} = http_pull_consumer) do
    postgres_database_id =
      if is_struct(http_pull_consumer.postgres_database, PostgresDatabase), do: http_pull_consumer.postgres_database.id

    source_table = List.first(http_pull_consumer.source_tables)

    %{
      "id" => http_pull_consumer.id,
      "name" => http_pull_consumer.name,
      "ack_wait_ms" => http_pull_consumer.ack_wait_ms,
      "max_ack_pending" => http_pull_consumer.max_ack_pending,
      "max_deliver" => http_pull_consumer.max_deliver,
      "max_waiting" => http_pull_consumer.max_waiting,
      "message_kind" => http_pull_consumer.message_kind,
      "status" => http_pull_consumer.status,
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

  defp update_http_pull_consumer(socket, params) do
    consumer = socket.assigns.http_pull_consumer

    case Consumers.update_consumer_with_lifecycle(consumer, params) do
      {:ok, http_pull_consumer} ->
        socket.assigns.on_finish.(http_pull_consumer)

        socket
        |> assign(:http_pull_consumer, http_pull_consumer)
        |> push_navigate(to: ~p"/consumers/#{http_pull_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp create_http_pull_consumer(socket, params) do
    account_id = current_account_id(socket)

    case Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, params) do
      {:ok, http_pull_consumer} ->
        socket.assigns.on_finish.(http_pull_consumer)
        push_navigate(socket, to: ~p"/consumers/#{http_pull_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp reset_changeset(socket) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_pull_consumer.id do
        HttpPullConsumer.update_changeset(socket.assigns.http_pull_consumer, %{})
      else
        HttpPullConsumer.create_changeset(%HttpPullConsumer{account_id: account_id}, %{})
      end

    assign(socket, :changeset, changeset)
  end

  defp merge_changeset(socket, params) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_pull_consumer.id do
        HttpPullConsumer.update_changeset(socket.assigns.http_pull_consumer, params)
      else
        HttpPullConsumer.create_changeset(%HttpPullConsumer{account_id: account_id}, params)
      end

    assign(socket, :changeset, changeset)
  end

  defp assign_databases(socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)
    assign(socket, :databases, databases)
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
