defmodule SequinWeb.ConsumersLive.Form do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Error
  alias Sequin.Name
  alias Sequin.Postgres
  alias Sequin.Posthog
  alias Sequin.Repo

  require Logger

  defguardp is_create?(socket) when is_nil(socket.assigns.consumer) or is_nil(socket.assigns.consumer.id)

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
        component: component,
        prev_params: %{}
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
    params = form |> decode_params(socket) |> maybe_put_replication_slot_id(socket)

    socket =
      socket
      |> handle_params_changes(params)
      |> merge_changeset(params)
      |> assign(prev_params: params)

    {:noreply, socket}
  end

  def handle_event("form_submitted", %{"form" => form}, socket) do
    socket = assign(socket, :submit_error, nil)

    params =
      form
      |> decode_params(socket)
      |> maybe_put_replication_slot_id(socket)
      |> Sequin.Map.reject_nil_values()

    res =
      if socket.assigns.consumer.id do
        update_consumer(socket, params)
      else
        create_consumer(socket, params)
      end

    case res do
      {:ok, socket} ->
        {:reply, %{ok: true}, socket}

      {:error, socket} ->
        socket = assign(socket, :show_errors?, true)
        {:reply, %{ok: false}, socket}
    end
  end

  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.consumer && socket.assigns.consumer.id do
        push_navigate(socket, to: ~p"/consumers/#{socket.assigns.consumer.id}")
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

  def handle_event("generate_webhook_site_url", _params, socket) do
    case generate_webhook_site_endpoint(socket) do
      {:ok, %HttpEndpoint{} = http_endpoint} ->
        {:reply, %{http_endpoint_id: http_endpoint.id}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("refresh_http_endpoints", _params, socket) do
    {:noreply, assign_http_endpoints(socket)}
  end

  defp decode_params(form, socket) do
    message_kind = form["messageKind"]

    source_table_actions =
      if message_kind == "change" do
        [:insert, :update, :delete]
      else
        form["sourceTableActions"] || []
      end

    %{
      "consumer_kind" => form["consumerKind"],
      "ack_wait_ms" => form["ackWaitMs"],
      "http_endpoint_id" => form["httpEndpointId"],
      "http_endpoint_path" => form["httpEndpointPath"],
      "max_ack_pending" => form["maxAckPending"],
      "max_waiting" => form["maxWaiting"],
      "message_kind" => message_kind,
      "name" => form["name"],
      "postgres_database_id" => form["postgresDatabaseId"],
      "source_tables" => [
        %{
          "oid" => form["tableOid"],
          "column_filters" => Enum.map(form["sourceTableFilters"], &ColumnFilter.from_external/1),
          "actions" => source_table_actions,
          "sort_column_attnum" => form["sortColumnAttnum"]
        }
      ]
    }
    |> maybe_delete_http_endpoint()
    |> maybe_put_record_consumer_state(form, socket)
  end

  defp maybe_delete_http_endpoint(params) do
    if params["http_endpoint_id"] do
      Map.delete(params, "http_endpoint")
    else
      params
    end
  end

  defp maybe_put_record_consumer_state(%{"message_kind" => "record"} = params, form, socket) when is_create?(socket) do
    [source_table] = params["source_tables"]
    table = table(socket.assigns.databases, params["postgres_database_id"], source_table)

    initial_min_sort_col = get_in(form, ["recordConsumerState", "initialMinSortCol"])
    producer = get_in(form, ["recordConsumerState", "producer"]) || "table_and_wal"

    initial_min_cursor =
      cond do
        producer == "wal" -> nil
        initial_min_sort_col -> KeysetCursor.min_cursor(table, initial_min_sort_col)
        true -> source_table["sort_column_attnum"] && KeysetCursor.min_cursor(table)
      end

    Map.put(params, "record_consumer_state", %{"producer" => producer, "initial_min_cursor" => initial_min_cursor})
  end

  defp maybe_put_record_consumer_state(params, _form, _socket) do
    params
  end

  defp table(databases, postgres_database_id, source_table) do
    if postgres_database_id do
      db = Sequin.Enum.find!(databases, &(&1.id == postgres_database_id))
      table = Sequin.Enum.find!(db.tables, &(&1.oid == source_table["oid"]))
      %{table | sort_column_attnum: source_table["sort_column_attnum"]}
    end
  end

  defp handle_params_changes(socket, next_params) do
    if is_nil(socket.assigns.prev_params["consumer_kind"]) and next_params["consumer_kind"] do
      case next_params["consumer_kind"] do
        "http_pull" -> assign(socket, :consumer, %HttpPullConsumer{})
        "http_push" -> assign(socket, :consumer, %HttpPushConsumer{})
      end
    else
      socket
    end
  end

  defp encode_consumer(nil), do: nil

  defp encode_consumer(%{__struct__: consumer_type} = consumer) do
    postgres_database_id =
      if is_struct(consumer.postgres_database, PostgresDatabase), do: consumer.postgres_database.id

    source_table = List.first(consumer.source_tables)

    base = %{
      "id" => consumer.id,
      "name" => consumer.name || Name.generate(999),
      "ack_wait_ms" => consumer.ack_wait_ms,
      "max_ack_pending" => consumer.max_ack_pending,
      "max_deliver" => consumer.max_deliver,
      "max_waiting" => consumer.max_waiting,
      "message_kind" => consumer.message_kind,
      "status" => consumer.status,
      "postgres_database_id" => postgres_database_id,
      "table_oid" => source_table && source_table.oid,
      "source_table_actions" => (source_table && source_table.actions) || [:insert, :update, :delete],
      "source_table_filters" => source_table && Enum.map(source_table.column_filters, &ColumnFilter.to_external/1),
      "sort_column_attnum" => source_table && source_table.sort_column_attnum
    }

    case consumer_type do
      HttpPushConsumer ->
        Map.merge(base, %{
          "http_endpoint_id" => consumer.http_endpoint_id,
          "http_endpoint_path" => consumer.http_endpoint_path
        })

      HttpPullConsumer ->
        base
    end
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
      "baseUrl" => HttpEndpoint.url(http_endpoint)
    }
  end

  defp update_consumer(socket, params) do
    consumer = socket.assigns.consumer

    case Consumers.update_consumer_with_lifecycle(consumer, params) do
      {:ok, updated_consumer} ->
        socket =
          socket
          |> assign(:consumer, updated_consumer)
          |> push_navigate(to: ~p"/consumers/#{updated_consumer.id}")

        {:ok, socket}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, assign(socket, :changeset, changeset)}
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
        consumer_type = if is_struct(consumer, HttpPullConsumer), do: "HttpPullConsumer", else: "HttpPushConsumer"

        Posthog.capture("Consumer Created", %{
          distinct_id: socket.assigns.current_user.id,
          properties: %{
            consumer_type: consumer_type,
            stream_type: consumer.message_kind,
            consumer_id: consumer.id,
            consumer_name: consumer.name,
            "$groups": %{account: consumer.account_id}
          }
        })

        {:ok, push_navigate(socket, to: ~p"/consumers/#{consumer.id}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, assign(socket, :changeset, changeset)}
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
    case Databases.get_db_for_account(current_account_id(socket), postgres_database_id) do
      {:ok, database} ->
        database = Repo.preload(database, :replication_slot)
        Map.put(params, "replication_slot_id", database.replication_slot.id)

      _ ->
        Map.merge(params, %{"replication_slot_id" => nil, "postgres_database_id" => nil})
    end
  end

  defp generate_webhook_site_endpoint(socket) do
    url = "https://webhook.site/token"
    headers = [{"Content-Type", "application/json"}]

    body =
      Jason.encode!(%{
        default_status: 200,
        default_content: Jason.encode!(%{ok: true}),
        default_content_type: "application/json",
        cors: true
      })

    case Req.post(url, headers: headers, body: body) do
      {:ok, %Req.Response{status: 201, body: %{"uuid" => uuid}}} ->
        Consumers.create_http_endpoint_for_account(current_account_id(socket), %{
          name: "webhook-site-#{String.slice(uuid, 0, 8)}",
          scheme: :https,
          host: "webhook.site",
          path: "/#{uuid}"
        })

      {:error, reason} ->
        {:error, "Failed to generate Webhook.site URL: #{inspect(reason)}"}

      _ ->
        {:error, "Unexpected response from Webhook.site"}
    end
  end
end
