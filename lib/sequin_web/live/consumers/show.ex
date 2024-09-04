defmodule SequinWeb.ConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.ApiTokens
  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo
  alias SequinWeb.ConsumersLive.Form

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    consumer = Consumers.get_consumer_for_account(current_account_id(socket), id)
    {:ok, api_token} = ApiTokens.get_token_by(account_id: current_account_id(socket), name: "Default")

    consumer =
      case consumer do
        %HttpPushConsumer{} ->
          Repo.preload(consumer, [:http_endpoint, :postgres_database])

        %HttpPullConsumer{} ->
          Repo.preload(consumer, [:postgres_database])
      end

    # Attempt to get existing health, fallback to initializing
    {:ok, health} = Health.get(consumer)
    consumer = %{consumer | health: health}

    if connected?(socket) do
      Process.send_after(self(), :update_health, 1000)
      Process.send_after(self(), :update_metrics, 1000)
    end

    {:ok,
     socket
     |> assign(:consumer, consumer)
     |> assign(:api_token, api_token)
     |> assign(:host, SequinWeb.Endpoint.url())
     |> assign_replica_identity()
     |> assign_metrics()}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :show, _params) do
    assign(socket, :page_title, "Show Consumer")
  end

  defp apply_action(socket, :edit, _params) do
    assign(socket, :page_title, "Edit Consumer")
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="consumer-show">
      <%= case {@live_action, @consumer} do %>
        <% {:edit, _consumer} -> %>
          <.live_component
            module={Form}
            id="edit-consumer"
            consumer={@consumer}
            on_finish={&handle_edit_finish/1}
            current_user={@current_user}
          />
        <% {:show, %HttpPushConsumer{}} -> %>
          <.svelte
            name="consumers/ShowHttpPush"
            props={
              %{
                consumer: encode_consumer(@consumer),
                replica_identity: @replica_identity,
                parent: "consumer-show",
                metrics: @metrics
              }
            }
          />
        <% {:show, %HttpPullConsumer{}} -> %>
          <.svelte
            name="consumers/ShowHttpPull"
            props={
              %{
                consumer: encode_consumer(@consumer),
                replica_identity: @replica_identity,
                parent: "consumer-show",
                metrics: @metrics,
                host: @host,
                api_token: encode_api_token(@api_token)
              }
            }
          />
      <% end %>
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("edit", _params, socket) do
    {:noreply, push_patch(socket, to: ~p"/consumers/#{socket.assigns.consumer.id}/edit")}
  end

  def handle_event("delete", _params, socket) do
    case Consumers.delete_consumer_with_lifecycle(socket.assigns.consumer) do
      {:ok, _deleted_consumer} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :success, title: "Consumer deleted successfully."})
         |> push_navigate(to: ~p"/consumers")}

      {:error, _changeset} ->
        {:noreply, push_toast(socket, %{kind: :error, title: "Failed to delete consumer. Please try again."})}
    end
  end

  defp handle_edit_finish(updated_consumer) do
    send(self(), {:updated_consumer, updated_consumer})
  end

  @impl Phoenix.LiveView
  def handle_info({:updated_consumer, updated_consumer}, socket) do
    {:noreply,
     socket
     |> assign(consumer: updated_consumer)
     |> push_patch(to: ~p"/consumers/#{updated_consumer.id}")}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)

    case Health.get(socket.assigns.consumer) do
      {:ok, health} ->
        updated_consumer = Map.put(socket.assigns.consumer, :health, health)
        {:noreply, assign(socket, consumer: updated_consumer)}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 1000)
    {:noreply, assign_metrics(socket)}
  end

  defp assign_replica_identity(socket) do
    consumer = socket.assigns.consumer
    [source_table] = consumer.source_tables
    source_table = Sequin.Enum.find!(consumer.postgres_database.tables, &(&1.oid == source_table.oid))

    case Databases.check_replica_identity(consumer.postgres_database, source_table.schema, source_table.name) do
      {:ok, replica_identity} ->
        assign(socket, :replica_identity, replica_identity)

      {:error, _} ->
        assign(socket, :replica_identity, nil)
    end
  end

  defp assign_metrics(socket) do
    consumer = socket.assigns.consumer

    {:ok, messages_processed_count} = Metrics.get_consumer_messages_processed_count(consumer)
    {:ok, messages_processed_throughput} = Metrics.get_consumer_messages_processed_throughput(consumer)

    metrics = %{
      messages_processed_count: messages_processed_count,
      messages_processed_throughput: Float.round(messages_processed_throughput * 60, 1)
    }

    assign(socket, :metrics, metrics)
  end

  defp encode_consumer(%HttpPushConsumer{} = consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      status: consumer.status,
      message_kind: consumer.message_kind,
      ack_wait_ms: consumer.ack_wait_ms,
      max_ack_pending: consumer.max_ack_pending,
      max_deliver: consumer.max_deliver,
      max_waiting: consumer.max_waiting,
      inserted_at: consumer.inserted_at,
      updated_at: consumer.updated_at,
      http_endpoint: encode_http_endpoint(consumer.http_endpoint),
      http_endpoint_path: consumer.http_endpoint_path,
      source_table: encode_source_table(List.first(consumer.source_tables), consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      # FIXME: Implement health calculation
      health: Health.to_external(consumer.health),
      # FIXME: Implement messages processed count
      messages_processed: 1_234_567,
      # FIXME: Implement average latency calculation
      avg_latency: 45
    }
  end

  defp encode_consumer(%HttpPullConsumer{} = consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      status: consumer.status,
      message_kind: consumer.message_kind,
      ack_wait_ms: consumer.ack_wait_ms,
      max_ack_pending: consumer.max_ack_pending,
      max_deliver: consumer.max_deliver,
      max_waiting: consumer.max_waiting,
      inserted_at: consumer.inserted_at,
      updated_at: consumer.updated_at,
      source_table: encode_source_table(List.first(consumer.source_tables), consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      health: Health.to_external(consumer.health),
      # FIXME: Implement messages processed count
      messages_processed: 1_234_567,
      # FIXME: Implement average latency calculation
      avg_latency: 45
    }
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      url: http_endpoint.base_url
    }
  end

  defp encode_source_table(source_table, postgres_database) do
    table = find_table_by_oid(source_table.oid, postgres_database.tables)

    %{
      name: table.name,
      schema: table.schema,
      column_filters: Enum.map(source_table.column_filters, &encode_column_filter(&1, table))
    }
  end

  defp find_table_by_oid(oid, tables) do
    Enum.find(tables, &(&1.oid == oid))
  end

  defp encode_column_filter(column_filter, table) do
    column = Enum.find(table.columns, &(&1.attnum == column_filter.column_attnum))

    %{
      column: column.name,
      operator: column_filter.operator,
      value: column_filter.value.value
    }
  end

  defp encode_postgres_database(postgres_database) do
    %{
      id: postgres_database.id,
      name: postgres_database.name
    }
  end

  defp encode_api_token(%ApiToken{} = api_token) do
    %{
      name: api_token.name,
      token: api_token.token
    }
  end
end
