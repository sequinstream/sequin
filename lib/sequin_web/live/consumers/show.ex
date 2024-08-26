defmodule SequinWeb.ConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Repo
  alias SequinWeb.ConsumersLive.Form

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    consumer = Consumers.get_consumer_for_account(current_account_id(socket), id)

    consumer =
      case consumer do
        %HttpPushConsumer{} ->
          Repo.preload(consumer, [:http_endpoint, :postgres_database])

        %HttpPullConsumer{} ->
          Repo.preload(consumer, [:postgres_database])
      end

    {:ok, assign(socket, consumer: consumer)}
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
            current_account={@current_account}
          />
        <% {:show, %HttpPushConsumer{}} -> %>
          <.svelte
            name="consumers/ShowHttpPush"
            props={%{consumer: encode_consumer(@consumer), parent: "consumer-show"}}
          />
        <% {:show, %HttpPullConsumer{}} -> %>
          <.svelte
            name="consumers/ShowHttpPull"
            props={%{consumer: encode_consumer(@consumer), parent: "consumer-show"}}
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
         |> put_flash(:info, "Consumer deleted successfully.")
         |> push_navigate(to: ~p"/consumers")}

      {:error, _changeset} ->
        {:noreply, put_flash(socket, :error, "Failed to delete consumer. Please try again.")}
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
      source_table: encode_source_table(List.first(consumer.source_tables), consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      # FIXME: Implement health calculation
      health: 98,
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
      # FIXME: Implement health calculation
      health: 98,
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
end
