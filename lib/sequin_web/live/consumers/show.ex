defmodule SequinWeb.ConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Phoenix.LiveView.AsyncResult
  alias Sequin.ApiTokens
  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo
  alias SequinWeb.ConsumersLive.Form
  alias SequinWeb.RouteHelpers

  require Logger

  # For message management
  @page_size 25

  @impl Phoenix.LiveView
  def mount(%{"id" => id} = params, _session, socket) do
    case load_consumer(id, socket) do
      {:ok, consumer} ->
        {:ok, api_token} = ApiTokens.get_token_by(account_id: current_account_id(socket), name: "Default")

        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1000)
          # Start message updates
          Process.send_after(self(), :update_messages, 100)
        end

        api_base_url = Application.get_env(:sequin, :api_base_url, SequinWeb.Endpoint.url())

        consumer =
          case consumer do
            %HttpPushConsumer{} ->
              Repo.preload(consumer, [:http_endpoint, :postgres_database, :sequence])

            %HttpPullConsumer{} ->
              Repo.preload(consumer, [:postgres_database, :sequence])
          end

        # Attempt to get existing health, fallback to initializing
        {:ok, health} = Health.get(consumer)
        consumer = %{consumer | health: health}

        # Initialize message-related assigns
        socket =
          socket
          |> assign(:consumer, consumer)
          |> assign(:api_token, api_token)
          |> assign(:api_base_url, api_base_url)
          |> assign_metrics()
          |> assign(:paused, false)
          |> assign(:show_acked, params["showAcked"] == "true")
          |> assign(:page, 0)
          |> assign(:page_size, @page_size)
          |> assign(:total_count, 0)
          |> load_consumer_messages()

        {:ok, socket}

      {:error, _error} ->
        {:ok,
         socket
         |> put_flash(:error, "Consumer not found")
         |> push_navigate(to: ~p"/sequences")}
    end
  end

  defp load_consumer(id, socket) do
    case Consumers.get_consumer_for_account(current_account_id(socket), id) do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    show_acked =
      case Map.get(params, "showAcked", "true") do
        "true" -> true
        "false" -> false
        _ -> true
      end

    socket = assign(socket, :show_acked, show_acked)
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :show, _params) do
    assign(socket, :page_title, "Show Consumer")
  end

  defp apply_action(socket, :edit, _params) do
    assign(socket, :page_title, "Edit Consumer")
  end

  defp apply_action(socket, :messages, _params) do
    assign(socket, :page_title, "Messages")
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :kind, Consumers.kind(assigns.consumer))

    ~H"""
    <!-- Use Flexbox to arrange header and content vertically -->
    <div id="consumer-show" class="flex flex-col">
      <!-- The header component -->
      <.svelte
        name="consumers/ShowHeader"
        props={
          %{
            consumer: encode_consumer(@consumer),
            kind: @kind,
            parent: "consumer-show",
            live_action: @live_action,
            messages_failing: @metrics.messages_failing_count > 0
          }
        }
      />
      <!-- Main content area that fills the remaining space -->
      <div class="flex-1 overflow-auto">
        <%= case {@live_action, @consumer} do %>
          <% {:edit, _consumer} -> %>
            <!-- Edit component -->
            <.live_component
              module={Form}
              id="edit-consumer"
              consumer={@consumer}
              on_finish={&handle_edit_finish/1}
              current_user={@current_user}
            />
          <% {:show, %HttpPushConsumer{}} -> %>
            <!-- ShowHttpPush component -->
            <.svelte
              name="consumers/ShowHttpPush"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  parent: "consumer-show",
                  metrics: @metrics
                }
              }
            />
          <% {:show, %HttpPullConsumer{}} -> %>
            <!-- ShowHttpPull component -->
            <.svelte
              name="consumers/ShowHttpPull"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  parent: "consumer-show",
                  metrics: @metrics,
                  apiBaseUrl: @api_base_url,
                  api_token: encode_api_token(@api_token)
                }
              }
            />
          <% {:messages, _consumer} -> %>
            <!-- ShowMessages component -->
            <.svelte
              name="consumers/ShowMessages"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  messages: encode_messages(@consumer, @messages),
                  totalCount: @total_count,
                  pageSize: @page_size,
                  paused: @paused,
                  showAcked: @show_acked
                }
              }
            />
        <% end %>
      </div>
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("edit", _params, socket) do
    case Consumers.kind(socket.assigns.consumer) do
      :push ->
        {:noreply, push_patch(socket, to: ~p"/consumers/push/#{socket.assigns.consumer.id}/edit")}

      :pull ->
        {:noreply, push_patch(socket, to: ~p"/consumers/pull/#{socket.assigns.consumer.id}/edit")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("delete", _params, socket) do
    case Consumers.delete_consumer_with_lifecycle(socket.assigns.consumer) do
      {:ok, _deleted_consumer} ->
        push_to =
          case Consumers.kind(socket.assigns.consumer) do
            :push -> ~p"/consumers/push"
            :pull -> ~p"/consumers/pull"
          end

        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :success, title: "Consumer deleted."})
         |> push_navigate(to: push_to)}

      {:error, _changeset} ->
        {:noreply, push_toast(socket, %{kind: :error, title: "Failed to delete consumer. Please try again."})}
    end
  end

  def handle_event("pause_updates", _params, socket) do
    {:noreply, assign(socket, paused: true)}
  end

  def handle_event("resume_updates", _params, socket) do
    {:noreply, assign(socket, paused: false)}
  end

  def handle_event("change_page", %{"page" => page}, socket) do
    {:noreply,
     socket
     |> assign(page: page)
     |> load_consumer_messages()}
  end

  def handle_event("fetch_message_data", %{"message_id" => message_id}, socket) do
    message = Enum.find(socket.assigns.messages, &(&1.id == message_id))

    case fetch_message_data(message, socket.assigns.consumer) do
      {:ok, data} ->
        {:reply, %{data: data}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("fetch_message_logs", %{"trace_id" => trace_id}, socket) do
    account_id = current_account_id(socket)

    case Sequin.Logs.get_logs_for_consumer_message(account_id, trace_id) do
      {:ok, logs} ->
        {:reply, %{logs: logs}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("update_page_size", %{"page_size" => page_size}, socket) when page_size < 0 do
    {:noreply, socket}
  end

  def handle_event("update_page_size", %{"page_size" => page_size}, socket) do
    {:noreply,
     socket
     |> assign(:page_size, page_size)
     |> load_consumer_messages()}
  end

  def handle_event("toggle_show_acked", %{"show_acked" => show_acked}, socket) do
    {:noreply,
     socket
     |> assign(:show_acked, show_acked)
     |> assign(:page, 0)
     |> load_consumer_messages()
     |> push_patch(to: RouteHelpers.consumer_path(socket.assigns.consumer, "/messages?showAcked=#{show_acked}"))}
  end

  def handle_event("reset_message_visibility", %{"message_id" => message_id}, socket) do
    consumer = socket.assigns.consumer

    case Consumers.reset_message_visibility(consumer, message_id) do
      {:ok, updated_message} ->
        {:reply, %{updated_message: encode_message(consumer, updated_message)}, load_consumer_messages(socket)}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
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
     |> push_patch(to: RouteHelpers.consumer_path(updated_consumer))}
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

  @impl Phoenix.LiveView
  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 1000)
    {:noreply, assign_metrics(socket)}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_messages, %{assigns: %{paused: true}} = socket) do
    schedule_update()
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_messages, socket) do
    socket = load_consumer_messages(socket)
    schedule_update()
    {:noreply, socket}
  end

  defp assign_metrics(socket) do
    consumer = socket.assigns.consumer

    {:ok, messages_processed_count} = Metrics.get_consumer_messages_processed_count(consumer)
    {:ok, messages_processed_throughput} = Metrics.get_consumer_messages_processed_throughput(consumer)
    messages_failing_count = Consumers.count_messages_for_consumer(consumer, delivery_count_gte: 2)

    metrics = %{
      messages_processed_count: messages_processed_count,
      messages_processed_throughput: Float.round(messages_processed_throughput * 60, 1),
      messages_failing_count: messages_failing_count
    }

    assign(socket, :metrics, metrics)
  end

  defp encode_consumer(%HttpPushConsumer{} = consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      kind: :push,
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
      sequence: encode_sequence(consumer.sequence, consumer.sequence_filter, consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      health: Health.to_external(consumer.health),
      href: RouteHelpers.consumer_path(consumer)
    }
  end

  defp encode_consumer(%HttpPullConsumer{} = consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      kind: :pull,
      status: consumer.status,
      message_kind: consumer.message_kind,
      ack_wait_ms: consumer.ack_wait_ms,
      max_ack_pending: consumer.max_ack_pending,
      max_deliver: consumer.max_deliver,
      max_waiting: consumer.max_waiting,
      inserted_at: consumer.inserted_at,
      updated_at: consumer.updated_at,
      sequence: encode_sequence(consumer.sequence, consumer.sequence_filter, consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      health: Health.to_external(consumer.health),
      href: RouteHelpers.consumer_path(consumer)
    }
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      url: HttpEndpoint.url(http_endpoint)
    }
  end

  defp encode_sequence(
         %Sequence{} = sequence,
         %SequenceFilter{} = sequence_filter,
         %PostgresDatabase{} = postgres_database
       ) do
    table = find_table_by_oid(sequence.table_oid, postgres_database.tables)

    %{
      table_name: sequence.table_name,
      table_schema: sequence.table_schema,
      column_filters: Enum.map(sequence_filter.column_filters, &encode_column_filter(&1, table))
    }
  end

  defp find_table_by_oid(oid, tables) do
    Enum.find(tables, &(&1.oid == oid))
  end

  defp encode_column_filter(column_filter, table) do
    column = Enum.find(table.columns, &(&1.attnum == column_filter.column_attnum))

    %{
      column: column.name,
      operator: ColumnFilter.to_external_operator(column_filter.operator),
      value: column_filter.value.value,
      is_jsonb: column_filter.is_jsonb,
      jsonb_path: column_filter.jsonb_path
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

  # Function to load messages for the consumer

  defp load_consumer_messages(
         %{assigns: %{consumer: consumer, page: page, page_size: page_size, show_acked: show_acked}} = socket
       ) do
    messages = load_consumer_messages(consumer, page_size, page * page_size, show_acked)

    db_count = Consumers.fast_count_messages_for_consumer(consumer)

    total_count =
      if show_acked do
        {:ok, redis_count} = AcknowledgedMessages.count_messages(consumer.id)
        db_count + redis_count
      else
        db_count
      end

    socket
    |> assign(:messages, messages)
    |> assign(:total_count, total_count)
  end

  defp load_consumer_messages(consumer, limit, offset, show_acked) do
    db_messages = load_consumer_messages_from_db(consumer, offset + limit)

    redis_messages =
      if show_acked do
        load_consumer_messages_from_redis(consumer, offset + limit)
      else
        []
      end

    (db_messages ++ redis_messages)
    |> Enum.sort_by(& &1.id, :asc)
    |> Enum.uniq_by(& &1.id)
    |> Enum.drop(offset)
    |> Enum.take(limit)
  end

  defp load_consumer_messages_from_db(consumer, limit) do
    params = [order_by: {:asc, :id}, limit: limit]

    case consumer do
      %{message_kind: :record} ->
        Consumers.list_consumer_records_for_consumer(consumer.id, params)

      %{message_kind: :event} ->
        Consumers.list_consumer_events_for_consumer(consumer.id, params)
    end
  end

  defp load_consumer_messages_from_redis(consumer, limit) do
    case Consumers.AcknowledgedMessages.fetch_messages(consumer.id, limit) do
      {:ok, messages} ->
        messages

      {:error, error} ->
        Logger.error("Failed to load messages from Redis: #{inspect(error)}")
        []
    end
  end

  # Function to fetch message data
  defp fetch_message_data(nil, _consumer) do
    {:error,
     """
     Message not found.

     The message may have been acknowledged and removed from the outbox.
     """}
  end

  defp fetch_message_data(%ConsumerRecord{} = record, %{message_kind: :record} = consumer) do
    case Consumers.put_source_data(consumer, [record]) do
      {:ok, [record]} ->
        {:ok, record.data}

      {:error, error} when is_exception(error) ->
        {:error, Exception.message(error)}

      {:error, error} when is_atom(error) ->
        {:error, Atom.to_string(error)}

      {:error, error} when is_binary(error) ->
        {:error, error}

      {:error, error} ->
        {:error, inspect(error)}
    end
  end

  defp fetch_message_data(%ConsumerEvent{} = event, %{message_kind: :event}) do
    {:ok, event.data}
  end

  defp fetch_message_data(%AcknowledgedMessage{}, _) do
    {:ok, nil}
  end

  # Function to schedule periodic message updates
  defp schedule_update do
    # Adjust the interval as needed
    Process.send_after(self(), :update_messages, 1000)
  end

  # Function to encode messages for the Svelte component
  defp encode_messages(consumer, messages) do
    Enum.map(messages, &encode_message(consumer, &1))
  end

  defp encode_message(consumer, message) do
    case message do
      %ConsumerRecord{} = message ->
        %{
          id: message.id,
          type: "record",
          consumer_id: message.consumer_id,
          commit_lsn: message.commit_lsn,
          ack_id: message.ack_id,
          deliver_count: message.deliver_count,
          last_delivered_at: message.last_delivered_at,
          record_pks: message.record_pks,
          table_oid: message.table_oid,
          not_visible_until: message.not_visible_until,
          inserted_at: message.inserted_at,
          data: message.data,
          trace_id: message.replication_message_trace_id,
          state: get_message_state(consumer, message)
        }

      %ConsumerEvent{} = message ->
        %{
          id: message.id,
          type: "event",
          consumer_id: message.consumer_id,
          commit_lsn: message.commit_lsn,
          ack_id: message.ack_id,
          deliver_count: message.deliver_count,
          last_delivered_at: message.last_delivered_at,
          record_pks: message.record_pks,
          table_oid: message.table_oid,
          not_visible_until: message.not_visible_until,
          inserted_at: message.inserted_at,
          data: message.data,
          trace_id: message.replication_message_trace_id,
          state: get_message_state(consumer, message)
        }

      %AcknowledgedMessage{} = message ->
        %{
          id: message.id,
          type: "acknowledged_message",
          consumer_id: message.consumer_id,
          commit_lsn: message.commit_lsn,
          ack_id: message.ack_id,
          deliver_count: message.deliver_count,
          last_delivered_at: message.last_delivered_at,
          record_pks: message.record_pks,
          table_oid: message.table_oid,
          not_visible_until: message.not_visible_until,
          inserted_at: message.inserted_at,
          data: nil,
          trace_id: message.trace_id,
          state: get_message_state(consumer, message)
        }
    end
  end

  defp get_message_state(_consumer, %AcknowledgedMessage{}), do: "acknowledged"
  defp get_message_state(_consumer, %{deliver_count: 0}), do: "available"

  defp get_message_state(%{ack_wait_ms: ack_wait_ms}, %{
         last_delivered_at: last_delivered_at,
         not_visible_until: not_visible_until
       }) do
    cond do
      DateTime.after?(DateTime.add(last_delivered_at, ack_wait_ms, :millisecond), DateTime.utc_now()) ->
        "delivered"

      DateTime.after?(not_visible_until, DateTime.utc_now()) ->
        "not visible"

      true ->
        "available"
    end
  end
end
