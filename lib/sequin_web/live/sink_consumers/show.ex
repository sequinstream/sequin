defmodule SequinWeb.SinkConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo
  alias SequinWeb.Components.ConsumerForm
  alias SequinWeb.RouteHelpers

  require Logger

  # For message management
  @page_size 25

  @impl Phoenix.LiveView
  def mount(%{"id" => id} = params, _session, socket) do
    current_account = User.current_account(socket.assigns.current_user)

    case load_consumer(id, socket) do
      {:ok, consumer} ->
        if connected?(socket) do
          send(self(), :update_health)
          send(self(), :update_metrics)
          send(self(), :update_messages)
          send(self(), :update_backfill)
        end

        last_completed_backfill =
          Consumers.find_backfill(consumer.id, state: :completed, limit: 1, order_by: [desc: :completed_at])

        # Initialize message-related assigns
        socket =
          socket
          |> assign(:consumer, consumer)
          |> assign(:last_completed_backfill, last_completed_backfill)
          |> assign(:api_tokens, ApiTokens.list_tokens_for_account(current_account.id))
          |> assign(:api_base_url, Application.fetch_env!(:sequin, :api_base_url))
          |> assign_metrics()
          |> assign(:paused, false)
          |> assign(:show_acked, params["showAcked"] == "true")
          |> assign(:page, 0)
          |> assign(:page_size, @page_size)
          |> assign(:total_count, 0)
          |> assign(:cursor_position, nil)
          |> assign(:cursor_task_ref, nil)
          |> load_consumer_messages()

        {:ok, socket}

      {:error, _error} ->
        {:ok,
         socket
         |> put_flash(:error, "Consumer not found")
         |> push_navigate(to: ~p"/sinks")}
    end
  end

  defp load_consumer(id, socket) do
    with {:ok, consumer} <- Consumers.get_sink_consumer_for_account(current_account_id(socket), id) do
      consumer =
        consumer
        |> Repo.preload([:postgres_database, :sequence, :active_backfill], force: true)
        |> SinkConsumer.preload_http_endpoint()

      {:ok, health} = Health.get(consumer)
      {:ok, %{consumer | health: health}}
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
        name="consumers/ShowSinkHeader"
        props={
          %{
            consumer: encode_consumer(@consumer),
            consumerTitle: consumer_title(@consumer),
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
              module={ConsumerForm}
              id="edit-consumer"
              consumer={@consumer}
              on_finish={&handle_edit_finish/1}
              current_user={@current_user}
            />
          <% {:show, %SinkConsumer{}} -> %>
            <!-- ShowHttpPush component -->
            <.svelte
              name="consumers/ShowSink"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  parent: "consumer-show",
                  metrics: @metrics,
                  cursor_position: encode_backfill(@consumer, @last_completed_backfill),
                  apiBaseUrl: @api_base_url,
                  apiTokens: encode_api_tokens(@api_tokens)
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
                  showAcked: @show_acked,
                  apiBaseUrl: @api_base_url,
                  apiTokens: encode_api_tokens(@api_tokens)
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
    type = Consumers.kind(socket.assigns.consumer)
    {:noreply, push_patch(socket, to: ~p"/sinks/#{type}/#{socket.assigns.consumer.id}/edit")}
  end

  @impl Phoenix.LiveView
  def handle_event("delete", _params, socket) do
    case Consumers.delete_consumer_with_lifecycle(socket.assigns.consumer) do
      {:ok, _deleted_consumer} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :success, title: "Consumer deleted."})
         |> push_navigate(to: ~p"/sinks")}

      {:error, _changeset} ->
        {:noreply, push_toast(socket, %{kind: :error, title: "Failed to delete consumer. Please try again."})}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("pause_updates", _params, socket) do
    {:noreply, assign(socket, paused: true)}
  end

  @impl Phoenix.LiveView
  def handle_event("resume_updates", _params, socket) do
    {:noreply, assign(socket, paused: false)}
  end

  @impl Phoenix.LiveView
  def handle_event("change_page", %{"page" => page}, socket) do
    {:noreply,
     socket
     |> assign(page: page)
     |> load_consumer_messages()}
  end

  @impl Phoenix.LiveView
  def handle_event("run-backfill", %{"new_cursor_position" => new_cursor_position}, socket)
      when is_nil(new_cursor_position) or is_binary(new_cursor_position) or is_integer(new_cursor_position) do
    consumer = socket.assigns.consumer
    table = find_table_by_oid(consumer.sequence.table_oid, consumer.postgres_database.tables)
    table = %PostgresDatabaseTable{table | sort_column_attnum: consumer.sequence.sort_column_attnum}

    initial_min_cursor =
      if new_cursor_position do
        KeysetCursor.min_cursor(table, new_cursor_position)
      else
        KeysetCursor.min_cursor(table)
      end

    backfill_attrs = %{
      account_id: current_account_id(socket),
      sink_consumer_id: consumer.id,
      initial_min_cursor: initial_min_cursor,
      state: :active
    }

    case Consumers.create_backfill_with_lifecycle(backfill_attrs) do
      {:ok, _backfill} ->
        {:reply, %{ok: true}, put_flash(socket, :toast, %{kind: :success, title: "Backfill started successfully"})}

      {:error, error} ->
        Logger.error("Failed to start backfill: #{inspect(error)}", error: error)
        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to start backfill"})}
    end
  end

  # Add a catch-all clause for invalid parameters
  def handle_event("run-backfill", _params, socket) do
    {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Invalid backfill parameters provided"})}
  end

  @impl Phoenix.LiveView
  def handle_event("cancel-backfill", _params, socket) do
    case socket.assigns.consumer.active_backfill do
      nil ->
        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "No active backfill to cancel"})}

      backfill ->
        case Consumers.update_backfill_with_lifecycle(backfill, %{state: :cancelled}) do
          {:ok, _updated_backfill} ->
            {:reply, %{ok: true}, put_flash(socket, :toast, %{kind: :success, title: "Backfill cancelled successfully"})}

          {:error, _error} ->
            {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to cancel backfill"})}
        end
    end
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

  def handle_event("disable", _params, socket) do
    case Consumers.update_consumer_with_lifecycle(socket.assigns.consumer, %{status: :disabled}) do
      {:ok, updated_consumer} ->
        {:reply, %{ok: true},
         socket
         |> assign(:consumer, updated_consumer)
         |> put_flash(:toast, %{kind: :success, title: "Consumer paused"})}

      {:error, error} ->
        Logger.error("Failed to disable consumer: #{inspect(error)}", error: error)
        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to disable consumer"})}
    end
  end

  def handle_event("enable", _params, socket) do
    case Consumers.update_consumer_with_lifecycle(socket.assigns.consumer, %{status: :active}) do
      {:ok, updated_consumer} ->
        {:reply, %{ok: true},
         socket
         |> assign(:consumer, updated_consumer)
         |> put_flash(:toast, %{kind: :success, title: "Consumer resumed"})}

      {:error, error} ->
        Logger.error("Failed to enable consumer: #{inspect(error)}", error: error)
        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to enable consumer"})}
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

  def handle_info(:update_backfill, socket) do
    # Update backfill every 200ms if there is an active backfill, otherwise every second
    case socket.assigns.consumer.active_backfill do
      nil -> Process.send_after(self(), :update_backfill, 1000)
      %Backfill{} -> Process.send_after(self(), :update_backfill, 200)
    end

    case load_consumer(socket.assigns.consumer.id, socket) do
      {:ok, consumer} ->
        last_completed_backfill =
          Consumers.find_backfill(consumer.id, state: :completed, limit: 1, order_by: [desc: :completed_at])

        {:noreply,
         socket
         |> assign(:consumer, consumer)
         |> assign(:last_completed_backfill, last_completed_backfill)}

      {:error, _} ->
        {:noreply, socket}
    end
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

  defp encode_consumer(%SinkConsumer{type: _} = consumer) do
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
      sink: encode_sink(consumer),
      sequence: encode_sequence(consumer.sequence, consumer.sequence_filter, consumer.postgres_database),
      postgres_database: encode_postgres_database(consumer.postgres_database),
      health: Health.to_external(consumer.health),
      href: RouteHelpers.consumer_path(consumer),
      group_column_names: encode_group_column_names(consumer),
      batch_size: consumer.batch_size
    }
  end

  defp encode_sink(%SinkConsumer{sink: %HttpPushSink{} = sink}) do
    %{
      type: :http_push,
      http_endpoint: encode_http_endpoint(sink.http_endpoint),
      http_endpoint_path: sink.http_endpoint_path
    }
  end

  defp encode_sink(%SinkConsumer{sink: %SqsSink{} = sink}) do
    %{
      type: :sqs,
      queue_url: sink.queue_url,
      region: sink.region,
      is_fifo: sink.is_fifo
    }
  end

  defp encode_sink(%SinkConsumer{sink: %KafkaSink{} = sink}) do
    %{
      type: :kafka,
      kafka_url: KafkaSink.kafka_url(sink),
      hosts: sink.hosts,
      username: sink.username,
      password: sink.password,
      topic: sink.topic,
      tls: sink.tls,
      sasl_mechanism: sink.sasl_mechanism
    }
  end

  defp encode_sink(%SinkConsumer{sink: %RedisSink{} = sink}) do
    %{
      type: :redis,
      host: sink.host,
      port: sink.port,
      streamKey: sink.stream_key,
      database: sink.database,
      tls: sink.tls,
      url: RedisSink.redis_url(sink)
    }
  end

  defp encode_sink(%SinkConsumer{sink: %GcpPubsubSink{} = sink}) do
    %{
      type: :gcp_pubsub,
      project_id: sink.project_id,
      topic_id: sink.topic_id,
      connection_id: sink.connection_id
    }
  end

  defp encode_sink(%SinkConsumer{sink: %NatsSink{} = sink}) do
    %{
      type: :nats,
      host: sink.host,
      port: sink.port,
      connection_id: sink.connection_id
    }
  end

  defp encode_sink(%SinkConsumer{sink: %RabbitMqSink{} = sink} = consumer) do
    database_name = consumer.postgres_database.name
    schema_name = consumer.sequence.table_schema
    table_name = consumer.sequence.table_name

    topic =
      if consumer.message_kind == :event do
        "sequin.changes.#{database_name}.#{schema_name}.#{table_name}.{action}"
      else
        "sequin.rows.#{database_name}.#{schema_name}.#{table_name}"
      end

    %{
      type: :rabbitmq,
      host: sink.host,
      port: sink.port,
      exchange: sink.exchange,
      username: sink.username,
      virtual_host: sink.virtual_host,
      tls: sink.tls,
      topic: topic
    }
  end

  defp encode_sink(%SinkConsumer{sink: %SequinStreamSink{}}) do
    %{type: :sequin_stream}
  end

  defp encode_http_endpoint(%HttpEndpoint{} = http_endpoint) do
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
      table_name: table.name,
      table_schema: table.schema,
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

  defp encode_api_tokens(api_tokens) when is_list(api_tokens) do
    Enum.map(api_tokens, fn api_token ->
      %{
        id: api_token.id,
        name: api_token.name,
        inserted_at: api_token.inserted_at,
        token: api_token.token
      }
    end)
  end

  defp encode_group_column_names(%{
         sequence: %Sequence{} = sequence,
         sequence_filter: %SequenceFilter{group_column_attnums: nil},
         postgres_database: %PostgresDatabase{} = postgres_database
       }) do
    case find_table_by_oid(sequence.table_oid, postgres_database.tables) do
      %PostgresDatabaseTable{} = table ->
        table.columns
        |> Enum.filter(& &1.is_pk?)
        |> Enum.sort_by(& &1.attnum)
        |> Enum.map(& &1.name)

      nil ->
        []
    end
  end

  defp encode_group_column_names(%{
         sequence: %Sequence{} = sequence,
         sequence_filter: %SequenceFilter{group_column_attnums: group_column_attnums},
         postgres_database: %PostgresDatabase{} = postgres_database
       }) do
    case find_table_by_oid(sequence.table_oid, postgres_database.tables) do
      %PostgresDatabaseTable{} = table ->
        table.columns
        |> Enum.filter(&(&1.attnum in group_column_attnums))
        |> Enum.sort_by(& &1.attnum)
        |> Enum.map(& &1.name)

      nil ->
        []
    end
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
    state = get_message_state(consumer, message)

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
          state: state,
          state_color: get_message_state_color(consumer, state)
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
          state: state,
          state_color: get_message_state_color(consumer, state)
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
          state: state,
          state_color: "green"
        }
    end
  end

  defp encode_backfill(consumer, last_completed_backfill) do
    sort_column_attnum = consumer.sequence.sort_column_attnum
    table = Sequin.Enum.find!(consumer.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))
    column = Sequin.Enum.find!(table.columns, &(&1.attnum == sort_column_attnum))

    case consumer.active_backfill do
      %Backfill{state: :active} = backfill ->
        %{
          is_backfilling: true,
          cursor_type: column.type,
          backfill: %{
            id: backfill.id,
            state: backfill.state,
            rows_initial_count: backfill.rows_initial_count,
            rows_processed_count: backfill.rows_processed_count,
            rows_ingested_count: backfill.rows_ingested_count,
            completed_at: backfill.completed_at,
            canceled_at: backfill.canceled_at,
            inserted_at: backfill.inserted_at
          },
          last_completed_backfill:
            last_completed_backfill &&
              %{
                rows_processed_count: last_completed_backfill.rows_processed_count,
                rows_ingested_count: last_completed_backfill.rows_ingested_count,
                completed_at: last_completed_backfill.completed_at,
                inserted_at: last_completed_backfill.inserted_at
              }
        }

      _ ->
        %{
          is_backfilling: false,
          cursor_type: column.type,
          last_completed_backfill:
            last_completed_backfill &&
              %{
                rows_processed_count: last_completed_backfill.rows_processed_count,
                rows_ingested_count: last_completed_backfill.rows_ingested_count,
                completed_at: last_completed_backfill.completed_at,
                inserted_at: last_completed_backfill.inserted_at
              }
        }
    end
  end

  defp get_message_state(_consumer, %AcknowledgedMessage{}), do: "delivered"
  defp get_message_state(_consumer, %{deliver_count: 0}), do: "not delivered"

  defp get_message_state(consumer, %{not_visible_until: not_visible_until, state: state}) do
    cond do
      state == :delivered and consumer.type == :sequin_stream ->
        "delivered"

      state == :delivered ->
        "delivering"

      DateTime.after?(not_visible_until, DateTime.utc_now()) ->
        "backing off"

      true ->
        "pending re-delivery"
    end
  end

  defp get_message_state_color(%{type: :sequin_stream}, state) do
    case state do
      "delivered" -> "blue"
      "backing off" -> "yellow"
      _ -> "gray"
    end
  end

  defp get_message_state_color(_consumer, state) do
    case state do
      "delivered" -> "green"
      "backing off" -> "yellow"
      _ -> "gray"
    end
  end

  defp consumer_title(%{sink: %{type: :http_push}}), do: "Webhook Sink"
  defp consumer_title(%{sink: %{type: :sqs}}), do: "SQS Sink"
  defp consumer_title(%{sink: %{type: :redis}}), do: "Redis Sink"
  defp consumer_title(%{sink: %{type: :kafka}}), do: "Kafka Sink"
  defp consumer_title(%{sink: %{type: :sequin_stream}}), do: "Sequin Stream Sink"
  defp consumer_title(%{sink: %{type: :gcp_pubsub}}), do: "GCP Pub/Sub Sink"
  defp consumer_title(%{sink: %{type: :nats}}), do: "NATS Sink"
  defp consumer_title(%{sink: %{type: :rabbitmq}}), do: "RabbitMQ Sink"
end
