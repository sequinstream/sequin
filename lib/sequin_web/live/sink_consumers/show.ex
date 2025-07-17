defmodule SequinWeb.SinkConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.EnrichmentFunction
  alias Sequin.Consumers.FilterFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.Source
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Health.CheckSinkConfigurationWorker
  alias Sequin.Metrics
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.Trace
  alias Sequin.Transforms.Message
  alias SequinWeb.RouteHelpers

  require Logger

  # For message management
  @messages_page_size 25
  @trace_page_size 25
  @trace_event_limit 25
  @backfills_page_size 25

  @impl Phoenix.LiveView
  def mount(%{"id" => id} = params, _session, socket) do
    current_account = User.current_account(socket.assigns.current_user)
    show_ack_id = params["ack_id"]

    case load_consumer(id, socket) do
      {:ok, consumer} ->
        if connected?(socket) do
          send(self(), :update_health)
          send(self(), :update_metrics)
          send(self(), :update_messages)
          send(self(), :update_backfills)
        end

        # Initialize message-related assigns
        socket =
          socket
          |> assign(:consumer, consumer)
          |> assign(:show_ack_id, show_ack_id)
          |> assign(:api_tokens, ApiTokens.list_tokens_for_account(current_account.id))
          |> assign(:api_base_url, Application.fetch_env!(:sequin, :api_base_url))
          |> assign(:functions, Consumers.list_functions_for_account(current_account.id))
          |> assign_metrics()
          |> assign(:paused, false)
          |> assign(:show_acked, params["showAcked"] == "true")
          |> assign(:page, 0)
          |> assign(:page_size, @messages_page_size)
          |> assign(:total_count, 0)
          |> assign(:cursor_position, nil)
          |> assign(:cursor_task_ref, nil)
          |> assign(:trace, initial_trace())
          # Backfills tab assigns
          |> assign(:backfills_page, 0)
          |> assign(:backfills_page_size, @backfills_page_size)
          |> assign(:backfills_total_count, 0)
          |> assign(:backfills, [])
          |> load_consumer_messages()
          |> load_consumer_message_by_ack_id()
          |> load_consumer_backfills()

        :syn.join(:consumers, {:sink_config_checked, consumer.id}, self())

        {:ok, socket}

      {:error, _error} ->
        {:ok,
         socket
         |> put_flash(:error, "Consumer not found")
         |> push_navigate(to: ~p"/sinks")}
    end
  end

  defp load_consumer(id, socket) do
    with {:ok, consumer} <-
           Consumers.get_sink_consumer_for_account(current_account_id(socket), id) do
      consumer =
        consumer
        |> Repo.preload(
          [
            :postgres_database,
            :active_backfills,
            :replication_slot,
            :transform,
            :enrichment,
            :routing,
            :filter
          ],
          force: true
        )
        |> SinkConsumer.preload_http_endpoint!()
        |> put_health()

      {:ok, consumer}
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

    socket =
      socket
      |> assign(:show_acked, show_acked)
      |> apply_action(socket.assigns.live_action)

    {:noreply, socket}
  end

  defp apply_action(socket, :show) do
    %{consumer: consumer} = socket.assigns
    assign(socket, :page_title, "#{consumer.name} | Sequin")
  end

  defp apply_action(socket, :messages) do
    %{consumer: consumer} = socket.assigns
    assign(socket, :page_title, "#{consumer.name} | Messages | Sequin")
  end

  defp apply_action(socket, :trace) do
    %{consumer: consumer} = socket.assigns
    assign(socket, :page_title, "#{consumer.name} | Trace | Sequin")
  end

  defp apply_action(socket, :backfills) do
    %{consumer: consumer} = socket.assigns
    assign(socket, :page_title, "#{consumer.name} | Backfills | Sequin")
  end

  defp apply_action(socket, _) do
    %{consumer: consumer} = socket.assigns
    assign(socket, :page_title, "#{consumer.name} | Sequin")
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
          <% {:show, %SinkConsumer{}} -> %>
            <!-- ShowHttpPush component -->
            <.svelte
              name="consumers/ShowSink"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  tables: encode_tables(@consumer.postgres_database.tables),
                  parent: "consumer-show",
                  metrics: @metrics,
                  metrics_loading: @metrics_loading,
                  apiBaseUrl: @api_base_url,
                  apiTokens: encode_api_tokens(@api_tokens),
                  transform: encode_function(@consumer.transform)
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
                  apiTokens: encode_api_tokens(@api_tokens),
                  metrics: @metrics,
                  metrics_loading: @metrics_loading,
                  showAckId: @show_ack_id
                }
              }
            />
          <% {:trace, _consumer} -> %>
            <!-- ShowTrace component -->
            <.svelte
              name="consumers/ShowTrace"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  trace: encode_trace(@trace),
                  parent: "consumer-show"
                }
              }
            />
          <% {:backfills, _consumer} -> %>
            <!-- ShowBackfills component -->
            <.svelte
              name="consumers/ShowBackfills"
              props={
                %{
                  consumer: encode_consumer(@consumer),
                  backfills: encode_backfills(@backfills),
                  totalCount: @backfills_total_count,
                  pageSize: @backfills_page_size,
                  page: @backfills_page
                }
              }
            />
        <% end %>
      </div>
    </div>
    """
  end

  # FIXME: TEMP
  defp encode_trace(trace) do
    %{
      events: Enum.map(trace.events, &Trace.Event.to_external/1),
      total_count: trace.total_count,
      page_size: trace.page_size,
      page: trace.page,
      page_count: ceil(trace.total_count / trace.page_size),
      paused: trace.paused
    }
  end

  def handle_event("edit", _params, socket) do
    type = Consumers.kind(socket.assigns.consumer)
    {:noreply, push_navigate(socket, to: ~p"/sinks/#{type}/#{socket.assigns.consumer.id}/edit")}
  end

  @impl Phoenix.LiveView
  def handle_event("delete", _params, socket) do
    case Consumers.delete_sink_consumer(socket.assigns.consumer) do
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
  def handle_event("change_backfills_page", %{"page" => page}, socket) do
    {:noreply,
     socket
     |> assign(backfills_page: page)
     |> load_consumer_backfills()}
  end

  @impl Phoenix.LiveView
  def handle_event("run-backfill", %{"selectedTables" => []}, socket) do
    {:reply, %{ok: true}, socket}
  end

  def handle_event("run-backfill", %{"selectedTables" => selected_tables} = params, socket) do
    consumer = socket.assigns.consumer
    max_timeout_ms = Map.get(params, "maxTimeoutMs", 5000)

    case Consumers.create_backfills_for_form(
           current_account_id(socket),
           consumer,
           selected_tables,
           max_timeout_ms
         ) do
      %{failed: changesets} ->
        # Some failed but some may also have been created successfully
        maybe_start_table_readers(consumer)

        error_messages = Enum.map_join(changesets, "; ", &format_changeset_errors/1)

        {:reply, %{ok: true},
         socket
         |> load_consumer_backfills()
         |> put_flash(:toast, %{
           kind: :error,
           title: "Failed to create #{length(changesets)} backfill(s)",
           description: error_messages
         })}

      %{created: backfills} ->
        maybe_start_table_readers(consumer)

        {:reply, %{ok: true},
         socket
         |> load_consumer_backfills()
         |> put_flash(:toast, %{kind: :success, title: "Created #{length(backfills)} backfill(s)"})}
    end
  end

  # Add a catch-all clause for invalid parameters
  def handle_event("run-backfill", _params, socket) do
    {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Invalid backfill parameters provided"})}
  end

  def handle_event("cancel_backfill", %{"backfill_id" => backfill_id}, socket) do
    update_backfill_state(socket, backfill_id, :cancelled)
  end

  def handle_event("pause_backfill", %{"backfill_id" => backfill_id}, socket) do
    update_backfill_state(socket, backfill_id, :paused)
  end

  def handle_event("resume_backfill", %{"backfill_id" => backfill_id}, socket) do
    update_backfill_state(socket, backfill_id, :active)
  end

  def handle_event("fetch_message_data", %{"message_ack_id" => message_ack_id}, socket) do
    message = Enum.find(socket.assigns.messages, &(&1.ack_id == message_ack_id))

    case fetch_message_data(message, socket.assigns.consumer) do
      {:ok, data} ->
        {:reply, %{data: data}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("fetch_message_logs", %{"trace_id" => trace_id}, socket) do
    account_id = current_account_id(socket)
    consumer_id = socket.assigns.consumer.id

    case Sequin.Logs.get_logs_for_consumer_message(account_id, consumer_id, trace_id) do
      {:ok, logs} ->
        {:reply, %{logs: logs}, socket}

      {:error, reason} ->
        Logger.error("Failed to fetch message logs: #{inspect(reason)}", error: reason)
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("acknowledge_message", %{"ack_id" => ack_id}, socket) do
    consumer = socket.assigns.consumer
    {:ok, messages} = SlotMessageStore.messages_succeeded_returning_messages(consumer, [ack_id])
    {:ok, _count} = Consumers.after_messages_acked(consumer, messages)

    updated_socket =
      socket
      |> load_consumer_messages()
      |> put_flash(:toast, %{kind: :success, title: "Message acknowledged"})

    case Enum.find(socket.assigns.messages, &(&1.ack_id == ack_id)) do
      nil ->
        {:reply, %{ok: true}, updated_socket}

      message ->
        message = AcknowledgedMessages.to_acknowledged_message(message)
        {:reply, %{ok: true, updated_message: encode_message(consumer, message)}, updated_socket}
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

  def handle_event("reset_message_visibility", %{"ack_id" => ack_id}, socket) do
    case SlotMessageStore.reset_message_visibilities(socket.assigns.consumer, [ack_id]) do
      {:ok, []} ->
        Logger.error("Failed to reset message visibility: Message not found", ack_id: ack_id)
        {:reply, %{error: "Message not found"}, load_consumer_messages(socket)}

      {:ok, [updated_message]} ->
        encoded_message = encode_message(socket.assigns.consumer, updated_message)
        {:reply, %{updated_message: encoded_message}, load_consumer_messages(socket)}

      {:error, reason} ->
        Logger.error("Failed to reset message visibility: #{inspect(reason)}",
          error: reason,
          ack_id: ack_id
        )

        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("disable", _params, socket) do
    case Consumers.update_sink_consumer(socket.assigns.consumer, %{status: :disabled}) do
      {:ok, updated_consumer} ->
        {:reply, %{ok: true},
         socket
         |> assign(:consumer, updated_consumer)
         |> put_flash(:toast, %{kind: :success, title: "Consumer disabled"})}

      {:error, error} ->
        Logger.error("Failed to disable consumer: #{inspect(error)}", error: error)

        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to disable consumer"})}
    end
  end

  def handle_event("pause", _params, socket) do
    case Consumers.update_sink_consumer(socket.assigns.consumer, %{status: :paused}) do
      {:ok, updated_consumer} ->
        {:reply, %{ok: true},
         socket
         |> assign(:consumer, updated_consumer)
         |> put_flash(:toast, %{kind: :success, title: "Consumer paused"})}

      {:error, error} ->
        Logger.error("Failed to pause consumer: #{inspect(error)}", error: error)

        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Failed to pause consumer"})}
    end
  end

  def handle_event("enable", _params, socket) do
    case Consumers.update_sink_consumer(socket.assigns.consumer, %{status: :active}) do
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

  @impl Phoenix.LiveView
  def handle_event("refresh_health", _params, socket) do
    CheckSinkConfigurationWorker.enqueue_for_user(socket.assigns.consumer.id)
    consumer = put_health(socket.assigns.consumer)
    {:noreply, assign(socket, :consumer, consumer)}
  end

  def handle_event("refresh_check", %{"slug" => "sink_configuration"}, socket) do
    CheckSinkConfigurationWorker.enqueue_for_user(socket.assigns.consumer.id)
    {:noreply, socket}
  end

  def handle_event("dismiss_check", %{"error_slug" => error_slug}, socket) do
    consumer = socket.assigns.consumer

    event_slug =
      case String.to_existing_atom(error_slug) do
        :replica_identity_not_full ->
          :alert_replica_identity_not_full_dismissed

        :replica_identity_not_full_partitioned ->
          :alert_replica_identity_not_full_dismissed

        :toast_columns_detected ->
          :alert_toast_columns_detected_dismissed

        :invalid_transaction_annotation_received ->
          :invalid_transaction_annotation_received_dismissed

        :load_shedding_policy_discarded ->
          :load_shedding_policy_discarded_dismissed
      end

    Health.put_event(consumer, %Health.Event{slug: event_slug})
    consumer = put_health(consumer)

    {:noreply, assign(socket, :consumer, consumer)}
  end

  def handle_event("reset_all_visibility", _params, socket) do
    consumer = socket.assigns.consumer

    case SlotMessageStore.reset_all_message_visibilities(consumer) do
      :ok ->
        {:reply, %{ok: true},
         socket
         |> load_consumer_messages()
         |> put_flash(:toast, %{kind: :success, title: "Scheduled redelivery for all messages"})}

      {:error, reason} ->
        {:reply, %{ok: false},
         put_flash(socket, :toast, %{
           kind: :error,
           title: "Failed to reset message visibility: #{inspect(reason)}"
         })}
    end
  end

  def handle_event("trace_stop", _params, socket) do
    Trace.unsubscribe(socket.assigns.consumer.id)
    {:noreply, update(socket, :trace, fn trace -> %{trace | paused: true} end)}
  end

  def handle_event("trace_start", _params, socket) do
    Trace.subscribe(socket.assigns.consumer.id)

    # Reset trace state on start
    {:noreply, assign(socket, trace: %{initial_trace() | paused: false})}
  end

  def handle_event("trace_toggle_show_acked", %{"show_acked" => show_acked}, socket) do
    {:noreply,
     socket
     |> assign(:trace_show_acked, show_acked)
     |> assign(:trace_page, 0)
     |> push_patch(to: RouteHelpers.consumer_path(socket.assigns.consumer, "/trace?showAcked=#{show_acked}"))}
  end

  def handle_event("trace_change_page", %{"page" => page}, socket) do
    {:noreply, assign(socket, trace_page: page)}
  end

  @impl Phoenix.LiveView
  def handle_info({:updated_consumer, updated_consumer}, socket) do
    {:noreply,
     socket
     |> assign(consumer: updated_consumer)
     |> push_patch(to: RouteHelpers.consumer_path(updated_consumer))}
  end

  def handle_info(:update_backfills, socket) do
    socket = load_consumer_backfills(socket)

    # Update backfill every 200ms if there is an active backfill, otherwise every second
    case socket.assigns.consumer.active_backfills do
      [] -> Process.send_after(self(), :update_backfills, 1000)
      [_ | _] -> Process.send_after(self(), :update_backfills, 200)
    end

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    consumer = put_health(socket.assigns.consumer)
    {:noreply, assign(socket, :consumer, consumer)}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_metrics, socket) do
    Task.async(fn -> load_metrics(socket.assigns.consumer) end)
    {:noreply, socket}
  end

  def handle_info({ref, metrics}, socket) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    Process.send_after(self(), :update_metrics, 1000)

    {:noreply, assign(socket, metrics: metrics, metrics_loading: false)}
  end

  # The load_metrics/1 task failed
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, socket) do
    Process.send_after(self(), :update_metrics, 200)
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_messages, %{assigns: %{paused: true}} = socket) do
    Process.send_after(self(), :update_messages, 1000)
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_messages, socket) do
    socket = load_consumer_messages(socket)
    Process.send_after(self(), :update_messages, 1000)
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:sink_config_checked, socket) do
    consumer = put_health(socket.assigns.consumer)
    {:noreply, assign(socket, :consumer, consumer)}
  end

  @impl Phoenix.LiveView
  # If the trace is paused, don't add any more events
  def handle_info({:trace_event, _event}, %{assigns: %{trace: %{paused: true}}} = socket) do
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  # If the live action is not :trace, don't add any more events
  def handle_info({:trace_event, _event}, %{assigns: %{live_action: live_action}} = socket) when live_action != :trace do
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info({:trace_event, event}, socket) do
    trace = socket.assigns.trace
    events = [event | trace.events]
    total_count = length(events)
    to_pause? = total_count >= trace.event_limit

    if to_pause? do
      Trace.unsubscribe(socket.assigns.consumer.id)
    end

    {:noreply, assign(socket, trace: %{trace | events: events, total_count: total_count, paused: to_pause?})}
  end

  defp initial_trace do
    %{
      events: [],
      total_count: 0,
      page_size: @trace_page_size,
      page: 0,
      page_count: 0,
      event_limit: @trace_event_limit,
      paused: true
    }
  end

  @smoothing_window 5
  @timeseries_window_count 60
  defp load_metrics(consumer) do
    {:ok, messages_processed_count} = Metrics.get_consumer_messages_processed_count(consumer)

    # Get 60 + @smoothing_window seconds of throughput data
    {:ok, messages_processed_throughput_timeseries} =
      Metrics.get_consumer_messages_processed_throughput_timeseries_smoothed(
        consumer,
        @timeseries_window_count,
        @smoothing_window
      )

    messages_processed_throughput =
      messages_processed_throughput_timeseries
      |> List.last()
      |> Float.ceil()

    {:ok, messages_processed_bytes_timeseries} =
      Metrics.get_consumer_messages_processed_bytes_timeseries_smoothed(
        consumer,
        @timeseries_window_count,
        @smoothing_window
      )

    messages_processed_bytes = List.last(messages_processed_bytes_timeseries)

    messages_failing_count =
      Consumers.count_messages_for_consumer(consumer, delivery_count_gte: 1)

    messages_pending_count =
      case SlotMessageStore.count_messages(consumer) do
        {:ok, count} -> count
        {:error, _} -> 0
      end

    %{
      messages_processed_count: messages_processed_count,
      messages_processed_throughput: messages_processed_throughput,
      messages_processed_bytes: messages_processed_bytes,
      messages_processed_throughput_timeseries: messages_processed_throughput_timeseries,
      messages_processed_bytes_timeseries: messages_processed_bytes_timeseries,
      messages_failing_count: messages_failing_count,
      messages_pending_count: messages_pending_count
    }
  end

  # Initialize metrics with default values
  defp assign_metrics(socket) do
    assign(socket,
      metrics: %{
        messages_processed_count: 0,
        messages_processed_throughput: 0,
        messages_processed_bytes: 0,
        messages_processed_throughput_timeseries: Enum.map(1..@timeseries_window_count, fn _ -> 0 end),
        messages_processed_bytes_timeseries: Enum.map(1..@timeseries_window_count, fn _ -> 0 end),
        messages_failing_count: 0,
        messages_pending_count: 0
      },
      metrics_loading: true
    )
  end

  defp encode_consumer(%SinkConsumer{type: _} = consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      annotations: consumer.annotations,
      kind: :push,
      type: consumer.type,
      status: consumer.status,
      message_kind: consumer.message_kind,
      message_grouping: consumer.message_grouping,
      ack_wait_ms: consumer.ack_wait_ms,
      max_ack_pending: consumer.max_ack_pending,
      max_deliver: consumer.max_deliver,
      max_waiting: consumer.max_waiting,
      max_retry_count: consumer.max_retry_count,
      inserted_at: consumer.inserted_at,
      updated_at: consumer.updated_at,
      database: encode_database(consumer.postgres_database, consumer.replication_slot),
      tables_included_in_source: encode_tables_included_in_source(consumer.source, consumer.postgres_database),
      sink: encode_sink(consumer),
      source: encode_source(consumer.source),
      health: encode_health(consumer),
      href: RouteHelpers.consumer_path(consumer),
      batch_size: consumer.batch_size,
      transform_id: consumer.transform_id,
      routing_id: consumer.routing_id,
      enrichment_id: consumer.enrichment_id,
      enrichment: encode_function(consumer.enrichment),
      routing: encode_function(consumer.routing),
      filter_id: consumer.filter_id,
      filter: encode_function(consumer.filter),
      active_backfills: Enum.map(consumer.active_backfills, &encode_backfill/1)
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
      is_fifo: sink.is_fifo,
      use_task_role: sink.use_task_role
    }
  end

  defp encode_sink(%SinkConsumer{sink: %SnsSink{} = sink}) do
    %{
      type: :sns,
      topic_arn: sink.topic_arn,
      region: sink.region,
      is_fifo: sink.is_fifo,
      use_task_role: sink.use_task_role
    }
  end

  defp encode_sink(%SinkConsumer{sink: %KinesisSink{} = sink}) do
    %{
      type: :kinesis,
      stream_arn: sink.stream_arn,
      region: sink.region,
      use_task_role: sink.use_task_role
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

  defp encode_sink(%SinkConsumer{sink: %RedisStreamSink{} = sink}) do
    default_host = if env() == :dev, do: "localhost"

    %{
      type: :redis_stream,
      host: sink.host || default_host,
      port: sink.port || 6379,
      streamKey: sink.stream_key,
      database: sink.database,
      tls: sink.tls,
      url: RedisStreamSink.redis_url(sink)
    }
  end

  defp encode_sink(%SinkConsumer{sink: %S2Sink{} = sink}) do
    %{
      type: :s2,
      basin: sink.basin,
      stream: sink.stream,
      access_token: sink.access_token,
      dashboard_url: S2Sink.dashboard_url(sink)
    }
  end

  defp encode_sink(%SinkConsumer{sink: %RedisStringSink{} = sink}) do
    default_host = if env() == :dev, do: "localhost"

    %{
      type: :redis_string,
      host: sink.host || default_host,
      port: sink.port || 6379,
      database: sink.database,
      tls: sink.tls,
      url: RedisStringSink.redis_url(sink),
      expireMs: sink.expire_ms
    }
  end

  defp encode_sink(%SinkConsumer{sink: %GcpPubsubSink{} = sink}) do
    %{
      type: :gcp_pubsub,
      project_id: sink.project_id,
      topic_id: sink.topic_id,
      connection_id: sink.connection_id,
      use_emulator: sink.use_emulator,
      emulator_base_url: sink.emulator_base_url
    }
  end

  defp encode_sink(%SinkConsumer{sink: %NatsSink{} = sink}) do
    %{
      type: :nats,
      host: sink.host,
      port: sink.port,
      username: sink.username,
      password: sink.password,
      tls: sink.tls,
      nkey_seed: sink.nkey_seed,
      jwt: sink.jwt,
      connection_id: sink.connection_id
    }
  end

  defp encode_sink(%SinkConsumer{sink: %RabbitMqSink{} = sink} = consumer) do
    database_name = consumer.postgres_database.name

    topic =
      if consumer.message_kind == :event do
        "sequin.changes.#{database_name}.{table-schema}.{table-name}.{action}"
      else
        "sequin.rows.#{database_name}.{table-schema}.{table-name}"
      end

    %{
      type: :rabbitmq,
      host: sink.host,
      port: sink.port,
      exchange: sink.exchange,
      username: sink.username,
      virtual_host: sink.virtual_host,
      tls: sink.tls,
      topic: topic,
      headers: sink.headers
    }
  end

  defp encode_sink(%SinkConsumer{sink: %AzureEventHubSink{} = sink}) do
    %{
      type: :azure_event_hub,
      namespace: sink.namespace,
      event_hub_name: sink.event_hub_name,
      shared_access_key_name: sink.shared_access_key_name,
      shared_access_key: sink.shared_access_key
    }
  end

  defp encode_sink(%SinkConsumer{sink: %TypesenseSink{} = sink}) do
    %{
      type: :typesense,
      endpoint_url: sink.endpoint_url,
      collection_name: sink.collection_name,
      batch_size: sink.batch_size,
      timeout_seconds: sink.timeout_seconds
    }
  end

  defp encode_sink(%SinkConsumer{sink: %MeilisearchSink{} = sink}) do
    %{
      type: :meilisearch,
      endpoint_url: sink.endpoint_url,
      index_name: sink.index_name,
      primary_key: sink.primary_key,
      batch_size: sink.batch_size,
      timeout_seconds: sink.timeout_seconds
    }
  end

  defp encode_sink(%SinkConsumer{sink: %ElasticsearchSink{} = sink}) do
    %{
      type: :elasticsearch,
      endpoint_url: sink.endpoint_url,
      index_name: sink.index_name,
      auth_type: sink.auth_type,
      auth_value: sink.auth_value,
      batch_size: sink.batch_size
    }
  end

  defp encode_sink(%SinkConsumer{sink: %SequinStreamSink{}}) do
    %{type: :sequin_stream}
  end

  defp encode_database(%PostgresDatabase{} = database, %PostgresReplicationSlot{} = slot) do
    %{
      id: database.id,
      name: database.name,
      pg_major_version: database.pg_major_version,
      publication_name: slot.publication_name,
      tables: encode_tables(database.tables)
    }
  end

  defp encode_http_endpoint(%HttpEndpoint{} = http_endpoint) do
    %{
      id: http_endpoint.id,
      url: HttpEndpoint.url(http_endpoint)
    }
  end

  defp encode_source(nil) do
    %{
      include_schemas: nil,
      exclude_schemas: nil,
      include_table_oids: nil,
      exclude_table_oids: nil
    }
  end

  defp encode_source(%Source{} = source) do
    %{
      include_schemas: source.include_schemas,
      exclude_schemas: source.exclude_schemas,
      include_table_oids: source.include_table_oids,
      exclude_table_oids: source.exclude_table_oids
    }
  end

  defp encode_tables_included_in_source(nil, %PostgresDatabase{} = database) do
    encode_tables(database.tables)
  end

  defp encode_tables_included_in_source(%Source{} = source, %PostgresDatabase{} = database) do
    database.tables
    |> Enum.filter(&Source.table_in_source?(source, &1))
    |> encode_tables()
  end

  defp encode_function(nil), do: nil

  defp encode_function(%Function{type: type, function: inner_function} = function) do
    %{
      id: function.id,
      name: function.name,
      description: function.description,
      function: Map.merge(%{type: type}, encode_function_inner(inner_function))
    }
  end

  defp encode_function_inner(%PathFunction{path: path}), do: %{path: path}
  defp encode_function_inner(%TransformFunction{code: code}), do: %{code: code}
  defp encode_function_inner(%FilterFunction{code: code}), do: %{code: code}
  defp encode_function_inner(%EnrichmentFunction{code: code}), do: %{code: code}

  defp encode_function_inner(%RoutingFunction{sink_type: sink_type, code: code}) do
    %{sink_type: sink_type, code: code}
  end

  defp encode_tables(tables) do
    tables
    |> Enum.map(&encode_table/1)
    |> Enum.sort_by(&{&1.schema, &1.name}, :asc)
  end

  defp encode_table(%PostgresDatabaseTable{} = table) do
    %{
      oid: table.oid,
      schema: table.schema,
      name: table.name,
      columns: Enum.map(table.columns, &encode_column/1)
    }
  end

  defp encode_column(%PostgresDatabaseTable.Column{} = column) do
    %{
      name: column.name,
      attnum: column.attnum,
      type: column.type
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

  # Function to load messages for the consumer
  defp load_consumer_messages(%{assigns: %{live_action: action}} = socket) when action != :messages do
    assign(socket, messages: [], total_count: 0)
  end

  defp load_consumer_messages(
         %{assigns: %{consumer: consumer, page: page, page_size: page_size, show_acked: show_acked}} = socket
       ) do
    offset = page * page_size
    store_messages = load_consumer_messages_from_store(consumer, offset + page_size)

    redis_messages =
      if show_acked do
        load_consumer_messages_from_redis(consumer, offset + page_size)
      else
        []
      end

    messages =
      (store_messages ++ redis_messages)
      |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx}, :desc)
      |> Enum.uniq_by(&{&1.commit_lsn, &1.commit_idx})
      |> Enum.drop(offset)
      |> Enum.take(page_size)

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

  defp load_consumer_messages_from_store(consumer, limit) do
    case SlotMessageStore.peek_messages(consumer, limit) do
      messages when is_list(messages) ->
        messages

      {:error, error} ->
        Logger.error("Failed to load messages from store for consumer #{consumer.id}: #{Exception.message(error)}")

        []
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

  defp load_consumer_message_by_ack_id(%{assigns: %{show_ack_id: nil}} = socket), do: socket

  defp load_consumer_message_by_ack_id(%{assigns: %{consumer: consumer, show_ack_id: show_ack_id}} = socket) do
    case SlotMessageStore.peek_message(consumer, show_ack_id) do
      {:ok, message} ->
        update_in(socket.assigns.messages, fn messages -> [message | messages] end)

      {:error, error} ->
        Logger.warning("Failed to load message from store for consumer #{consumer.id}: #{Exception.message(error)}")

        put_flash(socket, :toast, %{
          kind: :error,
          title: "Message with ID #{show_ack_id} not found. It may have been acknowledged or deleted."
        })
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

  defp fetch_message_data(%ConsumerRecord{} = record, %{message_kind: :record}) do
    {:ok, record.data}
  end

  defp fetch_message_data(%ConsumerEvent{} = event, %{message_kind: :event}) do
    {:ok, event.data}
  end

  defp fetch_message_data(%AcknowledgedMessage{}, _) do
    {:ok, nil}
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
          ack_id: message.ack_id,
          commit_lsn: message.commit_lsn,
          commit_timestamp: message.data.metadata.commit_timestamp,
          data: message.data,
          deliver_count: message.deliver_count,
          inserted_at: message.inserted_at,
          last_delivered_at: message.last_delivered_at,
          not_visible_until: message.not_visible_until,
          record_pks: message.record_pks,
          seq: message.commit_lsn + message.commit_idx,
          state: state,
          state_color: get_message_state_color(consumer, state),
          table_name: message.data.metadata.table_name,
          table_schema: message.data.metadata.table_schema,
          table_oid: message.table_oid,
          trace_id: message.replication_message_trace_id,
          functioned_message: maybe_function_message(consumer, message)
        }

      %ConsumerEvent{} = message ->
        %{
          id: message.id,
          type: "event",
          consumer_id: message.consumer_id,
          ack_id: message.ack_id,
          commit_lsn: message.commit_lsn,
          commit_timestamp: message.data.metadata.commit_timestamp,
          data: message.data,
          deliver_count: message.deliver_count,
          inserted_at: message.inserted_at,
          last_delivered_at: message.last_delivered_at,
          not_visible_until: message.not_visible_until,
          record_pks: message.record_pks,
          seq: message.commit_lsn + message.commit_idx,
          state: state,
          state_color: get_message_state_color(consumer, state),
          table_name: message.data.metadata.table_name,
          table_schema: message.data.metadata.table_schema,
          table_oid: message.table_oid,
          trace_id: message.replication_message_trace_id,
          functioned_message: maybe_function_message(consumer, message)
        }

      %AcknowledgedMessage{} = message ->
        %{
          id: message.id,
          type: "acknowledged_message",
          consumer_id: message.consumer_id,
          ack_id: message.ack_id,
          commit_lsn: message.commit_lsn,
          commit_idx: message.commit_idx,
          commit_timestamp: message.commit_timestamp,
          data: nil,
          deliver_count: message.deliver_count,
          inserted_at: message.inserted_at,
          last_delivered_at: message.last_delivered_at,
          not_visible_until: message.not_visible_until,
          record_pks: message.record_pks,
          seq: message.seq,
          state: state,
          state_color: get_message_state_color(consumer, state),
          table_name: message.table_name,
          table_schema: message.table_schema,
          table_oid: message.table_oid,
          trace_id: message.trace_id
        }
    end
  end

  defp maybe_function_message(consumer, message) do
    case consumer.function do
      nil -> nil
      _ -> Message.to_external(consumer, message)
    end
  rescue
    error ->
      "Error functioning message: #{Exception.message(error)}"
  end

  defp get_message_state(%{type: :sequin_stream}, %AcknowledgedMessage{}), do: "acknowledged"
  defp get_message_state(_consumer, %AcknowledgedMessage{state: "discarded"}), do: "discarded"
  defp get_message_state(_consumer, %AcknowledgedMessage{}), do: "delivered"

  defp get_message_state(consumer, %{not_visible_until: not_visible_until, last_delivered_at: last_delivered_at}) do
    now = DateTime.utc_now()

    ack_deadline =
      if last_delivered_at,
        do: DateTime.add(last_delivered_at, consumer.ack_wait_ms, :millisecond)

    cond do
      is_nil(last_delivered_at) ->
        "available"

      # Message is being delivered and within ack window
      (is_nil(not_visible_until) || not DateTime.before?(last_delivered_at, not_visible_until)) &&
          not DateTime.after?(now, ack_deadline) ->
        if consumer.type == :sequin_stream do
          # wording change for sequin stream sink
          "delivered"
        else
          "delivering"
        end

      # Message is backing off (either explicitly set not_visible_until or past ack window)
      (not_visible_until && DateTime.before?(now, not_visible_until)) ||
          DateTime.after?(now, ack_deadline) ->
        "backing off"

      # We're past the not_visible_until time
      true ->
        "pending re-delivery"
    end
  end

  defp get_message_state_color(%{type: :sequin_stream}, state) do
    case state do
      "acknowledged" -> "green"
      "delivered" -> "blue"
      "backing off" -> "yellow"
      _ -> "gray"
    end
  end

  defp get_message_state_color(_consumer, state) do
    case state do
      "delivered" -> "green"
      "backing off" -> "yellow"
      "discarded" -> "red"
      _ -> "gray"
    end
  end

  defp consumer_title(%{sink: %{type: :azure_event_hub}}), do: "Azure Event Hub Sink"
  defp consumer_title(%{sink: %{type: :elasticsearch}}), do: "Elasticsearch Sink"
  defp consumer_title(%{sink: %{type: :gcp_pubsub}}), do: "GCP Pub/Sub Sink"
  defp consumer_title(%{sink: %{type: :http_push}}), do: "Webhook Sink"
  defp consumer_title(%{sink: %{type: :kafka}}), do: "Kafka Sink"
  defp consumer_title(%{sink: %{type: :nats}}), do: "NATS Sink"
  defp consumer_title(%{sink: %{type: :rabbitmq}}), do: "RabbitMQ Sink"
  defp consumer_title(%{sink: %{type: :redis_stream}}), do: "Redis Stream Sink"
  defp consumer_title(%{sink: %{type: :redis_string}}), do: "Redis String Sink"
  defp consumer_title(%{sink: %{type: :sequin_stream}}), do: "Sequin Stream Sink"
  defp consumer_title(%{sink: %{type: :sns}}), do: "SNS Sink"
  defp consumer_title(%{sink: %{type: :kinesis}}), do: "Kinesis Sink"
  defp consumer_title(%{sink: %{type: :s2}}), do: "S2 Sink"
  defp consumer_title(%{sink: %{type: :sqs}}), do: "SQS Sink"
  defp consumer_title(%{sink: %{type: :typesense}}), do: "Typesense Sink"
  defp consumer_title(%{sink: %{type: :meilisearch}}), do: "Meilisearch Sink"

  defp put_health(%SinkConsumer{} = consumer) do
    with {:ok, health} <- Health.health(consumer),
         {:ok, slot_health} <- Health.health(consumer.replication_slot) do
      health = Health.add_slot_health_to_consumer_health(health, slot_health)
      %{consumer | health: health}
    else
      {:error, error} ->
        Logger.error("Failed to load health for consumer: #{inspect(error)}")
        consumer
    end
  end

  defp encode_health(%SinkConsumer{} = consumer) do
    consumer.health
    |> Health.to_external()
    |> Map.update!(:checks, fn checks ->
      Enum.map(checks, fn check ->
        maybe_augment_alert(check, consumer)
      end)
    end)
  end

  defp maybe_augment_alert(%{slug: :messages_delivered, status: :error, extra: %{"ack_id" => ack_id}} = check, consumer) do
    link_id = ~p"/sinks/#{consumer.type}/#{consumer.id}/messages/#{ack_id}?showAcked=false"

    Map.merge(check, %{
      alertTitle: "Error: Message not delivered",
      alertMessage: """
      Messages are failing to be delivered to the destination. See the <a href="#{link_id}">message details</a> of one failing message for more information.
      """,
      refreshable: false,
      dismissable: false
    })
  end

  defp maybe_augment_alert(
         %{slug: :sink_configuration, error_slug: :replica_identity_not_full_partitioned, extra: extra} = check,
         _consumer
       ) do
    %{
      "tables_with_replica_identities" => tables_with_replica_identities
    } = extra

    root_table_names_without_full_replica_identity =
      tables_with_replica_identities
      |> Enum.filter(fn
        %{"relation_kind" => "p", "partition_replica_identities" => partition_replica_identities} ->
          Enum.any?(partition_replica_identities, &(&1["replica_identity"] != "f"))

        _ ->
          false
      end)
      |> Enum.map(fn %{"table_schema" => table_schema, "table_name" => table_name} ->
        ~s("#{table_schema}"."#{table_name}")
      end)
      |> Enum.sort()

    Map.merge(
      check,
      %{
        alertTitle: "Notice: Replica identity not set to full for all tables",
        alertMessage: """
        The replica identity for either your partition root table or one or more of its child tables is not set to `full`. This means the `changes` field in message payloads may be empty.

        If you want the `changes` field to appear in message payloads, run the following SQL command on the root table and on each partition table:
        """,
        refreshable: true,
        dismissable: true,
        code: %{
          language: "sql",
          code:
            Enum.map_join(root_table_names_without_full_replica_identity, "\n", fn table_name ->
              "ALTER TABLE #{table_name} REPLICA IDENTITY FULL;"
            end)
        }
      }
    )
  end

  defp maybe_augment_alert(
         %{slug: :sink_configuration, error_slug: :replica_identity_not_full, extra: extra} = check,
         _consumer
       ) do
    %{
      "tables_with_replica_identities" => tables_with_replica_identities
    } = extra

    table_names_without_full_replica_identity =
      tables_with_replica_identities
      |> Enum.filter(fn %{
                          "relation_kind" => relation_kind,
                          "replica_identity" => replica_identity
                        } ->
        relation_kind == "r" and replica_identity != "f"
      end)
      |> Enum.map(fn %{"table_schema" => table_schema, "table_name" => table_name} ->
        ~s("#{table_schema}"."#{table_name}")
      end)
      |> Enum.sort()

    Map.merge(
      check,
      %{
        alertTitle: "Notice: Replica identity not set to full",
        alertMessage: """
        The replica identity for one or more of your tables is not set to `full`. This means the `changes` field in message payloads will be empty for those tables.

        If you want the `changes` field to appear in message payloads, run the following SQL command:
        """,
        refreshable: true,
        dismissable: true,
        code: %{
          language: "sql",
          code:
            Enum.map_join(table_names_without_full_replica_identity, "\n", fn table_name ->
              "ALTER TABLE #{table_name} REPLICA IDENTITY FULL;"
            end)
        }
      }
    )
  end

  defp maybe_augment_alert(%{slug: :sink_configuration, error_slug: :toast_columns_detected} = check, _consumer) do
    Map.merge(check, %{
      alertTitle: "Notice: TOAST columns detected",
      alertMessage: """
      Some columns in your table use TOAST storage (their values are very large). As currently configured, Sequin will propagate these values as "unchanged_toast" if the column is unchanged.

      To have Sequin always propagate the values of these columns, set replica identity of your table to `full` with the following SQL command:

      ```sql
      alter table {table_name} replica identity full;
      ```
      """,
      refreshable: true,
      dismissable: true
    })
  end

  defp maybe_augment_alert(%{slug: :sink_configuration, error_slug: :table_not_in_publication} = check, consumer) do
    Map.merge(check, %{
      alertTitle: "Error: Table not in publication",
      alertMessage: """
      The table {table_name} is not in the publication #{consumer.replication_slot.publication_name}. That means changes to this table will not be propagated to Sequin.

      To fix this, you can add the table to the publication with the following SQL command:

      For more information on publications, <a href="https://sequinstream.com/docs/reference/databases#publications" target="_blank">see the docs</a>.
      """,
      refreshable: true,
      dismissable: false,
      code: %{
        language: "sql",
        code: """
        ALTER PUBLICATION {publication_name} ADD TABLE {table_name};
        """
      }
    })
  end

  defp maybe_augment_alert(%{error: %{code: :payload_size_limit_exceeded}} = check, _consumer) do
    Map.merge(check, %{
      alertTitle: "Error: Buffer size limit exceeded",
      alertMessage: """
      This sink has reached the maximum number of messages it can buffer. Sinks buffer messages when (1) they can't deliver messages fast enough to the sink, (2) they are paused, or (3) the sink is failing to deliver messages to the sink.

      When a sink reaches its buffer limit, Sequin stops processing new messages from the replication slot. This will impact other sinks that are using the same replication slot.
      """,
      refreshable: false,
      dismissable: false
    })
  end

  defp maybe_augment_alert(%{error_slug: :invalid_transaction_annotation_received} = check, _consumer) do
    Map.merge(
      check,
      %{
        alertTitle: "Notice: Invalid transaction annotation received",
        alertMessage: """
        Sequin was unable to parse the content of one or more transaction annotation messages. The content of these messages should be valid JSON.

        Transaction annotations were not included on these messages.

        Please check your application code to ensure that transaction annotations are being set correctly. See the [docs on annotations](https://sequinstream.com/docs/reference/annotations) for more information.
        """,
        refreshable: false,
        dismissable: true
      }
    )
  end

  defp maybe_augment_alert(%{error_slug: :load_shedding_policy_discarded} = check, _consumer) do
    Map.merge(
      check,
      %{
        alertTitle: "Notice: Messages were discarded for this sink",
        alertMessage: """
        This sink is configured with a [load shedding policy](https://sequinstream.com/docs/reference/sinks/overview#load-shedding-policy) of `discard_on_full`. The sink buffer reached its limit and messages were dropped.

        If the sink is still failing, please address the root cause of messages not being delivered or disable the sink.
        """,
        refreshable: false,
        dismissable: true
      }
    )
  end

  defp maybe_augment_alert(%{error_slug: :http_via_sqs_delivery} = check, _consumer) do
    Map.merge(
      check,
      %{
        alertTitle: "Notice: Webhooks failed to deliver after being pulled from SQS",
        alertMessage: """
        This sink is configured to send payloads to SQS, and then pull async to deliver.
        At least one message is failing to actually deliver to the destination.
        """,
        refreshable: false,
        dismissable: false
      }
    )
  end

  defp maybe_augment_alert(check, _consumer), do: check

  # Function to load backfills for the consumer
  defp load_consumer_backfills(%{assigns: %{live_action: action}} = socket) when action != :backfills do
    consumer = Repo.preload(socket.assigns.consumer, :active_backfills, force: true)
    assign(socket, consumer: consumer, backfills: [], backfills_total_count: 0)
  end

  defp load_consumer_backfills(
         %{assigns: %{consumer: consumer, backfills_page: page, backfills_page_size: page_size}} = socket
       ) do
    offset = page * page_size
    all_backfills = Consumers.list_backfills_for_sink_consumer(consumer.id)

    backfills =
      all_backfills
      |> Enum.drop(offset)
      |> Enum.take(page_size)

    consumer = Repo.preload(socket.assigns.consumer, :active_backfills, force: true)

    socket
    |> assign(:consumer, consumer)
    |> assign(:backfills, backfills)
    |> assign(:backfills_total_count, length(all_backfills))
  end

  # Function to encode backfills for the Svelte component
  defp encode_backfills(backfills) do
    Enum.map(backfills, &encode_backfill/1)
  end

  defp encode_backfill(%Backfill{} = backfill) do
    backfill = Repo.preload(backfill, sink_consumer: [:postgres_database])

    table =
      Enum.find(backfill.sink_consumer.postgres_database.tables, &(&1.oid == backfill.table_oid))

    table_name = if table, do: "#{table.schema}.#{table.name}", else: "Unknown table"

    %{
      id: backfill.id,
      state: backfill.state,
      table_name: table_name,
      rows_initial_count: backfill.rows_initial_count,
      rows_processed_count: backfill.rows_processed_count,
      rows_ingested_count: backfill.rows_ingested_count,
      completed_at: backfill.completed_at,
      canceled_at: backfill.canceled_at,
      inserted_at: backfill.inserted_at,
      failed_at: backfill.failed_at,
      updated_at: backfill.updated_at,
      progress: calculate_backfill_progress(backfill),
      error: if(backfill.error, do: Exception.message(backfill.error))
    }
  end

  defp calculate_backfill_progress(%Backfill{rows_initial_count: nil}), do: nil
  defp calculate_backfill_progress(%Backfill{rows_initial_count: 0}), do: 100.0

  defp calculate_backfill_progress(%Backfill{rows_initial_count: initial, rows_processed_count: processed}) do
    processed / initial * 100.0
  end

  defp maybe_start_table_readers(consumer) do
    if env() != :test do
      consumer = Repo.preload(consumer, :active_backfills, force: true)
      Sequin.Runtime.Supervisor.maybe_start_table_readers(consumer)
    end
  end

  defp update_backfill_state(socket, backfill_id, target_state) do
    consumer_id = socket.assigns.consumer.id

    with {:ok, backfill} <- Consumers.get_backfill_for_sink_consumer(consumer_id, backfill_id),
         {:ok, _updated_backfill} <- Consumers.update_backfill(backfill, %{state: target_state}) do
      {:reply, %{ok: true},
       socket
       |> put_flash(:toast, %{kind: :success, title: "Backfill #{format_state_action(target_state)} successfully"})
       |> load_consumer_backfills()}
    else
      {:error, %NotFoundError{}} ->
        {:reply, %{ok: false}, put_flash(socket, :toast, %{kind: :error, title: "Backfill not found"})}

      {:error, %Ecto.Changeset{} = changeset} ->
        error_message = format_changeset_errors(changeset)

        {:reply, %{ok: false},
         put_flash(socket, :toast, %{
           kind: :error,
           title: "Failed to #{format_state_action(target_state)} backfill",
           description: error_message
         })}

      {:error, error} ->
        error_message = format_error(error)

        {:reply, %{ok: false},
         put_flash(socket, :toast, %{
           kind: :error,
           title: "Failed to #{format_state_action(target_state)} backfill",
           description: error_message
         })}
    end
  end

  defp format_state_action(:cancelled), do: "cancel"
  defp format_state_action(:paused), do: "pause"
  defp format_state_action(:active), do: "resume"

  defp env do
    Application.get_env(:sequin, :env)
  end

  defp format_changeset_errors(changeset) do
    errors = Sequin.Error.errors_on(changeset)

    case errors do
      %{} when map_size(errors) == 0 ->
        "Validation error occurred"

      errors ->
        Enum.map_join(errors, "; ", fn {field, messages} ->
          "#{field}: #{Enum.join(messages, ", ")}"
        end)
    end
  end

  defp format_error(error) when is_binary(error), do: error
  defp format_error(error) when is_atom(error), do: to_string(error)
  defp format_error(%{message: message}), do: message
  defp format_error(error), do: inspect(error)
end
