defmodule SequinWeb.DatabasesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo
  alias Sequin.Tracer
  alias Sequin.Tracer.Server

  # Add this alias

  @impl Phoenix.LiveView
  def mount(%{"id" => id} = params, _session, socket) do
    account_id = current_account_id(socket)

    case Databases.get_db_for_account(account_id, id) do
      {:ok, database} ->
        database = Repo.preload(database, replication_slot: [:http_pull_consumers, :http_push_consumers])

        # Fetch initial health
        {:ok, health} = Health.get(database)
        database = Map.put(database, :health, health)

        socket = assign(socket, database: database, refreshing_tables: false)
        socket = assign_metrics(socket)

        # Messsages
        consumers = Consumers.list_consumers_for_account(account_id)
        paused = params["paused"] == "true"
        page = String.to_integer(params["page"] || "1")
        per_page = 50
        trace_state = get_trace_state(account_id)

        socket =
          assign(socket,
            trace_state: trace_state,
            consumers: consumers,
            paused: paused,
            page: page,
            per_page: per_page,
            tables: database.tables
          )

        if connected?(socket) do
          Tracer.DynamicSupervisor.start_for_account(account_id)

          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1000)
          Process.send_after(self(), :update_messages, 1000)
        end

        {:ok, socket}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    paused = params["paused"] == "true"
    page = String.to_integer(params["page"] || "1")

    {:noreply,
     socket
     |> assign(params: params, paused: paused, page: page)
     |> apply_action(socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :show, _params) do
    assign(socket, :page_title, "Show Database")
  end

  defp apply_action(socket, :edit, _params) do
    assign(socket, :page_title, "Edit Database")
  end

  defp apply_action(socket, :messages, _params) do
    assign(socket, :page_title, "Messages")
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_tables", _, socket) do
    %{database: database} = socket.assigns

    case Databases.update_tables(database) do
      {:ok, updated_database} ->
        {:reply, %{}, assign(socket, database: updated_database)}

      {:error, _reason} ->
        {:reply, %{}, socket}
    end
  end

  def handle_event("delete_database", _, socket) do
    %{database: database} = socket.assigns

    case Databases.delete_db_with_replication_slot(database) do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: ~p"/databases")}

      {:error, error} ->
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("edit", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/databases/#{socket.assigns.database.id}/edit")}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 10_000)

    case Health.get(socket.assigns.database) do
      {:ok, health} ->
        updated_database = Map.put(socket.assigns.database, :health, health)
        {:noreply, assign(socket, database: updated_database)}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  def handle_info(:update_messages, %{assigns: %{paused: true}} = socket) do
    Process.send_after(self(), :update_messages, 1000)
    {:noreply, socket}
  end

  def handle_info(:update_messages, %{assigns: %{paused: false}} = socket) do
    account_id = current_account_id(socket)

    consumers = Consumers.list_consumers_for_account(account_id)
    trace_state = get_trace_state(account_id)

    Process.send_after(self(), :update_messages, 1000)

    {:noreply, assign(socket, consumers: consumers, trace_state: trace_state)}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 1000)
    {:noreply, assign_metrics(socket)}
  end

  def handle_info({ref, {:ok, updated_db}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, database: updated_db, refreshing_tables: false)}
  end

  def handle_info({:updated_database, updated_database}, socket) do
    {:noreply,
     socket
     |> assign(database: updated_database)
     |> push_patch(to: ~p"/databases/#{updated_database.id}")}
  end

  def handle_info({ref, {:error, _reason}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, refreshing_tables: false)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    active_tab =
      case assigns.live_action do
        :messages -> "messages"
        :wal_pipelines -> "wal_pipelines"
        _ -> "overview"
      end

    assigns =
      assigns
      |> assign(:parent, "database-show")
      |> assign(:active_tab, active_tab)

    ~H"""
    <div id={@parent}>
      <.svelte
        name="databases/ShowHeader"
        props={%{database: encode_database(@database), parent: @parent, activeTab: @active_tab}}
      />
      <%= case @live_action do %>
        <% :edit -> %>
          <.live_component
            module={SequinWeb.Live.Databases.Form}
            id="edit-database"
            database={@database}
            on_finish={&handle_edit_finish/1}
            current_account={@current_account}
          />
        <% :show -> %>
          <.svelte
            name="databases/Show"
            props={%{database: encode_database(@database), parent: @parent, metrics: @metrics}}
          />
        <% :messages -> %>
          <.svelte
            name="databases/Messages"
            props={
              %{
                trace_state: encode_trace_state(assigns),
                consumers: encode_consumers(@consumers),
                database: encode_database(@database),
                tables: encode_tables(@tables),
                paused: @paused
              }
            }
          />
      <% end %>
    </div>
    """
  end

  defp assign_metrics(socket) do
    database = socket.assigns.database

    avg_latency =
      case Metrics.get_database_avg_latency(database) do
        {:ok, nil} -> nil
        {:ok, avg_latency} -> round(avg_latency)
        {:error, _} -> nil
      end

    metrics = %{
      avg_latency: avg_latency
    }

    assign(socket, :metrics, metrics)
  end

  defp handle_edit_finish(updated_database) do
    send(self(), {:updated_database, updated_database})
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      hostname: database.hostname,
      port: database.port,
      database: database.database,
      username: database.username,
      ssl: database.ssl,
      pool_size: database.pool_size,
      queue_interval: database.queue_interval,
      queue_target: database.queue_target,
      tables: encode_tables(database.tables),
      tables_refreshed_at: database.tables_refreshed_at,
      inserted_at: database.inserted_at,
      updated_at: database.updated_at,
      consumers:
        encode_consumers(database.replication_slot.http_pull_consumers, database) ++
          encode_consumers(database.replication_slot.http_push_consumers, database),
      health: Health.to_external(database.health)
    }
  end

  defp encode_consumers(consumers, database) do
    Enum.map(consumers, fn consumer ->
      %{
        id: consumer.id,
        consumer_kind: if(consumer.__struct__ == Sequin.Consumers.HttpPushConsumer, do: :http_push, else: :http_pull),
        name: consumer.name,
        message_kind: consumer.message_kind,
        source_tables: Consumers.enrich_source_tables(consumer.source_tables, database)
      }
    end)
  end

  defp encode_tables(tables) do
    Enum.map(tables, fn table ->
      %{
        schema: table.schema,
        name: table.name
      }
    end)
  end

  defp enrich_trace_state(account_id, state) do
    databases = Databases.list_dbs_for_account(account_id)
    consumers = Consumers.list_consumers_for_account(account_id)

    update_in(state.message_traces, fn message_traces ->
      message_traces
      |> Enum.map(fn message_trace ->
        database = Enum.find(databases, &(&1.id == message_trace.database_id))

        message_trace
        |> Map.put(:database, database)
        |> Map.update!(:consumer_traces, fn consumer_traces ->
          consumer_traces
          |> Enum.map(fn consumer_trace ->
            consumer = Enum.find(consumers, &(&1.id == consumer_trace.consumer_id))
            Map.put(consumer_trace, :consumer, consumer)
          end)
          |> Enum.filter(& &1.consumer)
        end)
      end)
      |> Enum.filter(& &1.database)
    end)
  end

  defp encode_trace_state(%{trace_state: nil}), do: %{}

  defp encode_trace_state(assigns) do
    message_traces =
      assigns.trace_state.message_traces
      |> Enum.flat_map(&encode_message_trace/1)
      |> Enum.filter(&filter_trace?(&1, assigns.params))

    total_count = length(message_traces)
    message_traces = Enum.slice(message_traces, (assigns.page - 1) * assigns.per_page, assigns.per_page)

    %{
      message_traces: message_traces,
      started_at: assigns.trace_state.started_at,
      total_count: total_count
    }
  end

  defp get_primary_keys(nil), do: []

  defp get_primary_keys(table) do
    Enum.filter(table.columns, & &1.is_pk?)
  end

  defp encode_primary_keys(ids, primary_keys) do
    ids |> Enum.zip(primary_keys) |> Enum.map_join(", ", fn {id, pk} -> "#{pk.name}: #{id}" end)
  end

  defp find_table(database, table_oid) do
    Enum.find(database.tables, &(&1.oid == table_oid))
  end

  defp encode_message_trace(message_trace) do
    table = find_table(message_trace.database, message_trace.message.table_oid)
    primary_keys = get_primary_keys(table)

    Enum.map(message_trace.consumer_traces, fn consumer_trace ->
      %{
        date: message_trace.message.commit_timestamp,
        consumer_id: consumer_trace.consumer_id,
        consumer: %{
          id: consumer_trace.consumer.id,
          name: consumer_trace.consumer.name
        },
        database: %{
          id: message_trace.database.id,
          name: message_trace.database.name
        },
        table: encode_table(table),
        primary_keys: encode_primary_keys(message_trace.message.ids, primary_keys),
        state: get_message_state([consumer_trace]),
        spans: [
          %{
            type: "replicated",
            timestamp: message_trace.replicated_at,
            duration: DateTime.diff(message_trace.replicated_at, message_trace.message.commit_timestamp, :millisecond)
          }
          | consumer_trace.spans
            |> Enum.map(fn span ->
              %{
                type: span.type,
                timestamp: span.timestamp,
                duration: span.duration
                # Add more span details as needed
              }
            end)
            |> Enum.reverse()
        ],
        trace_id: message_trace.message.trace_id,
        span_types: Enum.map(consumer_trace.spans, & &1.type)
      }
    end)
  end

  defp get_message_state(consumer_traces) do
    states =
      Enum.flat_map(consumer_traces, fn ct ->
        Enum.map(ct.spans, & &1.type)
      end)

    cond do
      :acked in states -> "Acked"
      :received in states -> "Received"
      :filtered in states -> "Filtered"
      :ingested in states -> "Ingested"
      true -> "Unknown"
    end
  end

  defp encode_consumers(consumers) do
    Enum.map(consumers, &%{id: &1.id, name: &1.name})
  end

  defp encode_table(nil), do: nil
  defp encode_table(%{name: name}), do: name

  defp get_trace_state(account_id) do
    case Server.get_state(account_id) do
      %Sequin.Tracer.State{} = state -> enrich_trace_state(account_id, state)
      {:error, _reason} -> nil
    end
  end

  defp filter_trace?(trace, params) do
    database_match?(trace, params["database"]) and
      consumer_match?(trace, params["consumer"]) and
      table_match?(trace, params["table"]) and
      state_match?(trace, params["state"])
  end

  defp database_match?(_trace, nil), do: true
  defp database_match?(trace, database_id), do: trace.database.id == database_id

  defp consumer_match?(_trace, nil), do: true
  defp consumer_match?(trace, consumer_id), do: trace.consumer_id == consumer_id

  defp table_match?(_trace, nil), do: true
  defp table_match?(trace, table), do: trace.table == table

  defp state_match?(_trace, nil), do: true
  defp state_match?(trace, state), do: String.downcase(trace.state) == String.downcase(state)
end
