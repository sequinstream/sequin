defmodule SequinWeb.TracerLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Tracer
  alias Sequin.Tracer.Server
  alias Sequin.Tracer.State

  @impl true
  def mount(params, _session, socket) do
    account_id = current_account_id(socket)

    if connected?(socket) do
      Tracer.Supervisor.start_for_account(account_id)
      schedule_update()
    end

    consumers = Consumers.list_consumers_for_account(account_id)
    databases = Databases.list_dbs_for_account(account_id)
    tables = Enum.flat_map(databases, & &1.tables)

    paused = params["paused"] == "true"
    page = String.to_integer(params["page"] || "1")
    per_page = 50

    trace_state = get_trace_state(account_id)

    {:ok,
     assign(socket,
       account_id: account_id,
       trace_state: trace_state,
       consumers: consumers,
       databases: databases,
       tables: tables,
       params: params,
       paused: paused,
       page: page,
       per_page: per_page
     )}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    paused = params["paused"] == "true"
    page = String.to_integer(params["page"] || "1")

    {:noreply, assign(socket, params: params, paused: paused, page: page)}
  end

  @impl true
  def handle_info(:update, %{assigns: %{paused: true}} = socket) do
    schedule_update()
    {:noreply, socket}
  end

  @impl true
  def handle_info(:update, %{assigns: %{paused: false}} = socket) do
    account_id = current_account_id(socket)
    Server.update_heartbeat(account_id)

    consumers = Consumers.list_consumers_for_account(account_id)
    trace_state = get_trace_state(account_id)

    schedule_update()

    {:noreply, assign(socket, consumers: consumers, trace_state: trace_state)}
  end

  @impl true
  def handle_event("pause_updates", _params, socket) do
    {:noreply, push_patch(socket, to: update_url_with_paused(socket, true), replace: true)}
  end

  @impl true
  def handle_event("resume_updates", _params, socket) do
    {:noreply, push_patch(socket, to: update_url_with_paused(socket, false), replace: true)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div id="tracer-live">
      <.svelte
        name="components/tracer/Index"
        props={
          %{
            trace_state: encode_trace_state(assigns),
            account_id: assigns.account_id,
            consumers: encode_consumers(assigns.consumers),
            databases: encode_databases(assigns.databases),
            tables: encode_tables(assigns.tables),
            paused: assigns.paused
          }
        }
      />
    </div>
    """
  end

  defp schedule_update do
    Process.send_after(self(), :update, 200)
  end

  defp get_trace_state(account_id) do
    case Server.get_state(account_id) do
      %State{} = state -> state
      {:error, _reason} -> nil
    end
  end

  defp encode_trace_state(%{trace_state: nil}), do: %{}

  defp encode_trace_state(assigns) do
    message_traces =
      assigns.trace_state.message_traces
      |> Enum.flat_map(&encode_message_trace(assigns.consumers, &1))
      |> Enum.filter(&filter_trace?(&1, assigns.params))

    total_count = length(message_traces)
    message_traces = Enum.slice(message_traces, (assigns.page - 1) * assigns.per_page, assigns.per_page)

    %{
      message_traces: message_traces,
      started_at: assigns.trace_state.started_at,
      total_count: total_count
    }
  end

  defp encode_message_trace(consumers, message_trace) do
    table = find_table(message_trace.database, message_trace.message.table_oid)
    primary_keys = get_primary_keys(table)

    Enum.map(message_trace.consumer_traces, fn consumer_trace ->
      consumer = Enum.find(consumers, &(&1.id == consumer_trace.consumer_id))

      %{
        date: message_trace.message.commit_timestamp,
        consumer_id: consumer_trace.consumer_id,
        consumer: %{
          id: consumer.id,
          name: consumer.name
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

  defp encode_consumers(consumers) do
    Enum.map(consumers, &%{id: &1.id, name: &1.name})
  end

  defp encode_databases(databases) do
    Enum.map(databases, &%{id: &1.id, name: &1.name})
  end

  defp encode_tables(tables) do
    Enum.map(tables, &%{oid: &1.oid, name: &1.name})
  end

  defp find_table(database, table_oid) do
    Enum.find(database.tables, &(&1.oid == table_oid))
  end

  defp encode_table(nil), do: nil
  defp encode_table(%{name: name}), do: name

  defp get_primary_keys(nil), do: []

  defp get_primary_keys(table) do
    Enum.filter(table.columns, & &1.is_pk?)
  end

  defp encode_primary_keys(ids, primary_keys) do
    ids |> Enum.zip(primary_keys) |> Enum.map_join(", ", fn {id, pk} -> "#{pk.name}: #{id}" end)
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

  defp update_url_with_paused(socket, paused) do
    current_path = URI.parse(socket.assigns.live_action)
    query_params = URI.decode_query(current_path.query || "")

    updated_params =
      if paused do
        Map.put(query_params, "paused", "true")
      else
        Map.delete(query_params, "paused")
      end

    query_string = URI.encode_query(updated_params)
    "#{current_path.path}?#{query_string}"
  end

  defp filter_trace?(trace, params) do
    database_match?(trace, params["database"]) and
      consumer_match?(trace, params["consumer"]) and
      table_match?(trace, params["table"]) and
      state_match?(trace, params["state"])
  end

  defp database_match?(_trace, nil), do: true
  defp database_match?(trace, database), do: trace.database == database

  defp consumer_match?(_trace, nil), do: true
  defp consumer_match?(trace, consumer_id), do: trace.consumer_id == consumer_id

  defp table_match?(_trace, nil), do: true
  defp table_match?(trace, table), do: trace.table == table

  defp state_match?(_trace, nil), do: true
  defp state_match?(trace, state), do: String.downcase(trace.state) == String.downcase(state)
end
