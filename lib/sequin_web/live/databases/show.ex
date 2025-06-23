defmodule SequinWeb.DatabasesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Health
  alias Sequin.Health.CheckPostgresReplicationSlotWorker
  alias Sequin.Metrics
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Tracer.Server
  alias Sequin.Tracer.State, as: TracerState
  alias SequinWeb.RouteHelpers

  # Add this alias

  @impl Phoenix.LiveView
  def mount(%{"id" => id} = params, _session, socket) do
    account_id = current_account_id(socket)

    case Databases.get_db_for_account(account_id, id) do
      {:ok, database} ->
        database = preload_database(database)

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
          # Tracer.DynamicSupervisor.start_for_account(account_id)

          Process.send_after(self(), :update, 1000)
        end

        :syn.join(:replication, {:postgres_replication_slot_checked, database.id}, self())

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
    %{database: database} = socket.assigns
    assign(socket, :page_title, "#{database.name} | Sequin")
  end

  defp apply_action(socket, :edit, _params) do
    %{database: database} = socket.assigns
    assign(socket, :page_title, "#{database.name} | Edit | Sequin")
  end

  defp apply_action(socket, :messages, _params) do
    %{database: database} = socket.assigns
    assign(socket, :page_title, "#{database.name} | Messages | Sequin")
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_postgres_info", _, socket) do
    %{database: database} = socket.assigns
    database = preload_database(database)
    {:ok, database} = Databases.update_pg_major_version(database)

    case Databases.update_tables(database) do
      {:ok, database} ->
        {:reply, %{}, assign(socket, database: database)}

      {:error, _reason} ->
        {:reply, %{}, socket}
    end
  end

  def handle_event("delete_database", _, socket) do
    %{database: database} = socket.assigns

    case Databases.delete_db_with_replication_slot(database) do
      :ok ->
        {:noreply, push_navigate(socket, to: ~p"/databases")}

      {:error, error} ->
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("edit", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/databases/#{socket.assigns.database.id}/edit")}
  end

  def handle_event("enable", _params, socket) do
    database = socket.assigns.database

    case Replication.update_pg_replication(database.replication_slot, %{status: :active}) do
      {:ok, updated_slot} ->
        updated_db = %{database | replication_slot: updated_slot}
        CheckPostgresReplicationSlotWorker.enqueue_for_user(database.id)

        socket =
          socket
          |> assign(:database, updated_db)
          |> assign_health()

        {:reply, %{ok: true}, socket}

      {:error, _changeset} ->
        {:reply, %{ok: false}, put_flash(socket, :error, "Failed to enable database. Please try again.")}
    end
  end

  def handle_event("disable", _params, socket) do
    database = socket.assigns.database

    case Replication.update_pg_replication(database.replication_slot, %{status: :disabled}) do
      {:ok, updated_slot} ->
        updated_db = %{database | replication_slot: updated_slot}

        socket =
          socket
          |> assign(:database, updated_db)
          |> assign_health()

        {:reply, %{ok: true}, socket}

      {:error, _changeset} ->
        {:reply, %{ok: false}, put_flash(socket, :error, "Failed to disable database. Please try again.")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_health", _params, socket) do
    # Will receive a :postgres_replication_slot_checked message when the worker finishes
    CheckPostgresReplicationSlotWorker.enqueue_for_user(socket.assigns.database.id)
    {:noreply, assign_health(socket)}
  end

  def handle_event("refresh_check", %{"slug" => "replication_configuration"}, socket) do
    CheckPostgresReplicationSlotWorker.enqueue_for_user(socket.assigns.database.id)
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info(:update, socket) do
    database = socket.assigns.database
    database = preload_database(database)

    {:noreply,
     socket
     |> assign(:database, database)
     |> assign_health()
     |> assign_metrics()
     |> assign_messages()}
  end

  def handle_info({ref, {:ok, updated_db}}, socket) do
    Process.demonitor(ref, [:flush])
    updated_db = preload_database(updated_db)
    {:noreply, assign(socket, database: updated_db, refreshing_tables: false)}
  end

  def handle_info({:updated_database, updated_database}, socket) do
    updated_database = preload_database(updated_database)

    {:noreply,
     socket
     |> assign(database: updated_database)
     |> push_patch(to: ~p"/databases/#{updated_database.id}")}
  end

  def handle_info({ref, {:error, _reason}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, refreshing_tables: false)}
  end

  def handle_info(:postgres_replication_slot_checked, socket) do
    socket = socket |> assign_health() |> assign_metrics()

    {:noreply, socket}
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
        {:ok, avg_latency} -> avg_latency && round(avg_latency)
        _ -> nil
      end

    replication_lag_bytes =
      case Metrics.get_postgres_replication_slot_lag(database.replication_slot) do
        {:ok, replication_lag_bytes} -> replication_lag_bytes
        _ -> nil
      end

    metrics = %{
      avg_latency: avg_latency,
      replication_lag_bytes: replication_lag_bytes
    }

    assign(socket, :metrics, metrics)
  end

  defp assign_health(socket) do
    case Health.health(socket.assigns.database.replication_slot) do
      {:ok, health} ->
        assign(socket, database: Map.put(socket.assigns.database, :health, health))

      {:error, _} ->
        socket
    end
  end

  def assign_messages(%{assigns: %{paused: true}} = socket), do: socket

  def assign_messages(%{assigns: %{paused: false}} = socket) do
    account_id = current_account_id(socket)

    consumers = Consumers.list_consumers_for_account(account_id)
    trace_state = get_trace_state(account_id)

    assign(socket, consumers: consumers, trace_state: trace_state)
  end

  defp handle_edit_finish(updated_database) do
    send(self(), {:updated_database, updated_database})
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      paused: database.replication_slot.status == :disabled,
      hostname: database.hostname,
      port: database.port,
      database: database.database,
      username: database.username,
      ssl: database.ssl,
      pool_size: database.pool_size,
      queue_interval: database.queue_interval,
      queue_target: database.queue_target,
      pg_major_version: database.pg_major_version,
      tables: encode_tables(database.tables),
      tables_refreshed_at: database.tables_refreshed_at,
      inserted_at: database.inserted_at,
      updated_at: database.updated_at,
      consumers: encode_consumers(database.replication_slot.sink_consumers, database),
      health: encode_health(database),
      publication_name: database.replication_slot.publication_name,
      slot_name: database.replication_slot.slot_name
    }
  end

  defp encode_consumers(consumers, database) do
    Enum.flat_map(consumers, fn consumer ->
      kind = Consumers.kind(consumer)

      try do
        [
          %{
            id: consumer.id,
            consumer_kind: kind,
            name: consumer.name,
            message_kind: consumer.message_kind,
            source_tables: Consumers.enrich_source_tables(consumer.source_tables, database),
            href: RouteHelpers.consumer_path(consumer)
          }
        ]
      rescue
        _ -> []
      end
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

  defp enrich_trace_state(account_id, %TracerState{} = state) do
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
      |> Enum.map(&encode_message_trace/1)
      |> Enum.filter(&filter_trace?(&1, assigns.params))

    total_count = length(message_traces)
    message_traces = Enum.slice(message_traces, (assigns.page - 1) * assigns.per_page, assigns.per_page)

    %{message_traces: message_traces, total_count: total_count}
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

    %{
      date: message_trace.message.commit_timestamp,
      table: encode_table(table),
      primary_keys: encode_primary_keys(message_trace.message.ids, primary_keys),
      trace_id: message_trace.message.trace_id,
      action: message_trace.message.action,
      consumer_traces: Enum.map(message_trace.consumer_traces, &encode_consumer_trace(message_trace, &1))
    }
  end

  defp encode_consumer_trace(message_trace, consumer_trace) do
    %{
      consumer_id: consumer_trace.consumer_id,
      consumer: %{
        id: consumer_trace.consumer.id,
        name: consumer_trace.consumer.name,
        type: consumer_trace.consumer.type
      },
      database: %{
        id: message_trace.database.id,
        name: message_trace.database.name
      },
      state: consumer_trace_state(consumer_trace),
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
            }
          end)
          |> Enum.reverse()
      ],
      span_types: Enum.map(consumer_trace.spans, & &1.type)
    }
  end

  defp consumer_trace_state(consumer_trace) do
    states = Enum.map(consumer_trace.spans, & &1.type)

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

  defp encode_health(%PostgresDatabase{health: %Health{} = health} = database) do
    health
    |> Health.to_external()
    |> Map.update!(:checks, fn checks ->
      Enum.map(checks, fn check ->
        maybe_augment_alert(check, database)
      end)
    end)
  end

  defp maybe_augment_alert(
         %{slug: :replication_messages, error: %{code: :replication_lag_high} = error} = check,
         _database
       ) do
    lag_bytes = error.details.lag_bytes
    lag_mb = Float.round(lag_bytes / 1024 / 1024, 0)

    Map.merge(
      check,
      %{
        alertTitle: "Notice: Replication Slot Size is High",
        alertMessage: """
        Sequin is processing messages, but the replication slot has grown to #{lag_mb}MB in size.

        This could mean:

        1. Sequin is processing a very large transaction. Large transactions cause the replication slot to grow quickly, and the slot won't shrink until Sequin has fully ingested the transaction.

        2. Sequin has fallen behind in processing messages.

        3. Sequin is applying back-pressure because the message buffer for one or more sinks is full. If this is the case, you'll see an error on the corresponding sink.

        Note: A temporarily high replication slot size is normal. However, if it grows too large, you risk running out of storage on your Postgres database.
        """,
        refreshable: false,
        dismissable: false
      }
    )
  end

  defp maybe_augment_alert(%{error_slug: :logical_messages_table_missing} = check, _database) do
    Map.merge(
      check,
      %{
        alertTitle: "Missing Required Table",
        alertMessage: """
        The `sequin_logical_messages` table is missing in your database. This table is required for Sequin with PostgreSQL versions older than 14.  <a href="https://docs.sequinstream.com/reference/databases#postgresql-12-and-13">Read more</a> about Sequin's support for PostgreSQL 12 and 13.

        Please create this table using the below SQL. Ensure also that this table is added to your publication.
        """,
        refreshable: true,
        dismissable: false,
        code: %{language: "sql", code: Postgres.logical_messages_table_ddl()}
      }
    )
  end

  defp maybe_augment_alert(%{error_slug: :logical_messages_table_in_publication} = check, database) do
    publication_name = database.replication_slot.publication_name

    Map.merge(
      check,
      %{
        alertTitle: "Missing Required Table",
        alertMessage: """
        The `sequin_logical_messages` table is missing from your publication. This table is required for Sequin with PostgreSQL versions older than 14.  <a href="https://docs.sequinstream.com/reference/databases#postgresql-12-and-13">Read more</a> about Sequin's support for PostgreSQL 12 and 13.

        Please add this table to your publication.
        """,
        refreshable: true,
        dismissable: false,
        code: %{
          language: "sql",
          code: "ALTER PUBLICATION #{publication_name} ADD TABLE public.#{Constants.logical_messages_table_name()};"
        }
      }
    )
  end

  defp maybe_augment_alert(check, _database), do: check

  defp get_trace_state(account_id) do
    case Server.get_state(account_id) do
      %TracerState{} = state -> enrich_trace_state(account_id, state)
      {:error, _reason} -> nil
    end
  catch
    :exit, _ -> nil
  end

  defp filter_trace?(trace, params) do
    table_match?(trace, params["table"])
  end

  defp table_match?(_trace, nil), do: true
  defp table_match?(trace, table), do: trace.table == table

  defp preload_database(database) do
    database =
      database
      |> Repo.reload!()
      |> Repo.preload(replication_slot: [:sink_consumers])

    {:ok, health} = Health.health(database.replication_slot)
    Map.put(database, :health, health)
  end
end
