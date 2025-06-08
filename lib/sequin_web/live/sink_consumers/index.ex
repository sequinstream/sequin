defmodule SequinWeb.SinkConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Metrics
  alias SequinWeb.RouteHelpers

  @smoothing_window 5
  @timeseries_window_count 60
  @page_size 50

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    user = current_user(socket)
    account = current_account(socket)

    page = (params["page"] && String.to_integer(params["page"])) || 0
    page_size = @page_size
    total_count = Consumers.count_sink_consumers_for_account(account.id)

    socket =
      socket
      |> assign(:page, page)
      |> assign(:page_size, page_size)
      |> assign(:total_count, total_count)
      |> assign(:encoded_consumers, nil)
      |> assign(:database_names, %{})
      |> assign(:consumer_health, %{})
      |> assign(:consumer_metrics, %{})
      |> async_assign_consumers()

    has_databases? = account.id |> Databases.list_dbs_for_account() |> Enum.any?()

    socket =
      if connected?(socket) do
        Process.send_after(self(), :update_consumers, 1000)

        push_event(socket, "ph-identify", %{
          userId: user.id,
          userEmail: user.email,
          userName: user.name,
          accountId: account.id,
          accountName: account.name,
          createdAt: user.inserted_at,
          contactEmail: account.contact_email,
          sequinVersion: Application.get_env(:sequin, :release_version)
        })
      else
        socket
      end

    socket =
      socket
      |> assign(:has_databases?, has_databases?)
      |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    page = (params["page"] && String.to_integer(params["page"])) || 0
    {:noreply, socket |> apply_action(socket.assigns.live_action, params) |> assign(:page, page)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="consumers-index">
      <.svelte
        name="consumers/SinkIndex"
        props={
          %{
            consumers: @encoded_consumers,
            hasDatabases: @has_databases?,
            selfHosted: @self_hosted,
            page: @page,
            pageSize: @page_size,
            totalCount: @total_count
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("change_page", %{"page" => page}, socket) do
    account_id = current_account_id(socket)
    total_count = Consumers.count_sink_consumers_for_account(account_id)

    socket =
      socket
      |> assign(:page, page)
      |> assign(:total_count, total_count)
      |> assign(:encoded_consumers, nil)
      |> async_assign_consumers()
      |> push_navigate(to: ~p"/sinks?page=#{page}", replace: true)

    {:noreply, socket}
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Sinks | Sequin")
    |> assign(:live_action, :index)
  end

  @impl Phoenix.LiveView
  def handle_info(:update_consumers, socket) do
    {:noreply, async_assign_consumers(socket)}
  end

  @impl Phoenix.LiveView
  def handle_async(:consumers_task, {:ok, {consumers, database_names, consumer_health, consumer_metrics}}, socket) do
    encoded_consumers = Enum.map(consumers, &encode_consumer(&1, database_names, consumer_health, consumer_metrics))
    Process.send_after(self(), :update_consumers, 1000)

    socket =
      socket
      |> assign(:encoded_consumers, encoded_consumers)
      |> assign(:database_names, database_names)
      |> assign(:consumer_health, consumer_health)
      |> assign(:consumer_metrics, consumer_metrics)

    {:noreply, socket}
  end

  defp async_assign_consumers(socket) do
    page = socket.assigns.page
    page_size = socket.assigns.page_size
    account_id = current_account_id(socket)

    start_async(socket, :consumers_task, fn -> load_consumers(account_id, page, page_size) end)
  end

  defp load_consumers(account_id, page, page_size) do
    consumers =
      Consumers.list_sink_consumers_for_account_paginated(account_id, page, page_size,
        preload: [:replication_slot, :active_backfills]
      )

    {consumers, load_database_names(consumers), load_consumer_health(consumers), load_consumer_metrics(consumers)}
  end

  defp load_database_names(consumers) do
    Databases.db_names_for_consumer_ids(Enum.map(consumers, & &1.id))
  end

  defp load_consumer_health(consumers) do
    Map.new(consumers, fn consumer ->
      with {:ok, health} <- Health.health(consumer),
           {:ok, slot_health} <- Health.health(consumer.replication_slot) do
        health = Health.add_slot_health_to_consumer_health(health, slot_health)
        {consumer.id, health}
      else
        {:error, _} -> {consumer.id, nil}
      end
    end)
  end

  defp load_consumer_metrics(consumers) do
    Map.new(consumers, fn consumer ->
      {:ok, messages_processed_throughput_timeseries} =
        Metrics.get_consumer_messages_processed_throughput_timeseries_smoothed(
          consumer,
          @timeseries_window_count,
          @smoothing_window
        )

      {consumer.id,
       %{
         messages_processed_throughput_timeseries: messages_processed_throughput_timeseries
       }}
    end)
  end

  defp encode_consumer(consumer, database_names, consumer_health, consumer_metrics) do
    %{
      id: consumer.id,
      name: consumer.name,
      insertedAt: consumer.inserted_at,
      type: consumer.type,
      status: consumer.status,
      database_name: database_names[consumer.id],
      health: Health.to_external(consumer_health[consumer.id]),
      href: RouteHelpers.consumer_path(consumer),
      active_backfill: consumer.active_backfills != [],
      metrics: consumer_metrics[consumer.id]
    }
  end
end
