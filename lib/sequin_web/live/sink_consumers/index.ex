defmodule SequinWeb.SinkConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Metrics
  alias SequinWeb.RouteHelpers

  @smoothing_window 5
  @timeseries_window_count 60

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    user = current_user(socket)
    account = current_account(socket)

    consumers = load_consumers(socket)
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
      |> assign(:consumers, consumers)
      |> assign(:has_databases?, has_databases?)
      |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    consumers = Enum.filter(assigns.consumers, &is_struct(&1, SinkConsumer))
    encoded_consumers = Enum.map(consumers, &encode_consumer/1)

    assigns = assign(assigns, :encoded_consumers, encoded_consumers)

    ~H"""
    <div id="consumers-index">
      <.svelte
        name="consumers/SinkIndex"
        props={
          %{
            consumers: @encoded_consumers,
            hasDatabases: @has_databases?,
            selfHosted: @self_hosted
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("consumer_clicked", %{"id" => id, "type" => type}, socket) do
    {:noreply, push_navigate(socket, to: ~p"/sinks/#{type}/#{id}")}
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Sinks")
    |> assign(:live_action, :index)
  end

  @impl Phoenix.LiveView
  def handle_info(:update_consumers, socket) do
    Process.send_after(self(), :update_consumers, 1000)
    {:noreply, assign(socket, :consumers, load_consumers(socket))}
  end

  defp load_consumers(socket) do
    socket
    |> current_account_id()
    |> Consumers.list_consumers_for_account([:postgres_database, :replication_slot, :active_backfill])
    |> load_consumer_health()
    |> load_consumer_metrics()
  end

  defp load_consumer_health(consumers) do
    Enum.map(consumers, fn consumer ->
      with {:ok, health} <- Health.health(consumer),
           {:ok, slot_health} <- Health.health(consumer.replication_slot) do
        health = Health.add_slot_health_to_consumer_health(health, slot_health)
        %{consumer | health: health}
      else
        {:error, _} -> consumer
      end
    end)
  end

  defp load_consumer_metrics(consumers) do
    Enum.map(consumers, fn consumer ->
      {:ok, messages_processed_throughput_timeseries} =
        Metrics.get_consumer_messages_processed_throughput_timeseries_smoothed(
          consumer,
          @timeseries_window_count,
          @smoothing_window
        )

      Map.put(consumer, :metrics, %{
        messages_processed_throughput_timeseries: messages_processed_throughput_timeseries
      })
    end)
  end

  defp encode_consumer(consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      insertedAt: consumer.inserted_at,
      type: consumer.type,
      status: consumer.status,
      database_name: consumer.postgres_database.name,
      health: Health.to_external(consumer.health),
      href: RouteHelpers.consumer_path(consumer),
      active_backfill: not is_nil(consumer.active_backfill),
      metrics: %{
        messages_processed_throughput_timeseries: consumer.metrics.messages_processed_throughput_timeseries
      }
    }
  end
end
