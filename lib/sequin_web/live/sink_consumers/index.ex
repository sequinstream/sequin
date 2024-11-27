defmodule SequinWeb.SinkConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Databases
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Health
  alias SequinWeb.Components.ConsumerForm
  alias SequinWeb.RouteHelpers

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    user = current_user(socket)
    account = current_account(socket)
    consumers = Consumers.list_consumers_for_account(account.id, :postgres_database)
    has_databases? = account.id |> Databases.list_dbs_for_account() |> Enum.any?()
    has_sequences? = account.id |> Databases.list_sequences_for_account() |> Enum.any?()
    consumers = load_consumer_health(consumers)

    socket =
      if connected?(socket) do
        Process.send_after(self(), :update_health, 1000)

        push_event(socket, "ph-identify", %{
          userId: user.id,
          userEmail: user.email,
          userName: user.name,
          accountId: account.id,
          accountName: account.name,
          createdAt: user.inserted_at
        })
      else
        socket
      end

    socket =
      socket
      |> assign(:consumers, consumers)
      |> assign(:form_errors, %{})
      |> assign(:has_databases?, has_databases?)
      |> assign(:has_sequences?, has_sequences?)

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  @impl Phoenix.LiveView
  def render(%{live_action: :new} = assigns) do
    ~H"""
    <div id="consumers-index">
      <%= render_consumer_form(assigns) %>
    </div>
    """
  end

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
            formErrors: @form_errors,
            hasDatabases: @has_databases?,
            hasSequences: @has_sequences?
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

  defp apply_action(socket, :list, _params) do
    socket
    |> assign(:page_title, "Sinks")
    |> assign(:live_action, :list)
  end

  defp apply_action(socket, :new, %{"kind" => kind}) do
    # Refresh tables for all databases in the account
    account = current_account(socket)
    databases = Databases.list_dbs_for_account(account.id)
    Enum.each(databases, &DatabaseUpdateWorker.enqueue(&1.id))

    socket
    |> assign(:page_title, "New Sink")
    |> assign(:live_action, :new)
    |> assign(:form_kind, kind)
  end

  defp render_consumer_form(%{form_kind: "http_push"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :http_push, sink: %HttpPushSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "sqs"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :sqs, sink: %SqsSink{}, batch_size: 10}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "kafka"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :kafka, sink: %KafkaSink{tls: false}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "redis"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :redis, batch_size: 100, sink: %RedisSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "sequin_stream"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :sequin_stream, sink: %SequinStreamSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "gcp_pubsub"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={
        %SinkConsumer{
          type: :gcp_pubsub,
          sink: %GcpPubsubSink{}
        }
      }
    />
    """
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    {:noreply, assign(socket, :consumers, load_consumer_health(socket.assigns.consumers))}
  end

  def handle_info({:database_tables_updated, _updated_database}, socket) do
    # Proxy down to ConsumerForm
    send_update(ConsumerForm, id: "new-consumer", event: :database_tables_updated)

    {:noreply, socket}
  end

  defp load_consumer_health(consumers) do
    Enum.map(consumers, fn consumer ->
      case Health.get(consumer) do
        {:ok, health} -> %{consumer | health: health}
        {:error, _} -> consumer
      end
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
      href: RouteHelpers.consumer_path(consumer)
    }
  end
end
