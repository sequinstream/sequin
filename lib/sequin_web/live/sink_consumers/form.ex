defmodule SequinWeb.SinkConsumersLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Databases
  alias Sequin.Databases.DatabaseUpdateWorker
  alias SequinWeb.Components.ConsumerForm

  @impl true
  def render(assigns) do
    ~H"""
    <div id="consumers-form">
      <%= render_consumer_form(assigns) %>
    </div>
    """
  end

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account = current_account(socket)
    has_databases? = account.id |> Databases.list_dbs_for_account() |> Enum.any?()

    socket =
      socket
      |> assign(:has_databases?, has_databases?)
      |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))

    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
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

  defp render_consumer_form(%{form_kind: "sns"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :sns, sink: %SnsSink{}, batch_size: 10}}
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
      consumer={%SinkConsumer{type: :kafka, batch_size: 10, sink: %KafkaSink{tls: false}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "redis_stream"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :redis_stream, batch_size: 10, sink: %RedisStreamSink{}}}
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

  defp render_consumer_form(%{form_kind: "nats"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :nats, sink: %NatsSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "rabbitmq"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :rabbitmq, sink: %RabbitMqSink{virtual_host: "/"}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "azure_event_hub"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :azure_event_hub, sink: %AzureEventHubSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "typesense"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :typesense, sink: %TypesenseSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "elasticsearch"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :elasticsearch, sink: %ElasticsearchSink{}}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "redis_string"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="new-consumer"
      action={:new}
      consumer={%SinkConsumer{type: :redis_string, batch_size: 10, sink: %RedisStringSink{}}}
    />
    """
  end

  @impl Phoenix.LiveView
  def handle_info({:database_tables_updated, _updated_database}, socket) do
    # Proxy down to ConsumerForm
    send_update(ConsumerForm, id: "new-consumer", event: :database_tables_updated)

    {:noreply, socket}
  end
end
