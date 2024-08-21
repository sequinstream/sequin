defmodule SequinWeb.ConsumersLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  # alias Sequin.Consumers
  # alias Sequin.Error

  require Logger

  @steps [:select_stream, :select_consumer, :select_table, :configure_filters, :configure_consumer, :confirmation]

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:changeset, %{})
      |> assign(:form_errors, %{})
      |> assign(:step, :select_consumer)
      |> assign(:form, %{
        message_kind: nil,
        consumer_kind: nil,
        source_table: %{
          name: nil,
          filters: [
            %{
              column: nil,
              operator: nil,
              value: nil
            }
          ]
        }
      })

    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
  end

  @impl Phoenix.LiveView
  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns =
      assigns
      |> assign(:parent_id, "new-consumer")
      |> assign(:encoded_form, encode_form(assigns.form))

    ~H"""
    <div id={@parent_id} class="w-full">
      <.svelte
        name="consumers/New"
        props={
          %{
            changeset: @changeset,
            formErrors: @form_errors,
            currentStep: @step,
            form: @encoded_form,
            parent: @parent_id
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("validate", _params, socket) do
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("consumer_updated", %{"consumer" => consumer, "step_forward" => true}, socket) do
    form = decode_form(consumer)

    socket =
      socket
      |> assign(:form, form)
      |> step_forward()

    {:noreply, socket}
  end

  def handle_event("consumer_updated", %{"consumer" => consumer}, socket) do
    form = decode_form(consumer)

    socket =
      assign(socket, :form, form)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("step_forward", _params, socket) do
    {:noreply, step_forward(socket)}
  end

  def handle_event("step_back", _params, socket) do
    {:noreply, step_back(socket)}
  end

  defp step_forward(socket) do
    step = socket.assigns.step
    next_step = Enum.at(@steps, current_step_index(socket) + 1)

    if next_step do
      assign(socket, :step, next_step)
    else
      Logger.error("Cannot step_forward from #{step}")
      socket
    end
  end

  defp step_back(socket) do
    step = socket.assigns.step
    prev_step = Enum.at(@steps, current_step_index(socket) - 1)

    if prev_step do
      assign(socket, :step, prev_step)
    else
      Logger.error("Cannot step_back from #{step}")
      socket
    end
  end

  defp current_step_index(socket) do
    Enum.find_index(@steps, &(&1 == socket.assigns.step))
  end

  defp decode_form(encoded_form) do
    %{
      message_kind: encoded_form["messageKind"],
      consumer_kind: encoded_form["consumerKind"],
      source_table: %{
        name: encoded_form["sourceTable"]["name"],
        filters:
          Enum.map(encoded_form["sourceTable"]["filters"] || [], fn filter ->
            %{
              column: filter["column"],
              operator: filter["operator"],
              value: filter["value"]
            }
          end)
      }
    }
  end

  defp encode_form(form) do
    %{
      "messageKind" => form.message_kind,
      "consumerKind" => form.consumer_kind,
      "sourceTable" => %{
        "name" => form.source_table.name,
        "filters" =>
          Enum.map(form.source_table.filters, fn filter ->
            %{
              "column" => filter.column,
              "operator" => filter.operator,
              "value" => filter.value
            }
          end)
      }
    }
  end

  # defp create_consumer(%{"consumer_kind" => "http_pull"} = params) do
  #   Consumers.create_http_pull_consumer_with_lifecycle(params)
  # end

  # defp create_consumer(%{"consumer_kind" => "http_push"} = params) do
  #   Consumers.create_http_push_consumer_with_lifecycle(params)
  # end
end
