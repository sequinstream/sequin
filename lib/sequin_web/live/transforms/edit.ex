defmodule SequinWeb.TransformsLive.Edit do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers
  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Runtime
  alias Sequin.Transforms.Message
  alias Sequin.Transforms.TestMessages

  require Logger

  def mount(params, _session, socket) do
    id = params["id"]

    if connected?(socket) do
      schedule_poll_test_messages()
      TestMessages.register_needs_messages(current_account_id(socket))
    end

    changeset =
      case id do
        nil ->
          Transform.changeset(%Transform{}, %{"transform" => %{"type" => "path", "path" => ""}})

        id ->
          transform = Consumers.get_transform!(current_account_id(socket), id)
          Transform.changeset(transform, %{})
      end

    used_by_consumers =
      if id do
        Consumers.list_consumers_for_transform(current_account_id(socket), id, [:replication_slot])
      else
        []
      end

    test_messages = TestMessages.get_test_messages(current_account_id(socket))

    {:ok,
     assign(socket,
       id: id,
       changeset: changeset,
       form_data: changeset_to_form_data(changeset),
       used_by_consumers: used_by_consumers,
       form_errors: %{},
       test_messages: test_messages,
       validating: false,
       show_errors?: false
     )}
  end

  def render(assigns) do
    ~H"""
    <div id="transform_new">
      <.svelte
        name="transforms/Edit"
        props={
          %{
            formData: @form_data,
            formErrors: if(@show_errors?, do: @form_errors, else: %{}),
            testMessages: encode_test_messages(@test_messages, @form_data),
            usedByConsumers: Enum.map(@used_by_consumers, &encode_consumer/1),
            validating: @validating,
            parent: "transform_new"
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  def handle_info(:poll_test_messages, socket) do
    test_messages = TestMessages.get_test_messages(current_account_id(socket))
    schedule_poll_test_messages()
    {:noreply, assign(socket, test_messages: test_messages)}
  end

  def handle_event("validate", %{"transform" => params}, socket) do
    changeset =
      %Transform{}
      |> Transform.changeset(params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset)
    form_errors = Sequin.Error.errors_on(changeset)

    {:noreply,
     socket
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)}
  end

  def handle_event("save", %{"transform" => params}, socket) do
    params = decode_params(params)

    case upsert_transform(socket, params) do
      {:ok, :created} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform created successfully"})
         |> push_navigate(to: ~p"/transforms")}

      {:ok, :updated} ->
        socket.assigns.used_by_consumers
        |> Enum.map(& &1.replication_slot)
        |> Enum.uniq_by(& &1.id)
        |> Enum.each(&Runtime.Supervisor.restart_replication/1)

        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform updated successfully"})
         |> push_navigate(to: ~p"/transforms")}

      {:error, %Ecto.Changeset{} = changeset} ->
        form_data = changeset_to_form_data(changeset)
        form_errors = Sequin.Error.errors_on(changeset)

        {:noreply,
         socket
         |> assign(:changeset, changeset)
         |> assign(:form_data, form_data)
         |> assign(:form_errors, form_errors)
         |> assign(:show_errors?, true)}
    end
  end

  def handle_event("delete", _params, socket) do
    case Consumers.delete_transform(current_account_id(socket), socket.assigns.id) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform deleted successfully"})
         |> push_navigate(to: ~p"/transforms")}

      {:error, error} ->
        Logger.error("Failed to delete transform", error: error)
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete transform"})}
    end
  end

  defp upsert_transform(%{assigns: %{id: nil}} = socket, params) do
    with {:ok, _transform} <- Consumers.create_transform(current_account_id(socket), params) do
      {:ok, :created}
    end
  end

  defp upsert_transform(%{assigns: %{id: id}} = socket, params) do
    with {:ok, _} <- Consumers.update_transform(current_account_id(socket), id, params) do
      {:ok, :updated}
    end
  end

  defp changeset_to_form_data(changeset) do
    transform = Ecto.Changeset.get_field(changeset, :transform)

    path =
      case transform do
        nil -> ""
        %PathTransform{path: path} -> path
        %Ecto.Changeset{} = changeset -> Ecto.Changeset.get_field(changeset, :path)
      end

    %{
      id: Ecto.Changeset.get_field(changeset, :id),
      name: Ecto.Changeset.get_field(changeset, :name),
      transform: %{
        path: path
      }
    }
  end

  defp encode_test_messages(test_messages, form_data) do
    original_consumer = %SinkConsumer{transform: nil, legacy_transform: :none}
    path = get_in(form_data, [:transform, :path]) || ""

    consumer = %SinkConsumer{
      transform: %Transform{transform: %PathTransform{path: path}},
      legacy_transform: :none
    }

    Enum.map(test_messages, fn message ->
      %{original: Message.to_external(original_consumer, message), transformed: Message.to_external(consumer, message)}
    end)
  end

  defp encode_consumer(consumer) do
    %{
      name: consumer.name
    }
  end

  defp decode_params(params) do
    %{
      "name" => params["name"],
      "transform" => %{
        "type" => "path",
        "path" => params["transform"]["path"]
      }
    }
  end

  defp schedule_poll_test_messages do
    Process.send_after(self(), :poll_test_messages, 1000)
  end
end
