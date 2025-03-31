defmodule SequinWeb.TransformsLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Databases.Sequence
  alias Sequin.Transforms.Message
  alias Sequin.Transforms.TestMessages

  def mount(%{"sequin_id" => sequence_id}, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(1000, self(), :poll_test_messages)
    end

    sequence = sequence_id |> Sequence.where_id() |> Sequin.Repo.one()
    changeset = PathTransform.changeset(%PathTransform{}, %{"path" => ""})

    test_messages =
      TestMessages.get_test_messages(sequence_id)

    {:ok,
     assign(socket,
       sequence: sequence,
       changeset: changeset,
       form_data: changeset_to_form_data(changeset),
       form_errors: %{},
       test_messages: test_messages,
       validating: false,
       show_errors?: false
     ), layout: {SequinWeb.Layouts, :app_no_sidenav}}
  end

  def render(assigns) do
    ~H"""
    <div id="transform_new">
      <.svelte
        name="transforms/NewTransform"
        props={
          %{
            sequence: @sequence,
            formData: @form_data,
            formErrors: if(@show_errors?, do: @form_errors, else: %{}),
            testMessages: encode_test_messages(@test_messages, @form_data[:path]),
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
    test_messages = TestMessages.get_test_messages(socket.assigns.sequence.id)
    {:noreply, assign(socket, test_messages: test_messages)}
  end

  def handle_event("validate", %{"path_transform" => params}, socket) do
    changeset =
      %PathTransform{}
      |> PathTransform.changeset(params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset)
    form_errors = errors_on(changeset)

    {:noreply,
     socket
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)}
  end

  def handle_event("save", %{"path_transform" => params}, socket) do
    case PathTransform.changeset(%PathTransform{}, params) do
      %Ecto.Changeset{valid?: true} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform created successfully"})
         |> push_navigate(to: ~p"/")}

      %Ecto.Changeset{} = changeset ->
        form_data = changeset_to_form_data(changeset)
        form_errors = errors_on(changeset)

        {:noreply,
         socket
         |> assign(:changeset, changeset)
         |> assign(:form_data, form_data)
         |> assign(:form_errors, form_errors)
         |> assign(:show_errors?, true)}
    end
  end

  defp changeset_to_form_data(changeset) do
    %{
      path: Ecto.Changeset.get_field(changeset, :path)
    }
  end

  defp encode_test_messages(test_messages, nil) do
    consumer = %SinkConsumer{transform: nil, legacy_transform: :none}

    Enum.map(test_messages, fn message ->
      %{original: Message.to_external(consumer, message), transformed: Message.to_external(consumer, message)}
    end)
  end

  defp encode_test_messages(test_messages, path) do
    original_consumer = %SinkConsumer{transform: nil, legacy_transform: :none}

    consumer = %SinkConsumer{
      transform: %Transform{transform: %PathTransform{path: path}},
      legacy_transform: :none
    }

    Enum.map(test_messages, fn message ->
      %{original: Message.to_external(original_consumer, message), transformed: Message.to_external(consumer, message)}
    end)
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
