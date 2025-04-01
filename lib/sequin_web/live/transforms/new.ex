defmodule SequinWeb.TransformsLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers
  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Transforms.Message
  alias Sequin.Transforms.TestMessages

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(1000, self(), :poll_test_messages)
      dbg(current_account_id(socket))
      TestMessages.register_needs_messages(current_account_id(socket))
    end

    changeset = PathTransform.changeset(%PathTransform{}, %{"path" => ""})

    test_messages = TestMessages.get_test_messages(current_account_id(socket))

    {:ok,
     assign(socket,
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
    test_messages = TestMessages.get_test_messages(current_account_id(socket))
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
    params = decode_params(params)

    case Consumers.create_transform(current_account_id(socket), params) do
      {:ok, _transform} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform created successfully"})
         |> push_navigate(to: ~p"/")}

      {:error, %Ecto.Changeset{} = changeset} ->
        form_data = changeset_to_form_data(changeset)
        form_errors = errors_on(changeset)
        dbg(form_errors)

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

  defp decode_params(params) do
    %{
      "name" => params["name"],
      "transform" => %{
        "type" => "path",
        "path" => params["path"]
      }
    }
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
