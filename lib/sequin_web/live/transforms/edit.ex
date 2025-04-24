defmodule SequinWeb.TransformsLive.Edit do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers
  alias Sequin.Consumers.RoutingTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Repo
  alias Sequin.Runtime
  alias Sequin.Transforms.Message
  alias Sequin.Transforms.MiniElixir
  alias Sequin.Transforms.TestMessages
  alias SequinWeb.TransformLive.AutoComplete

  require Logger

  @max_test_messages TestMessages.max_message_count()
  @initial_transform """
  def transform(action, record, changes, metadata) do
    %{
      action: action,
      record: record,
      changes: changes,
      metadata: metadata
    }
  end
  """
  @initial_route_no_sink_type """
  def route(action, record, changes, metadata) do
    # TODO: Choose a sink type for your router function
  end
  """

  @initial_route_http """
  def route(action, record, changes, metadata) do
    %{
      method: "PATCH",
      endpoint_path: "/entities/\#{record["id"]}"
    }
  end
  """

  @initial_code_map %{
    "path" => "",
    "function" => @initial_transform,
    "routing_undefined" => @initial_route_no_sink_type,
    "routing_http_push" => @initial_route_http
  }

  # We generate the function completions at compile time because
  # docs are not available at runtime in our release.
  @function_completions AutoComplete.function_completions()

  def mount(params, _session, socket) do
    id = params["id"]

    if connected?(socket) do
      schedule_poll_test_messages()
    end

    changeset =
      case id do
        nil ->
          Transform.changeset(%Transform{}, %{"transform" => %{}})

        id ->
          transform = Consumers.get_transform_for_account!(current_account_id(socket), id)
          Transform.changeset(transform, %{})
      end

    used_by_consumers =
      if id do
        Consumers.list_consumers_for_transform(current_account_id(socket), id, [:replication_slot])
      else
        []
      end

    socket =
      socket
      |> assign(
        id: id,
        changeset: changeset,
        form_data: changeset_to_form_data(changeset),
        used_by_consumers: used_by_consumers,
        form_errors: %{},
        test_messages: [],
        validating: false,
        show_errors?: false,
        selected_database_id: nil,
        selected_table_oid: nil,
        synthetic_test_message: Consumers.synthetic_message(),
        initial_code: @initial_code_map,
        function_completions: @function_completions
      )
      |> assign_databases()

    {:ok, socket}
  end

  def render(assigns) do
    ~H"""
    <div id="transform_new">
      <.svelte
        name="transforms/Edit"
        props={
          %{
            formData: @form_data,
            showErrors: @show_errors?,
            formErrors: @form_errors,
            testMessages: encode_test_messages(@test_messages, @form_data, @form_errors),
            syntheticTestMessages:
              encode_synthetic_test_message(@synthetic_test_message, @form_data, @form_errors),
            usedByConsumers: Enum.map(@used_by_consumers, &encode_consumer/1),
            databases: Enum.map(@databases, &encode_database/1),
            validating: @validating,
            parent: "transform_new",
            initialCodeMap: @initial_code,
            initialCode: "glugma",
            functionTransformsEnabled: Sequin.feature_enabled?(:function_transforms),
            functionCompletions: @function_completions
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  def handle_info(:poll_test_messages, socket) do
    database_id = socket.assigns.selected_database_id
    table_oid = socket.assigns.selected_table_oid

    schedule_poll_test_messages()

    if database_id && table_oid do
      test_messages = TestMessages.get_test_messages(database_id, table_oid)

      if length(test_messages) >= @max_test_messages do
        TestMessages.unregister_needs_messages(database_id)
      end

      {:noreply, assign(socket, test_messages: test_messages)}
    else
      {:noreply, assign(socket, test_messages: [])}
    end
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
         |> push_navigate(to: ~p"/functions")}

      {:ok, :updated} ->
        socket.assigns.used_by_consumers
        |> Enum.map(& &1.replication_slot)
        |> Enum.uniq_by(& &1.id)
        |> Enum.each(&Runtime.Supervisor.restart_replication/1)

        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Transform updated successfully"})
         |> push_navigate(to: ~p"/functions")}

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
         |> push_navigate(to: ~p"/functions")}

      {:error, error} ->
        Logger.error("[Transform.Edit] Failed to delete transform", error: error)
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete transform"})}
    end
  end

  def handle_event("table_selected", %{"database_id" => database_id, "table_oid" => table_oid}, socket) do
    socket = assign(socket, selected_database_id: database_id, selected_table_oid: table_oid)

    case TestMessages.get_test_messages(database_id, table_oid) do
      messages when length(messages) < @max_test_messages ->
        TestMessages.register_needs_messages(database_id)
        {:noreply, assign(socket, test_messages: messages)}

      messages ->
        {:noreply, assign(socket, test_messages: messages)}
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

    transform_data =
      case transform do
        nil ->
          %{}

        %Ecto.Changeset{} ->
          Ecto.Changeset.apply_changes(transform)

        %_{} = struct ->
          struct
      end

    %{
      id: Ecto.Changeset.get_field(changeset, :id),
      name: Ecto.Changeset.get_field(changeset, :name),
      description: Ecto.Changeset.get_field(changeset, :description),
      transform: transform_data
    }
  end

  defp encode_synthetic_test_message(synthetic_message, form_data, form_errors) do
    [synthetic_message]
    |> encode_test_messages(form_data, form_errors)
    |> Enum.map(&Map.put(&1, :isSynthetic, true))
  end

  defp encode_test_messages(test_messages, form_data, form_errors) do
    if is_nil(get_in(form_errors, [:transform, :code])) do
      do_encode_test_messages(test_messages, form_data)
    else
      Enum.map(test_messages, &prepare_test_message/1)
    end
  end

  defp do_encode_test_messages(test_messages, form_data) do
    transform = form_data[:transform]

    consumer = %SinkConsumer{
      transform: %Transform{transform: transform},
      legacy_transform: :none
    }

    consumer =
      case transform do
        %RoutingTransform{} -> %{consumer | type: transform.sink_type}
        _ -> consumer
      end

    base_messages = Enum.map(test_messages, &prepare_test_message/1)

    if is_struct(transform) do
      results = Enum.map(test_messages, &encode_one(&1, consumer))
      Enum.zip_with(base_messages, results, &Map.merge/2)
    else
      base_messages
    end
  end

  defp encode_one(message, consumer) do
    case :timer.tc(fn -> Message.to_external(consumer, message) end, :microsecond) do
      {time, value} ->
        case Jason.encode(value) do
          {:ok, _} -> %{transformed: value, time: time}
          {:error, error} -> %{error: MiniElixir.encode_error(error), time: time}
        end
    end
  rescue
    ex ->
      %{error: MiniElixir.encode_error(ex), time: nil}
  end

  defp prepare_test_message(m) do
    %{
      record: inspect(m.data.record, pretty: true),
      changes: inspect(m.data.changes, pretty: true),
      action: inspect(to_string(m.data.action), pretty: true),
      metadata: inspect(Sequin.Map.from_struct_deep(m.data.metadata), pretty: true)
    }
  end

  defp assign_databases(socket) do
    account_id = current_account_id(socket)

    databases =
      account_id
      |> Databases.list_dbs_for_account()
      |> Repo.preload(:sequences)

    assign(socket, :databases, databases)
  end

  defp encode_consumer(consumer) do
    %{
      name: consumer.name
    }
  end

  defp encode_database(database) do
    %{
      "id" => database.id,
      "name" => database.name,
      "tables" =>
        database.tables
        |> Enum.map(&encode_table/1)
        |> Enum.sort_by(&{&1["schema"], &1["name"]}, :asc)
    }
  end

  defp encode_table(%PostgresDatabaseTable{} = table) do
    %{
      "oid" => table.oid,
      "schema" => table.schema,
      "name" => table.name,
      "columns" => Enum.map(table.columns, &encode_column/1)
    }
  end

  defp encode_column(%PostgresDatabaseTable.Column{} = column) do
    %{
      "attnum" => column.attnum,
      "isPk?" => column.is_pk?,
      "name" => column.name,
      "type" => column.type
    }
  end

  defp decode_params(params) do
    Sequin.Map.reject_nil_values(%{
      "name" => params["name"],
      "description" => params["description"],
      "transform" => decode_transform(params["transform"])
    })
  end

  defp decode_transform(%{"type" => "path"} = transform) do
    %{"type" => "path", "path" => transform["path"]}
  end

  defp decode_transform(%{"type" => "function"} = transform) do
    %{"type" => "function", "code" => transform["code"]}
  end

  defp decode_transform(%{"type" => "routing"} = transform) do
    %{"type" => "routing", "code" => transform["code"], "sink_type" => transform["sink_type"]}
  end

  defp decode_transform(%{}), do: nil

  defp schedule_poll_test_messages do
    Process.send_after(self(), :poll_test_messages, 1000)
  end
end
