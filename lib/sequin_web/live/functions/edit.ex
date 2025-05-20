defmodule SequinWeb.FunctionsLive.Edit do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Functions.MiniElixir
  alias Sequin.Functions.TestMessages
  alias Sequin.Repo
  alias Sequin.Runtime
  alias Sequin.Transforms.Message
  alias SequinWeb.FunctionLive.AutoComplete

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
      method: "POST",
      endpoint_path: "/entities/\#{record["id"]}"
    }
  end
  """

  @initial_route_redis_string """
  def route(action, record, changes, metadata) do
    prefix = "sequin:\#{metadata.table_name}"
    key = "\#{prefix}:\#{record["id"]}"

    %{key: key}
  end
  """

  @initial_code_map %{
    "path" => "",
    "transform" => @initial_transform,
    "routing_undefined" => @initial_route_no_sink_type,
    "routing_http_push" => @initial_route_http,
    "routing_redis_string" => @initial_route_redis_string
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
          function =
            Sequin.Map.reject_nil_values(%{
              type: params["type"],
              sink_type: params["sink_type"]
            })

          Function.changeset(%Function{account_id: current_account_id(socket)}, %{"function" => function})

        id ->
          function = Consumers.get_function_for_account!(current_account_id(socket), id)
          Function.changeset(function, %{})
      end

    used_by_consumers =
      if id do
        Consumers.list_consumers_for_function(current_account_id(socket), id, [:replication_slot])
      else
        []
      end

    socket =
      socket
      |> assign(
        id: id,
        account_id: current_account_id(socket),
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
        function_completions: @function_completions,
        function_transforms_enabled: Sequin.feature_enabled?(current_account_id(socket), :function_transforms)
      )
      |> assign_databases()

    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}
  end

  def render(assigns) do
    ~H"""
    <div id="function_new">
      <.svelte
        name="functions/Edit"
        props={
          %{
            formData: @form_data,
            showErrors: @show_errors?,
            formErrors: @form_errors,
            testMessages:
              encode_test_messages(
                @test_messages,
                @form_data,
                @form_errors,
                @account_id
              ),
            syntheticTestMessages:
              encode_synthetic_test_message(
                @synthetic_test_message,
                @form_data,
                @form_errors,
                @account_id
              ),
            usedByConsumers: Enum.map(@used_by_consumers, &encode_consumer/1),
            databases: Enum.map(@databases, &encode_database/1),
            validating: @validating,
            parent: "function_new",
            initialCodeMap: @initial_code,
            initialCode: "glugma",
            functionTransformsEnabled: @function_transforms_enabled,
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

  def handle_event("validate", %{"function" => params}, socket) do
    changeset =
      %Function{account_id: current_account_id(socket)}
      |> Function.changeset(params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset)
    form_errors = Sequin.Error.errors_on(changeset)

    {:noreply,
     socket
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)}
  end

  def handle_event("save", %{"function" => params}, socket) do
    params = decode_params(params)

    case upsert_function(socket, params) do
      {:ok, :created} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Function created successfully"})
         |> push_navigate(to: ~p"/functions")}

      {:ok, :updated} ->
        socket.assigns.used_by_consumers
        |> Enum.map(& &1.replication_slot)
        |> Enum.uniq_by(& &1.id)
        |> Enum.each(&Runtime.Supervisor.restart_replication/1)

        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Function updated successfully"})
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
    case Consumers.delete_function(current_account_id(socket), socket.assigns.id) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Function deleted successfully"})
         |> push_navigate(to: ~p"/functions")}

      {:error, error} ->
        Logger.error("[Function.Edit] Failed to delete function", error: error)
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete function"})}
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

  def handle_event("form_closed", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/functions")}
  end

  defp upsert_function(%{assigns: %{id: nil}} = socket, params) do
    with {:ok, _function} <- Consumers.create_function(current_account_id(socket), params) do
      {:ok, :created}
    end
  end

  defp upsert_function(%{assigns: %{id: id}} = socket, params) do
    with {:ok, _} <- Consumers.update_function(current_account_id(socket), id, params) do
      {:ok, :updated}
    end
  end

  defp changeset_to_form_data(changeset) do
    function = Ecto.Changeset.get_field(changeset, :function)

    function_data =
      case function do
        nil ->
          %{}

        %Ecto.Changeset{} ->
          Ecto.Changeset.apply_changes(function)

        %_{} = struct ->
          struct
      end

    %{
      id: Ecto.Changeset.get_field(changeset, :id),
      name: Ecto.Changeset.get_field(changeset, :name),
      description: Ecto.Changeset.get_field(changeset, :description),
      function: function_data
    }
  end

  defp encode_synthetic_test_message(synthetic_message, form_data, form_errors, account_id) do
    [synthetic_message]
    |> encode_test_messages(form_data, form_errors, account_id)
    |> Enum.map(&Map.put(&1, :isSynthetic, true))
  end

  defp encode_test_messages(test_messages, form_data, form_errors, account_id) do
    if is_nil(get_in(form_errors, [:function, :code])) do
      do_encode_test_messages(test_messages, form_data, account_id)
    else
      Enum.map(test_messages, &prepare_test_message/1)
    end
  end

  defp do_encode_test_messages(test_messages, form_data, account_id) do
    function = form_data[:function]

    consumer = %SinkConsumer{
      transform: %Function{account_id: account_id, function: function},
      legacy_transform: :none
    }

    consumer =
      case function do
        %RoutingFunction{} -> %{consumer | type: function.sink_type}
        _ -> consumer
      end

    base_messages = Enum.map(test_messages, &prepare_test_message/1)

    if is_struct(function) do
      results = Enum.map(test_messages, &encode_one(&1, consumer))
      Enum.zip_with(base_messages, results, &Map.merge/2)
    else
      base_messages
    end
  end

  defp encode_one(message, consumer) do
    {time, value} = :timer.tc(Message, :to_external, [consumer, message], :microsecond)

    case Jason.encode(value) do
      {:ok, _} -> %{transformed: value, time: time}
      {:error, error} -> %{error: MiniElixir.encode_error(error), time: time}
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
        |> Databases.reject_sequin_internal_tables()
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
      "function" => decode_function(params["function"])
    })
  end

  defp decode_function(%{"type" => "path"} = function) do
    %{"type" => "path", "path" => function["path"]}
  end

  defp decode_function(%{"type" => "transform"} = function) do
    %{"type" => "transform", "code" => function["code"]}
  end

  defp decode_function(%{"type" => "routing"} = function) do
    %{"type" => "routing", "code" => function["code"], "sink_type" => function["sink_type"]}
  end

  defp decode_function(%{}), do: nil

  defp schedule_poll_test_messages do
    Process.send_after(self(), :poll_test_messages, 1000)
  end
end
