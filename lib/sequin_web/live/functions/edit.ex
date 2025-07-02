defmodule SequinWeb.FunctionsLive.Edit do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Consumers
  alias Sequin.Consumers.FilterFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Functions.MiniElixir
  alias Sequin.Functions.TestMessages
  alias Sequin.Runtime.Routing
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
      endpoint_path: "",
      headers: %{}
    }
  end
  """

  @initial_route_redis_string """
  def route(action, record, changes, metadata) do
    table_name = metadata.table_name
    pks = Enum.join(metadata.record_pks, "-")

    redis_action =
      case action do
        "insert" -> "set"
        "update" -> "set"
        "delete" -> "del"
        "read" -> "set"
      end

    %{key: "sequin:\#{table_name}:\#{pks}", action: redis_action, expire_ms: nil}
  end
  """

  @initial_route_redis_stream """
  def route(action, record, changes, metadata) do
    %{stream_key: "sequin.\#{metadata.table_schema}.\#{metadata.table_name}"}
  end
  """

  @initial_route_nats """
  def route(action, record, changes, metadata) do
    %{
      subject: "sequin.\#{metadata.database_name}.\#{metadata.table_schema}.\#{metadata.table_name}.\#{action}",
      headers: %{"Nats-Msg-Id" => metadata.idempotency_key}
    }
  end
  """

  @initial_route_kafka """
  def route(action, record, changes, metadata) do
    %{topic: "sequin.\#{metadata.database.name}.\#{metadata.table_schema}.\#{metadata.table_name}"}
  end
  """

  @initial_route_typesense """
  def route(action, record, changes, metadata) do
    typesense_action =
      case action do
        "insert" -> "index"
        "update" -> "index"
        "delete" -> "delete"
        # backfills emit a read action
        "read" -> "index"
      end

    %{
      action: typesense_action,
      collection_name: "\#{metadata.table_schema}.\#{metadata.table_name}"
    }
  end
  """

  @initial_route_meilisearch """
  def route(action, record, changes, metadata) do
    %{
      index_name: "\#{metadata.table_schema}.\#{metadata.table_name}"
    }
  end
  """

  @initial_route_gcp_pubsub """
  def route(action, record, changes, metadata) do
    %{
      topic_id: "sequin.\#{metadata.database_name}.\#{metadata.table_schema}.\#{metadata.table_name}"
    }
  end
  """

  @initial_route_elasticsearch """
  def route(action, record, changes, metadata) do
    %{index_name: "sequin.\#{metadata.database_name}.\#{metadata.table_schema}.\#{metadata.table_name}"}
  end
  """

  @initial_route_sqs """
  def route(action, record, changes, metadata) do
    %{
      queue_url: "https://sqs.<region>.amazonaws.com/<account_id>/<queue-name>"
    }
  end
  """

  @initial_filter """
  def filter(action, record, changes, metadata) do
    # Must return true or false!
    true # Keep everything by default
  end
  """

  @initial_code_map %{
    "path" => "",
    "transform" => @initial_transform,
    "filter" => @initial_filter,
    "routing_undefined" => @initial_route_no_sink_type,
    "routing_http_push" => @initial_route_http,
    "routing_redis_string" => @initial_route_redis_string,
    "routing_nats" => @initial_route_nats,
    "routing_kafka" => @initial_route_kafka,
    "routing_gcp_pubsub" => @initial_route_gcp_pubsub,
    "routing_typesense" => @initial_route_typesense,
    "routing_meilisearch" => @initial_route_meilisearch,
    "routing_redis_stream" => @initial_route_redis_stream,
    "routing_elasticsearch" => @initial_route_elasticsearch,
    "routing_sqs" => @initial_route_sqs
  }

  # We generate the function completions at compile time because
  # docs are not available at runtime in our release.
  @function_completions AutoComplete.function_completions()

  @impl Phoenix.LiveView
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
        show_errors?: false,
        selected_database_id: nil,
        selected_table_oid: nil,
        synthetic_test_message: Consumers.synthetic_message(),
        initial_code: @initial_code_map,
        function_completions: @function_completions,
        function_transforms_enabled: Sequin.feature_enabled?(current_account_id(socket), :function_transforms)
      )
      |> assign_encoded_messages()
      |> assign_databases()

    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}
  end

  @impl Phoenix.LiveView
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
            testMessages: @encoded_test_messages,
            syntheticTestMessages: @encoded_synthetic_test_message,
            usedByConsumers: Enum.map(@used_by_consumers, &encode_consumer/1),
            databases: Enum.map(@databases, &encode_database/1),
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

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, _action, _params) do
    %{id: id} = socket.assigns

    title =
      case id do
        nil ->
          "Create Function"

        _ ->
          Ecto.Changeset.get_field(socket.assigns.changeset, :name)
      end

    socket
    |> assign(:page_title, "#{title} | Sequin")
    |> assign(:live_action, :index)
  end

  @impl Phoenix.LiveView
  def handle_info(:poll_test_messages, socket) do
    database_id = socket.assigns.selected_database_id
    table_oid = socket.assigns.selected_table_oid

    schedule_poll_test_messages()

    if database_id && table_oid do
      test_messages = TestMessages.get_test_messages(database_id, table_oid)
      existing_test_messages = socket.assigns.test_messages

      # Merge new messages with existing modifications
      merged_messages =
        Enum.map(test_messages, fn msg ->
          case Enum.find(existing_test_messages, &(&1.replication_message_trace_id == msg.replication_message_trace_id)) do
            nil ->
              msg

            existing_msg ->
              # Preserve user modifications
              %{msg | data: Map.merge(msg.data, existing_msg.data)}
          end
        end)

      # Preserve any user modifications from the existing message
      if length(merged_messages) >= @max_test_messages do
        TestMessages.unregister_needs_messages(database_id)
      end

      socket =
        socket
        |> assign(test_messages: merged_messages)
        |> assign_encoded_messages()

      {:noreply, socket}
    else
      socket =
        socket
        |> assign(test_messages: [])
        |> assign_encoded_messages()

      {:noreply, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("validate", %{"function" => params}, socket) do
    changeset =
      %Function{account_id: current_account_id(socket)}
      |> Function.changeset(params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset)
    form_errors = Sequin.Error.errors_on(changeset)

    # Validate and transform the serialized maps using MiniElixir
    modified_test_messages =
      case params["modified_test_messages"] do
        nil ->
          %{}

        messages when is_map(messages) ->
          Map.new(messages, fn {replication_message_trace_id, message} ->
            {replication_message_trace_id, validate_test_message(message)}
          end)
      end

    synthetic_test_message = socket.assigns.synthetic_test_message

    synthetic_test_message =
      case modified_test_messages[synthetic_test_message.replication_message_trace_id] do
        {:ok, result} ->
          %{synthetic_test_message | data: Map.merge(synthetic_test_message.data, result)}

        _ ->
          synthetic_test_message
      end

    test_messages =
      Enum.map(socket.assigns.test_messages, fn message ->
        case modified_test_messages[message.replication_message_trace_id] do
          {:ok, result} ->
            %{message | data: Map.merge(message.data, result)}

          _ ->
            message
        end
      end)

    form_errors =
      Map.put(
        form_errors,
        :modified_test_messages,
        Enum.reduce(modified_test_messages, %{}, fn {replication_message_trace_id, result}, acc ->
          case result do
            {:ok, _} -> acc
            %{error: errors} -> Map.put(acc, replication_message_trace_id, errors)
          end
        end)
      )

    socket =
      socket
      |> assign(:changeset, changeset)
      |> assign(:form_data, form_data)
      |> assign(:form_errors, form_errors)
      |> assign(:synthetic_test_message, synthetic_test_message)
      |> assign(:test_messages, test_messages)
      |> assign_encoded_messages()

    {:reply, %{ok: true}, socket}
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
        {:noreply, socket |> assign(test_messages: messages) |> assign_encoded_messages()}

      messages ->
        {:noreply, socket |> assign(test_messages: messages) |> assign_encoded_messages()}
    end
  end

  def handle_event("delete_test_message", %{"replication_message_trace_id" => replication_message_trace_id}, socket) do
    database_id = socket.assigns.selected_database_id
    table_oid = socket.assigns.selected_table_oid

    if TestMessages.delete_test_message(database_id, table_oid, replication_message_trace_id) do
      TestMessages.register_needs_messages(database_id)
      {:reply, %{deleted: true}, assign_encoded_messages(socket)}
    else
      {:reply, %{deleted: false}, socket}
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

  defp assign_encoded_messages(socket) do
    cache_key = {socket.assigns.form_data[:function], socket.assigns.test_messages, socket.assigns.synthetic_test_message}

    if socket.assigns[:cache_key] == cache_key do
      socket
    else
      do_assign_encoded_messages(socket, cache_key)
    end
  end

  defp do_assign_encoded_messages(socket, cache_key) do
    socket
    |> assign(:cache_key, cache_key)
    |> assign(
      :encoded_test_messages,
      encode_test_messages(
        socket.assigns.test_messages,
        socket.assigns.form_data,
        socket.assigns.form_errors,
        current_account_id(socket)
      )
    )
    |> assign(
      :encoded_synthetic_test_message,
      encode_synthetic_test_message(
        socket.assigns.synthetic_test_message,
        socket.assigns.form_data,
        socket.assigns.form_errors,
        current_account_id(socket)
      )
    )
  end

  defp encode_synthetic_test_message(synthetic_message, form_data, form_errors, account_id) do
    [synthetic_message]
    |> encode_test_messages(form_data, form_errors, account_id)
    |> Enum.map(&Map.put(&1, :isSynthetic, true))
  end

  defp encode_test_messages(test_messages, form_data, form_errors, account_id) do
    function = form_data[:function]
    function_errors = form_errors[:function]

    if is_struct(function) and function_errors == nil do
      do_encode_test_messages(test_messages, function, account_id)
    else
      Enum.map(test_messages, &format_test_message/1)
    end
  end

  defp do_encode_test_messages(test_messages, function, account_id) do
    function = %Function{account_id: account_id, type: function.type, function: function}
    consumer = %SinkConsumer{account_id: account_id}

    consumer =
      case function.function do
        %RoutingFunction{} ->
          %SinkConsumer{consumer | type: function.function.sink_type, routing: function}

        %TransformFunction{} ->
          %SinkConsumer{consumer | transform: function}

        %FilterFunction{} ->
          %SinkConsumer{consumer | filter: function}

        %PathFunction{} ->
          %SinkConsumer{consumer | transform: function}
      end

    Enum.map(test_messages, fn test_message ->
      encoded = encode_one(consumer, test_message)
      formatted = format_test_message(test_message)
      Map.merge(formatted, encoded)
    end)
  end

  defp encode_one(consumer, message) do
    {time, value} = :timer.tc(fn -> run_function(consumer, message) end)

    case Jason.encode(value) do
      {:ok, _} -> %{transformed: value, time: time}
      {:error, error} -> %{error: MiniElixir.encode_error(error), time: time}
    end
  rescue
    ex ->
      %{error: MiniElixir.encode_error(ex), time: nil}
  end

  defp format_test_message(m) do
    %{
      replication_message_trace_id: m.replication_message_trace_id,
      record: inspect(m.data.record, pretty: true),
      changes: inspect(m.data.changes, pretty: true),
      action: inspect(to_string(m.data.action), pretty: true),
      metadata: inspect(Sequin.Map.from_struct_deep(m.data.metadata), pretty: true)
    }
  end

  defp run_function(%SinkConsumer{transform: %Function{} = function}, message) do
    case function.function do
      %TransformFunction{} ->
        MiniElixir.run_interpreted(function, message.data)

      %PathFunction{} = path_function ->
        PathFunction.apply(path_function, message.data)
    end
  end

  defp run_function(%SinkConsumer{routing: %Function{} = _function} = sink_consumer, message) do
    sink_consumer
    |> Routing.route_message(message)
    |> Map.from_struct()
  end

  defp run_function(%SinkConsumer{filter: %Function{} = function}, message) do
    case MiniElixir.run_interpreted(function, message.data) do
      true -> true
      false -> false
      other -> raise Sequin.Error.invariant(message: "Filter function must return true or false, got: #{inspect(other)}")
    end
  end

  defp assign_databases(socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)

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

  defp decode_function(%{"type" => "filter"} = function) do
    %{"type" => "filter", "code" => function["code"]}
  end

  defp decode_function(%{}), do: nil

  defp schedule_poll_test_messages do
    Process.send_after(self(), :poll_test_messages, 1000)
  end

  defp validate_test_message(message) do
    with {:ok, record} <- validate_field(message["record"], "record"),
         {:ok, metadata} <- validate_field(message["metadata"], "metadata"),
         {:ok, changes} <- validate_field(message["changes"], "changes"),
         {:ok, action} <- validate_action(message["action"]) do
      {:ok,
       %{
         record: record,
         action: action,
         metadata: struct(Sequin.Consumers.ConsumerEventData.Metadata, metadata),
         changes: changes
       }}
    else
      {:error, field, error} -> %{error: %{field => error}}
    end
  end

  defp validate_field(value, field) when is_binary(value) do
    with {:ok, parsed} <- MiniElixir.eval_raw_string(value, field) do
      validate_field(parsed, field)
    end
  end

  defp validate_field(nil, _field), do: {:ok, nil}
  defp validate_field(value, _field) when is_map(value), do: {:ok, value}

  defp validate_field(_, field),
    do: {:error, field, %{type: "Type error", info: %{description: ~s(Must be a map such as %{"key" => "value"})}}}

  defp validate_action(action) when is_binary(action) do
    case action |> String.trim("\"") |> String.downcase() do
      action when action in ["insert", "update", "delete", "read"] ->
        {:ok, action}

      _ ->
        {:error, "action", "Action must be one of: insert, update, delete, read"}
    end
  end

  defp validate_action(_), do: {:error, "action", "Action must be a string"}
end
