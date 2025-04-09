defmodule SequinWeb.Components.ConsumerForm do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.Transform
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Name
  alias Sequin.Postgres
  alias Sequin.Posthog
  alias Sequin.Repo
  alias Sequin.Runtime
  alias Sequin.Runtime.KeysetCursor
  alias Sequin.Sinks.Azure.EventHub
  alias Sequin.Sinks.Gcp.Credentials
  alias Sequin.Sinks.Gcp.PubSub
  alias Sequin.Sinks.Kafka
  alias Sequin.Sinks.Nats
  alias Sequin.Sinks.RabbitMq
  alias Sequin.Sinks.Redis
  alias SequinWeb.RouteHelpers

  require Logger

  @impl Phoenix.LiveComponent
  def render(assigns) do
    encoded_errors =
      if assigns.show_errors? do
        %{
          consumer: encode_errors(assigns.changeset),
          sequence: encode_errors(assigns.sequence_changeset)
        }
      else
        %{consumer: %{}, sequence: %{}}
      end

    assigns =
      assigns
      |> assign(:encoded_consumer, encode_consumer(assigns.consumer))
      |> assign(:encoded_errors, encoded_errors)
      |> assign(:encoded_databases, Enum.map(assigns.databases, &encode_database/1))
      |> assign(:encoded_http_endpoints, Enum.map(assigns.http_endpoints, &encode_http_endpoint/1))
      |> assign(:encoded_transforms, Enum.map(assigns.transforms, &encode_transform/1))
      |> assign(:consumer_title, consumer_title(assigns.consumer))
      |> assign(:self_hosted, self_hosted?())

    ~H"""
    <div id={@id}>
      <.svelte
        name={@component}
        ssr={false}
        props={
          %{
            consumer: @encoded_consumer,
            consumerTitle: @consumer_title,
            errors: @encoded_errors,
            submitError: @submit_error,
            parent: @id,
            databases: @encoded_databases,
            httpEndpoints: @encoded_http_endpoints,
            transforms: @encoded_transforms,
            isSelfHosted: @self_hosted
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveComponent
  def update(%{event: :database_tables_updated}, socket) do
    socket = assign_databases(socket)
    {:ok, socket}
  end

  # If the changeset is not nil, we avoid re-updating the assigns
  # the parent will cause update to fire if the props change
  # if you need to handle props changing, refactor - be sure to preserve the changeset
  @impl Phoenix.LiveComponent
  def update(_assigns, %{assigns: %{changeset: %Ecto.Changeset{}}} = socket) do
    {:ok, socket}
  end

  @impl Phoenix.LiveComponent
  def update(assigns, socket) do
    consumer = assigns[:consumer]
    component = "consumers/SinkConsumerForm"

    socket =
      socket
      |> assign(assigns)
      |> assign(
        consumer: Repo.preload(consumer, [:sequence, :postgres_database]),
        show_errors?: false,
        submit_error: nil,
        changeset: nil,
        sequence_changeset: nil,
        component: component,
        prev_params: %{}
      )
      |> assign_databases()
      |> assign_http_endpoints()
      |> assign_transforms()
      |> reset_changeset()

    :syn.join(:account, {:database_tables_updated, current_account_id(socket)}, self())

    {:ok, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("validate", _params, socket) do
    {:noreply, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("form_updated", %{"form" => form}, socket) do
    params = form |> decode_params(socket) |> maybe_put_replication_slot_id(socket)

    socket =
      socket
      |> merge_changeset(params)
      |> assign(prev_params: params)

    {:noreply, socket}
  end

  def handle_event("form_submitted", %{"form" => form}, socket) do
    socket = assign(socket, :submit_error, nil)

    params =
      form
      |> decode_params(socket)
      |> maybe_put_replication_slot_id(socket)

    res =
      if is_edit?(socket) do
        update_consumer(socket, params)
      else
        create_consumer(socket, params)
      end

    case res do
      {:ok, socket} ->
        {:reply, %{ok: true}, socket}

      {:error, socket} ->
        socket = assign(socket, :show_errors?, true)
        {:reply, %{ok: false}, socket}
    end
  end

  def handle_event("form_closed", _params, socket) do
    consumer = socket.assigns.consumer

    socket =
      if is_edit?(socket) do
        push_navigate(socket, to: RouteHelpers.consumer_path(consumer))
      else
        push_navigate(socket, to: ~p"/sinks")
      end

    {:noreply, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("refresh_databases", _params, socket) do
    {:noreply, assign_databases(socket)}
  end

  @impl Phoenix.LiveComponent
  def handle_event("refresh_tables", %{"database_id" => database_id}, socket) do
    with index when not is_nil(index) <- Enum.find_index(socket.assigns.databases, &(&1.id == database_id)),
         database = Enum.at(socket.assigns.databases, index),
         {:ok, updated_database} <- Databases.update_tables(database) do
      updated_databases = List.replace_at(socket.assigns.databases, index, updated_database)
      {:noreply, assign(socket, databases: updated_databases)}
    else
      _ ->
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to refresh tables"})}
    end
  end

  @impl Phoenix.LiveComponent
  def handle_event("refresh_sequences", %{"database_id" => database_id}, socket) do
    with {:ok, database} <- Databases.get_db(database_id),
         {:ok, _updated_database} <- Databases.update_tables(database) do
      {:noreply, assign_databases(socket)}
    else
      _ -> {:noreply, socket}
    end
  end

  def handle_event("generate_webhook_site_url", _params, socket) do
    case generate_webhook_site_endpoint(socket) do
      {:ok, %HttpEndpoint{} = http_endpoint} ->
        {:reply, %{http_endpoint_id: http_endpoint.id}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  def handle_event("refresh_http_endpoints", _params, socket) do
    {:noreply, assign_http_endpoints(socket)}
  end

  def handle_event("refresh_transforms", _params, socket) do
    {:noreply, assign_transforms(socket)}
  end

  def handle_event("test_connection", _params, socket) do
    case socket.assigns.consumer.type do
      :rabbitmq ->
        case test_rabbit_mq_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :nats ->
        case test_nats_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :sqs ->
        case test_sqs_connection(socket) do
          :ok ->
            {:reply, %{ok: true}, socket}

          {:error, error} ->
            Logger.error("SQS connection test failed", error: error)
            {:reply, %{ok: false, error: error}, socket}
        end

      :sns ->
        case test_sns_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :redis ->
        case test_redis_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :kafka ->
        case test_kafka_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :gcp_pubsub ->
        case test_pubsub_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end

      :azure_event_hub ->
        case test_azure_event_hub_connection(socket) do
          :ok -> {:reply, %{ok: true}, socket}
          {:error, error} -> {:reply, %{ok: false, error: error}, socket}
        end
    end
  end

  defp test_sqs_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %SqsSink{} = sink -> SqsSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)
      client = SqsSink.aws_client(sink)

      case Sequin.Aws.SQS.queue_meta(client, sink.queue_url) do
        {:ok, _} -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_sns_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %SnsSink{} = sink -> SnsSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)
      client = SnsSink.aws_client(sink)

      case Sequin.Aws.SNS.topic_meta(client, sink.topic_arn) do
        {:ok, _} -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_redis_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %RedisSink{} = sink -> RedisSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)

      case Redis.test_connection(sink) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_kafka_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %KafkaSink{} = sink -> KafkaSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)

      case Kafka.test_connection(sink) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_pubsub_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %GcpPubsubSink{} = sink -> GcpPubsubSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)

      client = GcpPubsubSink.pubsub_client(sink)

      case PubSub.test_connection(client, sink.topic_id) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_rabbit_mq_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %RabbitMqSink{} = sink -> RabbitMqSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)

      case RabbitMq.test_connection(sink) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_nats_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %NatsSink{} = sink -> NatsSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      sink = Ecto.Changeset.apply_changes(sink_changeset)

      case Nats.test_connection(sink) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp test_azure_event_hub_connection(socket) do
    sink_changeset =
      socket.assigns.changeset
      |> Ecto.Changeset.get_field(:sink)
      |> case do
        %Ecto.Changeset{} = changeset -> changeset
        %AzureEventHubSink{} = sink -> AzureEventHubSink.changeset(sink, %{})
      end

    if sink_changeset.valid? do
      client =
        sink_changeset
        |> Ecto.Changeset.apply_changes()
        |> AzureEventHubSink.event_hub_client()

      case EventHub.test_connection(client) do
        :ok -> :ok
        {:error, error} -> {:error, Exception.message(error)}
      end
    else
      {:error, encode_errors(sink_changeset)}
    end
  end

  defp decode_params(form, socket) do
    params =
      %{
        "consumer_kind" => form["consumerKind"],
        "ack_wait_ms" => form["ackWaitMs"],
        "sink" => decode_sink(socket.assigns.consumer.type, form["sink"]),
        "max_ack_pending" => form["maxAckPending"],
        "max_waiting" => form["maxWaiting"],
        "max_memory_mb" => form["maxMemoryMb"],
        "message_kind" => form["messageKind"],
        "name" => form["name"],
        "postgres_database_id" => form["postgresDatabaseId"],
        "table_oid" => form["tableOid"],
        "sequence_filter" => %{
          "column_filters" => Enum.map(form["sourceTableFilters"], &ColumnFilter.from_external/1),
          "actions" => form["sourceTableActions"],
          "group_column_attnums" => form["groupColumnAttnums"]
        },
        "batch_size" => form["batchSize"],
        "initial_backfill" => decode_initial_backfill(form),
        "transform_id" => if(form["transform"] === "none", do: nil, else: form["transform"])
      }

    maybe_put_replication_slot_id(params, socket)
  end

  defp decode_initial_backfill(%{"backfill" => %{"startPosition" => "none"}}), do: nil

  defp decode_initial_backfill(%{"backfill" => backfill}) do
    %{
      "start_position" => backfill["startPosition"],
      "initial_min_sort_col" => backfill["initialSortColumnValue"],
      "sort_column_attnum" => backfill["sortColumnAttnum"]
    }
  end

  defp decode_sink(:pull, _form), do: nil

  defp decode_sink(:http_push, sink) do
    %{
      "type" => "http_push",
      "http_endpoint_id" => sink["httpEndpointId"],
      "http_endpoint_path" => sink["httpEndpointPath"]
    }
  end

  defp decode_sink(:sqs, sink) do
    %{
      "type" => "sqs",
      "queue_url" => sink["queue_url"],
      "region" => aws_region_from_queue_url(sink["queue_url"]),
      "access_key_id" => sink["access_key_id"],
      "secret_access_key" => sink["secret_access_key"]
    }
  end

  defp decode_sink(:kafka, sink) do
    %{
      "type" => "kafka",
      "hosts" => sink["hosts"],
      "username" => sink["username"],
      "password" => sink["password"],
      "topic" => sink["topic"],
      "tls" => sink["tls"],
      "sasl_mechanism" => sink["sasl_mechanism"],
      "aws_access_key_id" => sink["aws_access_key_id"],
      "aws_secret_access_key" => sink["aws_secret_access_key"],
      "aws_region" => sink["aws_region"],
      "batch_size" => sink["batch_size"] && String.to_integer(sink["batch_size"])
    }
  end

  defp decode_sink(:redis, sink) do
    %{
      "type" => "redis",
      "host" => sink["host"],
      "port" => sink["port"],
      "stream_key" => sink["streamKey"],
      "database" => sink["database"],
      "tls" => sink["tls"],
      "username" => sink["username"],
      "password" => sink["password"]
    }
  end

  defp decode_sink(:rabbitmq, sink) do
    %{
      "type" => "rabbitmq",
      "host" => sink["host"],
      "port" => sink["port"],
      "exchange" => sink["exchange"],
      "username" => sink["username"],
      "password" => sink["password"],
      "virtual_host" => sink["virtual_host"],
      "tls" => sink["tls"]
    }
  end

  defp decode_sink(:nats, sink) do
    %{
      "type" => "nats",
      "host" => sink["host"],
      "port" => sink["port"],
      "username" => sink["username"],
      "password" => sink["password"],
      "jwt" => sink["jwt"],
      "nkey_seed" => sink["nkey_seed"],
      "tls" => sink["tls"]
    }
  end

  defp decode_sink(:sequin_stream, _sink) do
    %{
      "type" => "sequin_stream"
    }
  end

  defp decode_sink(:gcp_pubsub, sink) do
    creds =
      with true <- is_binary(sink["credentials"]),
           {:ok, creds} <- Jason.decode(sink["credentials"]) do
        creds
      else
        _ -> sink["credentials"]
      end

    %{
      "type" => "gcp_pubsub",
      "project_id" => sink["project_id"],
      "topic_id" => sink["topic_id"],
      "credentials" => creds,
      "use_emulator" => sink["use_emulator"],
      "emulator_base_url" => sink["emulator_base_url"]
    }
  end

  defp decode_sink(:azure_event_hub, sink) do
    %{
      "type" => "azure_event_hub",
      "namespace" => sink["namespace"],
      "event_hub_name" => sink["event_hub_name"],
      "shared_access_key_name" => sink["shared_access_key_name"],
      "shared_access_key" => sink["shared_access_key"]
    }
  end

  defp aws_region_from_queue_url(nil), do: nil

  defp aws_region_from_queue_url(queue_url) do
    case SqsSink.region_from_url(queue_url) do
      {:ok, region} -> region
      _ -> nil
    end
  end

  defp encode_consumer(nil), do: nil

  defp encode_consumer(%_{} = consumer) do
    postgres_database_id =
      if is_struct(consumer.postgres_database, PostgresDatabase), do: consumer.postgres_database.id

    source_table = Consumers.source_table(consumer)

    %{
      "id" => consumer.id,
      "name" => consumer.name || Name.generate(999),
      "ack_wait_ms" => consumer.ack_wait_ms,
      "batch_size" => Map.get(consumer, :batch_size),
      "max_memory_mb" => consumer.max_memory_mb,
      "group_column_attnums" => source_table && source_table.group_column_attnums,
      "max_ack_pending" => consumer.max_ack_pending,
      "max_deliver" => consumer.max_deliver,
      "max_waiting" => consumer.max_waiting,
      "message_kind" => consumer.message_kind,
      "postgres_database_id" => postgres_database_id,
      "sequence_filter" => consumer.sequence_filter && encode_sequence_filter(consumer.sequence_filter),
      "sequence_id" => consumer.sequence_id,
      "sink" => encode_sink(consumer.sink),
      "source_table_actions" => (source_table && source_table.actions) || [:insert, :update, :delete],
      "source_table_filters" => source_table && Enum.map(source_table.column_filters, &ColumnFilter.to_external/1),
      "status" => consumer.status,
      "table_oid" => source_table && source_table.oid,
      "type" => consumer.type,
      "transform_id" => consumer.transform_id
    }
  end

  defp encode_sequence_filter(%SequenceFilter{} = sequence_filter) do
    %{
      "column_filters" => Enum.map(sequence_filter.column_filters, &ColumnFilter.to_external/1),
      "actions" => sequence_filter.actions
    }
  end

  defp encode_sink(%HttpPushSink{} = sink) do
    %{
      "type" => "http_push",
      "httpEndpointId" => sink.http_endpoint_id,
      "httpEndpointPath" => sink.http_endpoint_path
    }
  end

  defp encode_sink(%SqsSink{} = sink) do
    %{
      "type" => "sqs",
      "queue_url" => sink.queue_url,
      "region" => sink.region,
      "access_key_id" => sink.access_key_id,
      "secret_access_key" => sink.secret_access_key,
      "is_fifo" => sink.is_fifo
    }
  end

  defp encode_sink(%KafkaSink{} = sink) do
    %{
      "type" => "kafka",
      "url" => KafkaSink.kafka_url(sink),
      "hosts" => sink.hosts,
      "username" => sink.username,
      "password" => sink.password,
      "topic" => sink.topic,
      "tls" => sink.tls,
      "sasl_mechanism" => sink.sasl_mechanism,
      "aws_access_key_id" => sink.aws_access_key_id,
      "aws_secret_access_key" => sink.aws_secret_access_key,
      "aws_region" => sink.aws_region
    }
  end

  defp encode_sink(%RedisSink{} = sink) do
    %{
      "type" => "redis",
      "host" => sink.host,
      "port" => sink.port,
      "streamKey" => sink.stream_key,
      "database" => sink.database,
      "tls" => sink.tls,
      "username" => sink.username,
      "password" => sink.password
    }
  end

  defp encode_sink(%RabbitMqSink{} = sink) do
    %{
      "type" => "rabbitmq",
      "host" => sink.host,
      "port" => sink.port,
      "tls" => sink.tls,
      "exchange" => sink.exchange,
      "username" => sink.username,
      "password" => sink.password,
      "virtual_host" => sink.virtual_host
    }
  end

  defp encode_sink(%NatsSink{} = sink) do
    %{
      "type" => "nats",
      "host" => sink.host,
      "port" => sink.port,
      "username" => sink.username,
      "password" => sink.password,
      "jwt" => sink.jwt,
      "nkey_seed" => sink.nkey_seed,
      "tls" => sink.tls
    }
  end

  defp encode_sink(%SequinStreamSink{}) do
    %{
      "type" => "sequin_stream"
    }
  end

  defp encode_sink(%GcpPubsubSink{} = sink) do
    credentials = sink.credentials || %Credentials{}

    creds = %{
      "type" => credentials.type,
      "project_id" => credentials.project_id,
      "private_key_id" => credentials.private_key_id,
      "private_key" => credentials.private_key,
      "client_email" => credentials.client_email,
      "client_id" => credentials.client_id,
      "auth_uri" => credentials.auth_uri,
      "token_uri" => credentials.token_uri,
      "auth_provider_x509_cert_url" => credentials.auth_provider_x509_cert_url,
      "client_x509_cert_url" => credentials.client_x509_cert_url,
      "universe_domain" => credentials.universe_domain,
      "client_secret" => credentials.client_secret,
      "api_key" => credentials.api_key
    }

    %{
      "type" => "gcp_pubsub",
      "project_id" => sink.project_id,
      "topic_id" => sink.topic_id,
      "use_emulator" => sink.use_emulator,
      "emulator_base_url" => sink.emulator_base_url,
      "credentials" => Jason.encode!(Sequin.Map.reject_nil_values(creds), pretty: true)
    }
  end

  defp encode_sink(%AzureEventHubSink{} = sink) do
    %{
      "type" => "azure_event_hub",
      "namespace" => sink.namespace,
      "event_hub_name" => sink.event_hub_name,
      "shared_access_key_name" => sink.shared_access_key_name,
      "shared_access_key" => sink.shared_access_key
    }
  end

  defp encode_errors(nil), do: %{}

  defp encode_errors(%Ecto.Changeset{} = changeset) do
    Error.errors_on(changeset)
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
    default_group_columns = PostgresDatabaseTable.default_group_column_attnums(table)

    %{
      "oid" => table.oid,
      "schema" => table.schema,
      "name" => table.name,
      "default_group_columns" => default_group_columns,
      "is_event_table" => Postgres.is_event_table?(table),
      "columns" => Enum.map(table.columns, &encode_column/1)
    }
  end

  defp encode_column(%PostgresDatabaseTable.Column{} = column) do
    %{
      "attnum" => column.attnum,
      "isPk?" => column.is_pk?,
      "name" => column.name,
      "type" => column.type,
      "filterType" => Postgres.pg_simple_type_to_filter_type(column.type)
    }
  end

  defp encode_transform(%Transform{} = transform) do
    %{
      "id" => transform.id,
      "name" => transform.name,
      "type" => transform.type,
      "description" => transform.description
    }
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      "id" => http_endpoint.id,
      "name" => http_endpoint.name,
      "baseUrl" => HttpEndpoint.url(http_endpoint)
    }
  end

  defp update_consumer(socket, params) do
    consumer = socket.assigns.consumer

    case Consumers.update_sink_consumer(consumer, params) do
      {:ok, updated_consumer} ->
        socket =
          socket
          |> assign(:consumer, updated_consumer)
          |> push_navigate(to: RouteHelpers.consumer_path(updated_consumer))

        {:ok, socket}

      {:error, %Ecto.Changeset{} = changeset} ->
        Logger.info("Update consumer failed validation: #{inspect(Error.errors_on(changeset), pretty: true)}")
        {:error, assign(socket, :changeset, changeset)}
    end
  end

  defp create_consumer(socket, params) do
    account_id = current_account_id(socket)
    initial_backfill = params["initial_backfill"]

    result =
      Repo.transact(fn ->
        with {:ok, sequence} <- find_or_create_sequence(account_id, params),
             params = Map.put(params, "sequence_id", sequence.id),
             {:ok, consumer} <- Consumers.create_sink_consumer(account_id, params) do
          case maybe_create_backfill(socket, consumer, params, initial_backfill) do
            :ok -> {:ok, Repo.preload(consumer, :active_backfill)}
            {:ok, %Backfill{}} -> {:ok, Repo.preload(consumer, :active_backfill)}
            {:error, changeset} -> {:error, changeset}
          end
        end
      end)

    case result do
      {:ok, consumer} ->
        case consumer.active_backfill do
          nil -> :ok
          %Backfill{state: :active} -> Runtime.Supervisor.start_table_reader(consumer)
          %Backfill{state: _} -> :ok
        end

        Posthog.capture("Consumer Created", %{
          distinct_id: socket.assigns.current_user.id,
          properties: %{
            consumer_type: consumer.type,
            stream_type: consumer.message_kind,
            consumer_id: consumer.id,
            consumer_name: consumer.name,
            "$groups": %{account: consumer.account_id}
          }
        })

        {:ok, push_navigate(socket, to: RouteHelpers.consumer_path(consumer))}

      {:error, %Ecto.Changeset{data: %Backfill{}} = changeset} ->
        Logger.info("Create backfill failed validation: #{inspect(Error.errors_on(changeset), pretty: true)}")
        {:error, assign(socket, :backfill_changeset, changeset)}

      {:error, %Ecto.Changeset{data: %Sequence{}} = changeset} ->
        Logger.info("Create sequence failed validation: #{inspect(Error.errors_on(changeset), pretty: true)}")
        {:error, assign(socket, :sequence_changeset, changeset)}

      {:error, %Ecto.Changeset{data: %SinkConsumer{}} = changeset} ->
        Logger.info("Create consumer failed validation: #{inspect(Error.errors_on(changeset), pretty: true)}")

        error_message =
          case changeset.errors do
            [{:name, {"has already been taken", _opts}} | _] -> "Name has already been taken"
            _ -> "Failed to create consumer"
          end

        {:error, socket |> assign(:changeset, changeset) |> assign(:submit_error, error_message)}
    end
  end

  defp maybe_create_backfill(_socket, _consumer, _params, nil), do: :ok

  defp maybe_create_backfill(socket, consumer, params, backfill_params) do
    table =
      table(
        socket.assigns.databases,
        params["postgres_database_id"],
        params["table_oid"],
        backfill_params["sort_column_attnum"]
      )

    initial_min_cursor =
      case backfill_params["start_position"] do
        "beginning" ->
          KeysetCursor.min_cursor(table)

        "specific" ->
          sort_col = backfill_params["initial_min_sort_col"]
          if sort_col, do: KeysetCursor.min_cursor(table, sort_col)
      end

    backfill_attrs = %{
      "account_id" => consumer.account_id,
      "sink_consumer_id" => consumer.id,
      "initial_min_cursor" => initial_min_cursor,
      "sort_column_attnum" => backfill_params["sort_column_attnum"],
      "state" => :active
    }

    Consumers.create_backfill(backfill_attrs)
  end

  defp find_or_create_sequence(account_id, %{"table_oid" => table_oid, "postgres_database_id" => postgres_database_id}) do
    case Databases.find_sequence_for_account(account_id, postgres_database_id: postgres_database_id, table_oid: table_oid) do
      {:ok, sequence} ->
        {:ok, sequence}

      {:error, %NotFoundError{}} ->
        Logger.info("Creating sequence for table #{table_oid}")

        case Databases.create_sequence(account_id, %{
               name: Ecto.UUID.generate(),
               table_oid: table_oid,
               postgres_database_id: postgres_database_id
             }) do
          {:ok, sequence} -> {:ok, sequence}
          {:error, changeset} -> {:error, changeset}
        end
    end
  end

  defp reset_changeset(socket) do
    consumer = socket.assigns.consumer
    assign(socket, :changeset, changeset(socket, consumer, %{}))
  end

  defp merge_changeset(socket, params) do
    consumer = socket.assigns.consumer
    assign(socket, :changeset, changeset(socket, consumer, params))
  end

  defp changeset(socket, %SinkConsumer{id: nil}, params) do
    account_id = current_account_id(socket)

    SinkConsumer.create_changeset(%SinkConsumer{account_id: account_id}, params)
  end

  defp changeset(_socket, %SinkConsumer{} = consumer, params) do
    SinkConsumer.update_changeset(consumer, params)
  end

  # user is in wizard and hasn't selected a consumer_kind yet
  defp changeset(_socket, nil, _params) do
    nil
  end

  defp assign_databases(socket) do
    account_id = current_account_id(socket)

    databases =
      account_id
      |> Databases.list_dbs_for_account()
      |> Repo.preload(:sequences)

    assign(socket, :databases, databases)
  end

  defp assign_http_endpoints(socket) do
    account_id = current_account_id(socket)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)
    assign(socket, :http_endpoints, http_endpoints)
  end

  defp assign_transforms(socket) do
    account_id = current_account_id(socket)
    transforms = Consumers.list_transforms_for_account(account_id)
    assign(socket, :transforms, transforms)
  end

  defp maybe_put_replication_slot_id(%{"postgres_database_id" => nil} = params, _socket) do
    params
  end

  defp maybe_put_replication_slot_id(%{"postgres_database_id" => postgres_database_id} = params, socket) do
    case Databases.get_db_for_account(current_account_id(socket), postgres_database_id) do
      {:ok, database} ->
        database = Repo.preload(database, :replication_slot)
        Map.put(params, "replication_slot_id", database.replication_slot.id)

      _ ->
        Map.merge(params, %{"replication_slot_id" => nil, "postgres_database_id" => nil})
    end
  end

  defp generate_webhook_site_endpoint(socket) do
    case Consumers.WebhookSiteGenerator.generate() do
      {:ok, uuid} ->
        Consumers.create_http_endpoint(current_account_id(socket), %{
          name: "webhook-site-#{String.slice(uuid, 0, 8)}",
          scheme: :https,
          host: "webhook.site",
          path: "/#{uuid}"
        })

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp is_edit?(socket) do
    not is_nil(socket.assigns.consumer) and not is_nil(socket.assigns.consumer.id)
  end

  defp consumer_title(consumer) do
    case consumer.type do
      :http_push -> "Webhook Sink"
      :kafka -> "Kafka Sink"
      :pull -> "Consumer Group"
      :redis -> "Redis Sink"
      :sqs -> "SQS Sink"
      :sequin_stream -> "Sequin Stream Sink"
      :gcp_pubsub -> "GCP Pub/Sub Sink"
      :nats -> "NATS Sink"
      :rabbitmq -> "RabbitMQ Sink"
      :azure_event_hub -> "Azure Event Hub Sink"
    end
  end

  defp table(databases, postgres_database_id, table_oid, sort_column_attnum) do
    if postgres_database_id do
      db = Sequin.Enum.find!(databases, &(&1.id == postgres_database_id))
      table = Sequin.Enum.find!(db.tables, &(&1.oid == table_oid))
      %{table | sort_column_attnum: sort_column_attnum}
    end
  end

  defp self_hosted? do
    Application.get_env(:sequin, :env) != :prod or
      Application.get_env(:sequin, :self_hosted)
  end
end
