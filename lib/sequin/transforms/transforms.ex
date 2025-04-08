defmodule Sequin.Transforms do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.Transform
  alias Sequin.Consumers.WebhookSiteGenerator
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo

  def to_external(resource, show_sensitive \\ false)

  def to_external(%Account{} = account, _show_sensitive) do
    %{
      id: account.id,
      name: account.name
    }
  end

  def to_external(%User{} = user, show_sensitive) do
    %{
      id: user.id,
      email: user.email,
      password: maybe_obfuscate(user.password, show_sensitive)
    }
  end

  def to_external(%PostgresDatabase{} = database, show_sensitive) do
    database = Repo.preload(database, [:replication_slot, :sequences])

    %{
      id: database.id,
      name: database.name,
      username: database.username,
      password: maybe_obfuscate(database.password, show_sensitive),
      hostname: database.hostname,
      database: database.database,
      slot_name: database.replication_slot.slot_name,
      publication_name: database.replication_slot.publication_name,
      port: database.port,
      pool_size: database.pool_size,
      ssl: database.ssl,
      ipv6: database.ipv6,
      use_local_tunnel: database.use_local_tunnel,
      tables:
        database.sequences
        |> Enum.map(fn sequence ->
          table = Enum.find(database.tables, &(&1.oid == sequence.table_oid))

          if table do
            %{
              table_name: table.name,
              table_schema: table.schema,
              sort_column_name: sequence.sort_column_name
            }
          end
        end)
        |> Enum.filter(& &1)
    }
  end

  def to_external(%HttpEndpoint{host: "webhook.site"} = http_endpoint, _show_sensitive) do
    %{
      name: http_endpoint.name,
      "webhook.site": true
    }
  end

  def to_external(%HttpEndpoint{use_local_tunnel: true} = http_endpoint, show_sensitive) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      local: true,
      path: http_endpoint.path,
      headers: format_headers(http_endpoint.headers),
      encrypted_headers:
        if(show_sensitive, do: format_headers(http_endpoint.encrypted_headers), else: encrypted_headers(http_endpoint))
    }
  end

  def to_external(%HttpEndpoint{} = http_endpoint, show_sensitive) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      url:
        URI.to_string(%URI{
          scheme: to_string(http_endpoint.scheme),
          userinfo: http_endpoint.userinfo,
          host: http_endpoint.host,
          port: http_endpoint.port,
          path: http_endpoint.path,
          query: http_endpoint.query,
          fragment: http_endpoint.fragment
        }),
      headers: format_headers(http_endpoint.headers),
      encrypted_headers:
        if(show_sensitive, do: format_headers(http_endpoint.encrypted_headers), else: encrypted_headers(http_endpoint))
    }
  end

  def to_external(%SinkConsumer{sink: sink} = consumer, show_sensitive) do
    consumer =
      consumer
      |> Repo.preload([:transform, sequence: [:postgres_database]])
      |> SinkConsumer.preload_http_endpoint()

    table = Sequin.Enum.find!(consumer.sequence.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))
    filters = consumer.sequence_filter.column_filters || []

    %{
      id: consumer.id,
      name: consumer.name,
      database: consumer.sequence.postgres_database.name,
      status: consumer.status,
      group_column_names: group_column_names(consumer.sequence_filter.group_column_attnums, table),
      table: "#{table.schema}.#{table.name}",
      actions: consumer.sequence_filter.actions,
      destination: to_external(sink, show_sensitive),
      filters: Enum.map(filters, &format_filter(&1, table)),
      consumer_start: %{
        position: "beginning | end | from with value"
      },
      batch_size: consumer.batch_size,
      transform: if(consumer.transform, do: consumer.transform.name, else: "none"),
      timestamp_format: consumer.timestamp_format
    }
  end

  def to_external(%HttpPushSink{} = sink, _show_sensitive) do
    sink = SinkConsumer.preload_http_endpoint(sink)

    %{
      type: "webhook",
      http_endpoint: sink.http_endpoint.name,
      http_endpoint_path: sink.http_endpoint_path
    }
  end

  def to_external(%SequinStreamSink{}, _show_sensitive) do
    %{
      type: "sequin_stream"
    }
  end

  def to_external(%KafkaSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "kafka",
      hosts: sink.hosts,
      topic: sink.topic,
      tls: sink.tls,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive),
      sasl_mechanism: sink.sasl_mechanism
    })
  end

  def to_external(%RabbitMqSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "rabbitmq",
      host: sink.host,
      port: sink.port,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive),
      virtual_host: sink.virtual_host,
      tls: sink.tls,
      exchange: sink.exchange,
      connection_id: sink.connection_id
    })
  end

  def to_external(%RedisSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "redis",
      host: sink.host,
      port: sink.port,
      stream_key: sink.stream_key,
      database: sink.database,
      tls: sink.tls,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive)
    })
  end

  def to_external(%SqsSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "sqs",
      queue_url: sink.queue_url,
      region: sink.region,
      access_key_id: maybe_obfuscate(sink.access_key_id, show_sensitive),
      secret_access_key: maybe_obfuscate(sink.secret_access_key, show_sensitive),
      is_fifo: sink.is_fifo
    })
  end

  def to_external(%GcpPubsubSink{} = sink, _show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "gcp_pubsub",
      project_id: sink.project_id,
      topic_id: sink.topic_id,
      use_emulator: sink.use_emulator,
      emulator_base_url: sink.emulator_base_url,
      credentials: "(credentials present) - sha256sum: #{sha256sum(sink.credentials)}"
    })
  end

  def to_external(%NatsSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "nats",
      host: sink.host,
      port: sink.port,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive),
      jwt: maybe_obfuscate(sink.jwt, show_sensitive),
      nkey_seed: maybe_obfuscate(sink.nkey_seed, show_sensitive),
      tls: sink.tls
    })
  end

  def to_external(%AzureEventHubSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "azure_event_hub",
      namespace: sink.namespace,
      event_hub_name: sink.event_hub_name,
      shared_access_key_name: sink.shared_access_key_name,
      shared_access_key: maybe_obfuscate(sink.shared_access_key, show_sensitive)
    })
  end

  def to_external(%Transform{} = transform, _show_sensitive) do
    %{
      name: transform.name,
      description: transform.description,
      transform: %{
        type: transform.type,
        path: transform.transform.path
      }
    }
  end

  def to_external(%ColumnFilter{} = column_filter, _show_sensitive) do
    %{
      column_name: column_filter.column_name,
      operator: column_filter.operator,
      comparison_value: column_filter.value.value
    }
  end

  def to_external(%WalPipeline{} = wal_pipeline, _show_sensitive) do
    wal_pipeline = Repo.preload(wal_pipeline, [:source_database, :destination_database])
    [source_table] = Consumers.enrich_source_tables(wal_pipeline.source_tables, wal_pipeline.source_database)

    destination_table =
      Sequin.Enum.find!(wal_pipeline.destination_database.tables, &(&1.oid == wal_pipeline.destination_oid))

    %{
      id: wal_pipeline.id,
      name: wal_pipeline.name,
      source_database: wal_pipeline.source_database.name,
      source_table_schema: source_table.schema_name,
      source_table_name: source_table.table_name,
      destination_database: wal_pipeline.destination_database.name,
      destination_table_schema: destination_table.schema,
      destination_table_name: destination_table.name,
      filters: Enum.map(source_table.column_filters, &to_external/1),
      actions: source_table.actions
    }
  end

  def group_column_names(nil, _table), do: []

  def group_column_names(column_attnums, table) when is_list(column_attnums) do
    table.columns
    |> Enum.filter(&(&1.attnum in column_attnums))
    |> Enum.map(& &1.name)
  end

  # Helper functions

  defp format_headers(headers) when is_map(headers) do
    Map.new(headers, fn {key, value} -> {key, value} end)
  end

  defp format_filter(filter, table) do
    column = Sequin.Enum.find!(table.columns, &(&1.attnum == filter.column_attnum))

    base = %{
      column_name: column.name,
      operator: format_operator(filter.operator)
    }

    if filter.operator == :not_null do
      base
    else
      Map.put(base, :comparison_value, filter.value.value)
    end
  end

  defp format_operator(:is_null), do: "is null"
  defp format_operator(:not_null), do: "is not null"
  defp format_operator(op), do: to_string(op)

  defp encrypted_headers(%HttpEndpoint{encrypted_headers: encrypted_headers}) do
    "(#{map_size(encrypted_headers)} encrypted header(s)) - sha256sum: #{sha256sum(encrypted_headers)}"
  end

  defp sha256sum(encrypted_headers) do
    encrypted_headers
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> String.slice(0, 8)
  end

  defp maybe_obfuscate(nil, _), do: nil
  defp maybe_obfuscate(value, true), do: value
  defp maybe_obfuscate(_value, false), do: "********"

  def from_external_http_endpoint(attrs) do
    # Parse headers and encrypted headers regardless of endpoint type
    with {:ok, headers} <- parse_headers(attrs["headers"]),
         {:ok, encrypted_headers} <- parse_headers(attrs["encrypted_headers"]) do
      # Base params that are common to all endpoint types
      base_params = %{
        name: attrs["name"],
        headers: headers,
        encrypted_headers: encrypted_headers
      }

      # Determine the endpoint type and add specific params
      params =
        cond do
          # Webhook.site endpoint
          Map.get(attrs, "webhook.site") in [true, "true"] ->
            Map.merge(base_params, %{
              scheme: :https,
              host: "webhook.site",
              path: "/" <> generate_webhook_site_id()
            })

          # Local endpoint
          Map.get(attrs, "local") == "true" ->
            Map.merge(base_params, %{
              use_local_tunnel: true,
              path: attrs["path"]
            })

          # External endpoint with URL
          is_binary(attrs["url"]) ->
            uri = URI.parse(attrs["url"])

            Map.merge(base_params, %{
              scheme: uri.scheme,
              host: uri.host,
              port: uri.port,
              path: uri.path,
              query: uri.query,
              fragment: uri.fragment
            })

          # No specific endpoint type provided
          true ->
            base_params
        end

      {:ok, params}
    end
  end

  # Helper functions

  defp parse_headers(nil), do: {:ok, %{}}

  defp parse_headers(headers) when is_map(headers) do
    {:ok, headers}
  end

  defp parse_headers(headers) when is_list(headers) do
    {:ok, Map.new(headers, fn %{"key" => key, "value" => value} -> {key, value} end)}
  end

  defp parse_headers(headers) when is_binary(headers) do
    {:error, Error.validation(summary: "Invalid headers configuration. Must be a list of key-value pairs.")}
  end

  defp generate_webhook_site_id do
    if env() == :test do
      UUID.uuid4()
    else
      case WebhookSiteGenerator.generate() do
        {:ok, uuid} ->
          uuid

        {:error, reason} ->
          raise "Failed to create webhook.site endpoint: #{reason}"
      end
    end
  end

  def from_external_sink_consumer(account_id, consumer_attrs, databases, http_endpoints) do
    # Start with base params that are always included if present
    base_params = %{
      name: consumer_attrs["name"],
      status: parse_status(consumer_attrs["status"]),
      batch_size: Map.get(consumer_attrs, "batch_size", 1)
    }

    base_params = Sequin.Map.put_if_present(base_params, :timestamp_format, consumer_attrs["timestamp_format"])

    # Handle table and database related params if present
    params =
      if table_ref = consumer_attrs["table"] do
        {schema, table_name} = parse_table_reference(table_ref)
        databases = Repo.preload(databases, [:sequences, :replication_slot])

        with {:ok, database} <- find_database_by_name(consumer_attrs["database"], databases),
             {:ok, sequence} <- find_sequence_by_name(databases, consumer_attrs["database"], consumer_attrs["table"]) do
          table = Sequin.Enum.find!(database.tables, &(&1.schema == schema && &1.name == table_name))

          {:ok,
           base_params
           |> Map.put(:sequence_id, sequence.id)
           |> Map.put(:replication_slot_id, database.replication_slot.id)
           |> Map.put(:sequence_filter, %{
             actions: consumer_attrs["actions"] || ["insert", "update", "delete"],
             group_column_attnums: group_column_attnums(consumer_attrs["group_column_names"], table),
             column_filters: column_filters(consumer_attrs["filters"], table)
           })}
        end
      else
        {:ok, base_params}
      end

    # Handle sink if present
    params =
      case params do
        {:ok, current_params} ->
          if sink_attrs = consumer_attrs["destination"] do
            case parse_sink(sink_attrs, %{http_endpoints: http_endpoints}) do
              {:ok, sink} -> {:ok, Map.put(current_params, :sink, sink)}
              {:error, error} -> {:error, error}
            end
          else
            {:ok, current_params}
          end

        error ->
          error
      end

    # Handle transform if present
    case params do
      {:ok, current_params} ->
        if transform_name = consumer_attrs["transform"] do
          case parse_transform_id(account_id, transform_name) do
            {:ok, transform_id} -> {:ok, Map.put(current_params, :transform_id, transform_id)}
            {:error, error} -> {:error, error}
          end
        else
          {:ok, current_params}
        end

      error ->
        error
    end
  end

  defp parse_sink(nil, _resources), do: {:error, Error.validation(summary: "`sink` is required on sink consumers.")}

  defp parse_sink(%{"type" => "sequin_stream"}, _resources) do
    {:ok, %{type: :sequin_stream}}
  end

  defp parse_sink(%{"type" => "webhook"} = attrs, resources) do
    http_endpoints = resources.http_endpoints

    with {:ok, http_endpoint} <- find_http_endpoint_by_name(http_endpoints, attrs["http_endpoint"]) do
      {:ok,
       %{
         type: :http_push,
         http_endpoint_id: http_endpoint.id,
         http_endpoint_path: attrs["http_endpoint_path"]
       }}
    end
  end

  defp parse_sink(%{"type" => "kafka"} = attrs, _resources) do
    with {:ok, sasl_mechanism} <- parse_sasl_mechanism(attrs["sasl_mechanism"]) do
      {:ok,
       %{
         type: :kafka,
         hosts: attrs["hosts"],
         topic: attrs["topic"],
         tls: attrs["tls"] || false,
         username: attrs["username"],
         password: attrs["password"],
         sasl_mechanism: sasl_mechanism
       }}
    end
  end

  defp parse_sink(%{"type" => "sqs"} = attrs, _resources) do
    {:ok,
     %{
       type: :sqs,
       queue_url: attrs["queue_url"],
       region: attrs["region"],
       access_key_id: attrs["access_key_id"],
       secret_access_key: attrs["secret_access_key"]
     }}
  end

  defp parse_sink(%{"type" => "rabbitmq"} = attrs, _resources) do
    {:ok,
     %{
       type: :rabbitmq,
       host: attrs["host"],
       port: attrs["port"],
       username: attrs["username"],
       password: attrs["password"],
       virtual_host: attrs["virtual_host"] || "/",
       tls: attrs["tls"] || false,
       exchange: attrs["exchange"]
     }}
  end

  defp parse_sink(%{"type" => "redis"} = attrs, _resources) do
    {:ok,
     %{
       type: :redis,
       host: attrs["host"],
       port: attrs["port"],
       stream_key: attrs["stream_key"],
       database: attrs["database"] || 0,
       tls: attrs["tls"] || false,
       username: attrs["username"],
       password: attrs["password"]
     }}
  end

  defp parse_sink(%{"type" => "azure_event_hub"} = attrs, _resources) do
    {:ok,
     %{
       type: :azure_event_hub,
       namespace: attrs["namespace"],
       event_hub_name: attrs["event_hub_name"],
       shared_access_key_name: attrs["shared_access_key_name"],
       shared_access_key: attrs["shared_access_key"]
     }}
  end

  defp parse_sink(%{"type" => "nats"} = attrs, _resources) do
    {:ok,
     %{
       type: :nats,
       host: attrs["host"],
       port: attrs["port"],
       username: attrs["username"],
       password: attrs["password"],
       jwt: attrs["jwt"],
       nkey_seed: attrs["nkey_seed"],
       tls: attrs["tls"] || false
     }}
  end

  defp parse_sink(%{"type" => "gcp_pubsub"} = attrs, _resources) do
    {:ok,
     %{
       type: :gcp_pubsub,
       project_id: attrs["project_id"],
       topic_id: attrs["topic_id"],
       credentials: attrs["credentials"],
       use_emulator: attrs["use_emulator"] || false,
       emulator_base_url: attrs["emulator_base_url"]
     }}
  end

  # Helper to parse table reference into schema and name
  defp parse_table_reference(table_ref) do
    case String.split(table_ref, ".", parts: 2) do
      [table_name] -> {"public", table_name}
      [schema, table_name] -> {schema, table_name}
    end
  end

  defp find_database_by_name(name, databases) do
    case Enum.find(databases, &(&1.name == name)) do
      nil -> {:error, Error.not_found(entity: :database, params: %{name: name})}
      database -> {:ok, database}
    end
  end

  defp find_http_endpoint_by_name(http_endpoints, name) do
    case Enum.find(http_endpoints, &(&1.name == name)) do
      nil -> {:error, Error.not_found(entity: :http_endpoint, params: %{name: name})}
      http_endpoint -> {:ok, http_endpoint}
    end
  end

  defp column_filters(nil, _table), do: []

  defp column_filters(filters, table) when is_list(filters) do
    Enum.map(filters, &parse_column_filter(&1, table))
  end

  defp parse_transform_id(_account_id, nil), do: {:ok, nil}
  defp parse_transform_id(_account_id, "none"), do: {:ok, nil}

  defp parse_transform_id(account_id, transform_name) do
    case Consumers.find_transform(account_id, name: transform_name) do
      {:ok, transform} -> {:ok, transform.id}
      {:error, %NotFoundError{}} -> {:error, Error.validation(summary: "Transform '#{transform_name}' not found.")}
    end
  end

  def parse_column_filter(%{"column_name" => column_name} = filter, table) do
    # Find the column by name
    column = Enum.find(table.columns, &(&1.name == column_name))
    unless column, do: raise("Column '#{column_name}' not found in table '#{table.name}'")

    is_jsonb = filter["field_path"] != nil
    value_type = determine_value_type(filter, column)

    Sequin.Consumers.SequenceFilter.ColumnFilter.from_external(%{
      "columnAttnum" => column.attnum,
      "operator" => filter["operator"],
      "valueType" => value_type,
      "value" => filter["comparison_value"],
      "isJsonb" => is_jsonb,
      "jsonbPath" => filter["field_path"]
    })
  end

  defp determine_value_type(%{"field_type" => explicit_type}, _column) when not is_nil(explicit_type) do
    case String.downcase(explicit_type) do
      "string" -> :string
      "cistring" -> :cistring
      "number" -> :number
      "boolean" -> :boolean
      "datetime" -> :datetime
      "list" -> :list
      invalid_type -> raise "Invalid field_type: #{invalid_type}"
    end
  end

  defp determine_value_type(%{"operator" => operator}, _column) when operator in ["is null", "is not null", "not null"] do
    :null
  end

  defp determine_value_type(%{"operator" => operator}, _column) when operator in ["in", "not in"] do
    :list
  end

  defp determine_value_type(_filter, column) do
    case column.type do
      "character varying" -> :string
      "text" -> :string
      "citext" -> :cistring
      "integer" -> :number
      "bigint" -> :number
      "numeric" -> :number
      "double precision" -> :number
      "boolean" -> :boolean
      "timestamp" -> :datetime
      "timestamp without time zone" -> :datetime
      "timestamp with time zone" -> :datetime
      "jsonb" -> :string
      "json" -> :string
      "uuid" -> :string
      atom when is_atom(atom) -> determine_value_type(nil, to_string(atom))
      type -> raise "Unsupported column type: #{type}"
    end
  end

  defp find_sequence_by_name(databases, database_name, table_name) do
    {schema, table_name} = parse_table_reference(table_name)

    with %PostgresDatabase{tables: tables, sequences: sequences} <- Enum.find(databases, &(&1.name == database_name)),
         %PostgresDatabaseTable{oid: table_oid} <- Enum.find(tables, &(&1.schema == schema and &1.name == table_name)),
         %Sequence{} = sequence <- Enum.find(sequences, &(&1.table_oid == table_oid)) do
      {:ok, sequence}
    else
      _ -> {:error, Error.not_found(entity: :sequence, params: %{database: database_name, table: table_name})}
    end
  end

  defp group_column_attnums(nil, %PostgresDatabaseTable{} = table) do
    PostgresDatabaseTable.default_group_column_attnums(table)
  end

  defp group_column_attnums(column_names, table) when is_list(column_names) do
    table.columns
    |> Enum.filter(&(&1.name in column_names))
    |> Enum.map(& &1.attnum)
  end

  def parse_status(nil), do: :active
  def parse_status(status), do: status

  # Helper function to parse SASL mechanism
  defp parse_sasl_mechanism(nil), do: {:ok, nil}
  defp parse_sasl_mechanism("plain"), do: {:ok, :plain}
  defp parse_sasl_mechanism("scram_sha_256"), do: {:ok, :scram_sha_256}
  defp parse_sasl_mechanism("scram_sha_512"), do: {:ok, :scram_sha_512}

  defp parse_sasl_mechanism(invalid),
    do:
      {:error,
       Error.validation(
         summary: "Invalid SASL mechanism '#{invalid}'. Must be one of: plain, scram_sha_256, scram_sha_512"
       )}

  defp env do
    Application.fetch_env!(:sequin, :env)
  end
end
