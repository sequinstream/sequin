defmodule Sequin.Transforms do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.FilterFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Consumers.WebhookSiteGenerator
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Error.ValidationError
  alias Sequin.Health
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Sinks.Gcp

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

    result = %{
      id: database.id,
      name: database.name,
      username: database.username,
      password: maybe_obfuscate(database.password, show_sensitive),
      hostname: database.hostname,
      database: database.database,
      slot: %{
        name: database.replication_slot.slot_name
      },
      publication: %{
        name: database.replication_slot.publication_name
      },
      port: database.port,
      pool_size: database.pool_size,
      ssl: database.ssl,
      ipv6: database.ipv6,
      use_local_tunnel: database.use_local_tunnel
    }

    if is_nil(database.primary) do
      result
    else
      Map.put(result, :primary, %{
        username: database.primary.username,
        password: maybe_obfuscate(database.primary.password, show_sensitive),
        hostname: database.primary.hostname,
        port: database.primary.port,
        database: database.primary.database
      })
    end
  end

  def to_external(%HttpEndpoint{host: "webhook.site"} = http_endpoint, show_sensitive) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      "webhook.site": true,
      headers: format_headers(http_endpoint.headers),
      encrypted_headers:
        if(show_sensitive, do: format_headers(http_endpoint.encrypted_headers), else: encrypted_headers(http_endpoint))
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
      |> Repo.preload([:active_backfills, :transform, sequence: [:postgres_database]])
      |> SinkConsumer.preload_http_endpoint!()

    table =
      cond do
        is_nil(consumer.sequence) -> nil
        table = Enum.find(consumer.sequence.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid)) -> table
        true -> nil
      end

    filters =
      cond do
        is_nil(table) -> []
        is_nil(consumer.sequence_filter) -> []
        is_nil(consumer.sequence_filter.column_filters) -> []
        true -> consumer.sequence_filter.column_filters
      end

    schema =
      if is_nil(consumer.schema_filter) do
        nil
      else
        consumer.schema_filter.schema
      end

    database_name =
      if consumer.sequence do
        consumer.sequence.postgres_database.name
      else
        # For schema filters, we need to get the database from the replication slot
        consumer = Repo.preload(consumer, replication_slot: [:postgres_database])
        consumer.replication_slot.postgres_database.name
      end

    actions =
      if consumer.sequence_filter do
        consumer.sequence_filter.actions
      else
        # For schema filters, we don't have specific actions, so use default
        [:insert, :update, :delete]
      end

    %{
      id: consumer.id,
      name: consumer.name,
      database: database_name,
      status: consumer.status,
      actions: actions,
      destination: to_external(sink, show_sensitive),
      filters: Enum.map(filters, &format_filter(&1, table)),
      batch_size: consumer.batch_size,
      transform: if(consumer.transform, do: consumer.transform.name, else: "none"),
      timestamp_format: consumer.timestamp_format,
      active_backfills: Enum.map(consumer.active_backfills, &to_external(&1, show_sensitive)),
      max_retry_count: consumer.max_retry_count,
      load_shedding_policy: consumer.load_shedding_policy,
      annotations: consumer.annotations
    }
    |> Sequin.Map.put_if_present(:table, if(table, do: "#{table.schema}.#{table.name}"))
    |> Sequin.Map.put_if_present(:schema, schema)
    |> Sequin.Map.put_if_present(:group_column_names, group_column_names(consumer, table))
    |> Sequin.Map.put_if_present(:health, if(consumer.health, do: to_external(consumer.health, show_sensitive)))
  end

  def to_external(%HttpPushSink{} = sink, _show_sensitive) do
    sink = SinkConsumer.preload_http_endpoint!(sink)

    %{
      type: "webhook",
      http_endpoint: sink.http_endpoint.name,
      http_endpoint_path: sink.http_endpoint_path,
      batch: sink.batch
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

  def to_external(%RedisStreamSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "redis_stream",
      host: sink.host,
      port: sink.port,
      stream_key: sink.stream_key,
      database: sink.database,
      tls: sink.tls,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive)
    })
  end

  def to_external(%RedisStringSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "redis_string",
      host: sink.host,
      port: sink.port,
      database: sink.database,
      tls: sink.tls,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive),
      expire_ms: sink.expire_ms
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

  def to_external(%SnsSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "sns",
      topic_arn: sink.topic_arn,
      region: sink.region,
      access_key_id: maybe_obfuscate(sink.access_key_id, show_sensitive),
      secret_access_key: maybe_obfuscate(sink.secret_access_key, show_sensitive),
      is_fifo: sink.is_fifo
    })
  end

  def to_external(%KinesisSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "kinesis",
      stream_arn: sink.stream_arn,
      access_key_id: maybe_obfuscate(sink.access_key_id, show_sensitive),
      secret_access_key: maybe_obfuscate(sink.secret_access_key, show_sensitive)
    })
  end

  def to_external(%GcpPubsubSink{credentials: %Gcp.Credentials{} = credentials} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "gcp_pubsub",
      project_id: sink.project_id,
      topic_id: sink.topic_id,
      use_emulator: sink.use_emulator,
      emulator_base_url: sink.emulator_base_url,
      credentials: %{
        type: credentials.type,
        project_id: credentials.project_id,
        private_key_id: maybe_obfuscate(credentials.private_key_id, show_sensitive),
        private_key: maybe_obfuscate(credentials.private_key, show_sensitive),
        client_email: maybe_obfuscate(credentials.client_email, show_sensitive),
        client_id: maybe_obfuscate(credentials.client_id, show_sensitive),
        auth_uri: credentials.auth_uri,
        token_uri: credentials.token_uri,
        auth_provider_x509_cert_url: credentials.auth_provider_x509_cert_url,
        client_x509_cert_url: credentials.client_x509_cert_url,
        universe_domain: maybe_obfuscate(credentials.universe_domain, show_sensitive),
        client_secret: maybe_obfuscate(credentials.client_secret, show_sensitive),
        api_key: maybe_obfuscate(credentials.api_key, show_sensitive)
      }
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

  def to_external(%TypesenseSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "typesense",
      endpoint_url: sink.endpoint_url,
      collection_name: sink.collection_name,
      api_key: maybe_obfuscate(sink.api_key, show_sensitive),
      batch_size: sink.batch_size,
      timeout_seconds: sink.timeout_seconds
    })
  end

  def to_external(%ElasticsearchSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "elasticsearch",
      endpoint_url: sink.endpoint_url,
      index_name: sink.index_name,
      auth_type: sink.auth_type,
      auth_value: maybe_obfuscate(sink.auth_value, show_sensitive),
      batch_size: sink.batch_size
    })
  end

  def to_external(%Function{function: %PathFunction{}} = function, _show_sensitive) do
    %{
      name: function.name,
      description: function.description,
      type: function.type,
      path: function.function.path
    }
  end

  def to_external(%Function{function: %TransformFunction{}} = function, _show_sensitive) do
    %{
      name: function.name,
      description: function.description,
      type: function.type,
      code: function.function.code
    }
  end

  def to_external(%Function{function: %RoutingFunction{}} = function, _show_sensitive) do
    %{
      name: function.name,
      description: function.description,
      type: function.type,
      sink_type: function.function.sink_type,
      code: function.function.code
    }
  end

  def to_external(%Function{function: %FilterFunction{}} = function, _show_sensitive) do
    %{
      name: function.name,
      description: function.description,
      type: function.type,
      code: function.function.code
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

  def to_external(%Backfill{} = backfill, _show_sensitive) do
    backfill = Repo.preload(backfill, sink_consumer: [:sequence, :postgres_database])
    database = backfill.sink_consumer.postgres_database

    table = Enum.find(database.tables, &(&1.oid == backfill.table_oid))
    sort_column = if table, do: Enum.find(table.columns, &(&1.attnum == backfill.sort_column_attnum))

    %{
      id: backfill.id,
      sink_consumer: backfill.sink_consumer.name,
      state: backfill.state,
      table: if(table, do: "#{table.schema}.#{table.name}"),
      sort_column: if(sort_column, do: sort_column.name),
      rows_initial_count: backfill.rows_initial_count,
      rows_processed_count: backfill.rows_processed_count,
      rows_ingested_count: backfill.rows_ingested_count,
      completed_at: backfill.completed_at,
      canceled_at: backfill.canceled_at,
      inserted_at: backfill.inserted_at,
      updated_at: backfill.updated_at
    }
  end

  def to_external(%Health{} = health, _show_sensitive) do
    %{
      name: Health.entity_name(health.entity_kind),
      status: health.status,
      checks: Enum.map(health.checks, &to_external/1)
    }
  end

  def to_external(%Health.Check{} = check, _show_sensitive) do
    Sequin.Map.put_if_present(
      %{name: Health.Check.check_name(check), status: check.status},
      :error,
      if(check.error, do: Exception.message(check.error))
    )
  end

  def group_column_names(%SinkConsumer{}, nil), do: nil

  def group_column_names(%SinkConsumer{} = consumer, table) do
    default_group_column_names =
      table.columns
      |> Enum.filter(& &1.is_pk?)
      |> Enum.sort_by(& &1.attnum)
      |> Enum.map(& &1.name)

    cond do
      is_nil(consumer.sequence_filter) ->
        default_group_column_names

      is_nil(consumer.sequence_filter.group_column_attnums) ->
        default_group_column_names

      true ->
        Enum.map(consumer.sequence_filter.group_column_attnums, fn attnum ->
          column = Enum.find(table.columns, &(&1.attnum == attnum))
          column.name
        end)
    end
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

  defp encrypted_headers(%HttpEndpoint{encrypted_headers: nil}), do: %{}

  defp encrypted_headers(%HttpEndpoint{encrypted_headers: encrypted_headers}) do
    Map.new(encrypted_headers, fn {key, value} -> {key, maybe_obfuscate(value, false)} end)
  end

  defp maybe_obfuscate(nil, _), do: nil
  defp maybe_obfuscate(value, true), do: value
  defp maybe_obfuscate(value, false), do: Sequin.String.obfuscate(value)

  def from_external_postgres_database(params, account_id) do
    with {:ok, db_params} <- parse_db_params(params) do
      sparams =
        db_params
        |> Map.put("account_id", account_id)
        |> Sequin.Map.atomize_keys()

      {:ok, struct(PostgresDatabase, sparams)}
      # {:ok, struct(PostgresDatabase, Map.put(db_params, "account_id", account_id))}
    end
  end

  # Add this function after the to_external for PostgresDatabase
  def parse_db_params(%{"url" => url} = params) do
    uri = URI.parse(url)

    uri_params = %{
      "hostname" => uri.host,
      "port" => uri.port,
      "database" =>
        case uri.path do
          "/" <> dbname -> dbname
          dbname -> dbname
        end
    }

    uri_params = maybe_add_userinfo(uri_params, uri.userinfo)

    missing_params = for {k, nil} <- uri_params, do: k
    overlapping_url_params = ["database", "hostname", "port", "username", "password"]
    example_url = "postgresql://user:password@localhost:5432/mydb"

    cond do
      Enum.any?(overlapping_url_params, fn p -> Map.get(params, p) end) ->
        {:error,
         Error.validation(
           summary: "Bad connection details. If `url` is specified, no other connection params are allowed"
         )}

      not Enum.empty?(missing_params) ->
        {:error,
         Error.validation(
           summary:
             "Parameters missing from `url`: #{Enum.join(missing_params, ", ")}. It should look like: #{example_url}"
         )}

      not is_nil(uri.query) ->
        {:error, Error.validation(summary: "Query parameters not allowed in `url` - specify e.g. ssl with `ssl` key")}

      true ->
        params
        |> Map.delete("url")
        |> Map.merge(uri_params)
        |> parse_db_params()
    end
  end

  # General case for parsing database parameters
  def parse_db_params(db_params) when is_map(db_params) do
    # Only allow specific fields to be set by the API
    allowed_params =
      db_params
      |> Map.take([
        "database",
        "hostname",
        "name",
        "port",
        "username",
        "password",
        "ssl",
        "use_local_tunnel",
        "ipv6",
        "annotations",
        "primary"
      ])
      |> Map.put_new("port", 5432)

    with {:ok, primary} <- parse_primary_params(db_params) do
      if is_map_key(allowed_params, "primary") do
        {:ok, Map.put(allowed_params, "primary", primary)}
      else
        {:ok, allowed_params}
      end
    end
  end

  # Helper functions for database parameter parsing
  defp parse_primary_params(%{"primary" => primary}) when is_map(primary) do
    parse_db_params(primary)
  end

  defp parse_primary_params(_) do
    {:ok, nil}
  end

  defp maybe_add_userinfo(uri_params, nil), do: uri_params

  defp maybe_add_userinfo(uri_params, userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] ->
        Map.merge(uri_params, %{"username" => username, "password" => password})

      _ ->
        uri_params
    end
  end

  def from_external_http_endpoint(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case key do
        "name" ->
          {:cont, {:ok, Map.put(acc, :name, value)}}

        "headers" ->
          case parse_headers(value) do
            {:ok, headers} -> {:cont, {:ok, Map.put(acc, :headers, headers)}}
            {:error, error} -> {:halt, {:error, error}}
          end

        "encrypted_headers" ->
          case parse_headers(value) do
            {:ok, headers} ->
              {:cont, {:ok, Map.put(acc, :encrypted_headers, headers)}}

            {:error,
             %ValidationError{
               summary: "Headers appear to be encrypted. Please provide decrypted headers as a list of key-value pairs."
             }} ->
              {:cont, {:ok, acc}}

            {:error, error} ->
              {:halt, {:error, error}}
          end

        "webhook.site" ->
          if value in [true, "true"] do
            {:cont,
             {:ok,
              acc
              |> Map.put(:scheme, :https)
              |> Map.put(:host, "webhook.site")
              |> Map.put(:path, "/" <> generate_webhook_site_id())}}
          else
            {:cont, {:ok, acc}}
          end

        "local" ->
          if value == "true" do
            {:cont,
             {:ok,
              acc
              |> Map.put(:use_local_tunnel, true)
              |> Map.put(:path, attrs["path"])}}
          else
            {:cont, {:ok, acc}}
          end

        "url" ->
          uri = URI.parse(value)

          {:cont,
           {:ok,
            acc
            |> Map.put(:scheme, uri.scheme)
            |> Map.put(:host, uri.host)
            |> Map.put(:port, uri.port)
            |> Map.put(:path, uri.path)
            |> Map.put(:query, uri.query)
            |> Map.put(:fragment, uri.fragment)}}

        "path" ->
          {:cont, {:ok, Map.put(acc, :path, value)}}

        # Ignore internal fields that might be present in the external data
        "id" ->
          {:cont, {:ok, acc}}

        "inserted_at" ->
          {:cont, {:ok, acc}}

        "updated_at" ->
          {:cont, {:ok, acc}}

        "account_id" ->
          {:cont, {:ok, acc}}

        # Unknown field
        _ ->
          {:halt, {:error, Error.validation(summary: "Unknown field: #{key}")}}
      end
    end)
  end

  # Helper functions

  defp parse_headers(nil), do: {:ok, %{}}

  defp parse_headers(headers) when is_map(headers) do
    {:ok, headers}
  end

  defp parse_headers(headers) when is_list(headers) do
    {:ok, Map.new(headers, fn %{"key" => key, "value" => value} -> {key, value} end)}
  end

  @deprecated "Use parse_headers with is_map, or is_list guard"
  defp parse_headers(headers) when is_binary(headers) do
    # NOTE: This encrypted header serialization format was used previously.
    # We are keeping this for backwards compatibility, consider removing eventually.
    if Regex.match?(~r/\(\d+ encrypted header\(s\)\) - sha256sum: [a-f0-9]+/, headers) do
      {:error,
       Error.validation(
         summary: "Headers appear to be encrypted. Please provide decrypted headers as a list of key-value pairs."
       )}
    else
      {:error, Error.validation(summary: "Invalid headers configuration. Must be a list of key-value pairs.")}
    end
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
    Enum.reduce_while(consumer_attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case key do
        "name" ->
          {:cont, {:ok, Map.put(acc, :name, value)}}

        "status" ->
          {:cont, {:ok, Map.put(acc, :status, parse_status(value))}}

        "annotations" ->
          {:cont, {:ok, Map.put(acc, :annotations, value)}}

        "batch_size" ->
          case value do
            size when is_integer(size) and size > 0 and size <= 1_000 ->
              {:cont, {:ok, Map.put(acc, :batch_size, size)}}

            _ ->
              {:halt, {:error, Error.validation(summary: "batch_size must be a positive integer <= 1000")}}
          end

        "table" ->
          # Check for mutual exclusion with schema
          if consumer_attrs["schema"] do
            {:halt, {:error, Error.validation(summary: "Cannot specify both `schema` and `table`. Choose one.")}}
          else
            {schema, table_name} = parse_table_reference(value)
            databases = Repo.preload(databases, [:sequences, :replication_slot])

            with {:ok, database} <- find_database_by_name(consumer_attrs["database"], databases),
                 {:ok, sequence} <- find_sequence_by_name(database, value),
                 table = Sequin.Enum.find!(database.tables, &(&1.schema == schema && &1.name == table_name)),
                 {:ok, group_column_attnums} <- group_column_attnums(consumer_attrs["group_column_names"], table),
                 {:ok, column_filters} <- column_filters(consumer_attrs["filters"], table) do
              {:cont,
               {:ok,
                acc
                |> Map.put(:sequence_id, sequence.id)
                |> Map.put(:replication_slot_id, database.replication_slot.id)
                |> Map.put(:sequence_filter, %{
                  actions: consumer_attrs["actions"] || ["insert", "update", "delete"],
                  group_column_attnums: group_column_attnums,
                  column_filters: column_filters
                })}}
            else
              {:error, error} -> {:halt, {:error, error}}
            end
          end

        "schema" ->
          # Check for mutual exclusion with table
          cond do
            Map.has_key?(consumer_attrs, "table") ->
              {:halt, {:error, Error.validation(summary: "Cannot specify both `schema` and `table`. Choose one.")}}

            Map.has_key?(consumer_attrs, "group_column_names") ->
              {:halt, {:error, Error.validation(summary: "Cannot specify `group_column_names` with `schema`.")}}

            true ->
              databases = Repo.preload(databases, [:replication_slot])

              case find_database_by_name(consumer_attrs["database"], databases) do
                {:ok, database} ->
                  {:cont,
                   {:ok,
                    acc
                    |> Map.put(:replication_slot_id, database.replication_slot.id)
                    |> Map.put(:schema_filter, %{
                      schema: value
                    })}}

                {:error, error} ->
                  {:halt, {:error, error}}
              end
          end

        # handled in "table" or "schema"
        key when key in ~w(database filters group_column_names actions) ->
          {:cont, {:ok, acc}}

        "destination" ->
          case parse_sink(value, %{http_endpoints: http_endpoints}) do
            {:ok, sink} -> {:cont, {:ok, Map.put(acc, :sink, sink)}}
            {:error, error} -> {:halt, {:error, error}}
          end

        "transform" ->
          case parse_transform_id(account_id, value) do
            {:ok, transform_id} -> {:cont, {:ok, Map.put(acc, :transform_id, transform_id)}}
            {:error, error} -> {:halt, {:error, error}}
          end

        "routing" ->
          case parse_transform_id(account_id, value) do
            {:ok, transform_id} ->
              {:cont, {:ok, Map.merge(acc, %{routing_mode: "dynamic", routing_id: transform_id})}}

            {:error, error} ->
              {:halt, {:error, error}}
          end

        "filter" ->
          case parse_transform_id(account_id, value) do
            {:ok, transform_id} -> {:cont, {:ok, Map.put(acc, :filter_id, transform_id)}}
            {:error, error} -> {:halt, {:error, error}}
          end

        "timestamp_format" ->
          {:cont, {:ok, Map.put(acc, :timestamp_format, value)}}

        "max_retry_count" when is_integer(value) and value >= 0 ->
          {:cont, {:ok, Map.put(acc, :max_retry_count, value)}}

        "max_retry_count" when is_nil(value) ->
          {:cont, {:ok, acc}}

        "max_retry_count" ->
          {:halt, {:error, Error.validation(summary: "max_retry_count must be a non-negative integer")}}

        "load_shedding_policy" when value in ~w(pause_on_full discard_on_full) ->
          {:cont, {:ok, Map.put(acc, :load_shedding_policy, value)}}

        "load_shedding_policy" ->
          {:halt,
           {:error, Error.validation(summary: "load_shedding_policy must be one of: 'pause_on_full', 'discard_on_full'")}}

        # temporary for backwards compatibility
        "consumer_start" ->
          {:cont, {:ok, acc}}

        # Ignore until it is properly implemented
        "active_backfill" ->
          {:cont, {:ok, acc}}

        "active_backfills" ->
          {:cont, {:ok, acc}}

        # Ignore internal fields that might be present in the external data
        ignored when ignored in ~w(id inserted_at updated_at account_id replication_slot_id sequence_id) ->
          {:cont, {:ok, acc}}

        # Unknown field
        _ ->
          {:halt, {:error, Error.validation(summary: "Unknown field: #{key}")}}
      end
    end)
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
         http_endpoint_path: attrs["http_endpoint_path"],
         batch: Map.get(attrs, "batch", true)
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
       secret_access_key: attrs["secret_access_key"],
       endpoint: attrs["endpoint"],
       proto: attrs["proto"]
     }}
  end

  defp parse_sink(%{"type" => "sns"} = attrs, _resources) do
    {:ok,
     %{
       type: :sns,
       topic_arn: attrs["topic_arn"],
       region: attrs["region"],
       access_key_id: attrs["access_key_id"],
       secret_access_key: attrs["secret_access_key"]
     }}
  end

  defp parse_sink(%{"type" => "kinesis"} = attrs, _resources) do
    {:ok,
     %{
       type: :kinesis,
       stream_arn: attrs["stream_arn"],
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

  # Backwards compatibility for Redis sink -> Redis Stream sink
  defp parse_sink(%{"type" => "redis"} = attrs, resources) do
    parse_sink(%{attrs | "type" => "redis_stream"}, resources)
  end

  defp parse_sink(%{"type" => "redis_stream"} = attrs, _resources) do
    {:ok,
     %{
       type: :redis_stream,
       host: attrs["host"],
       port: attrs["port"],
       stream_key: attrs["stream_key"],
       database: attrs["database"] || 0,
       tls: attrs["tls"] || false,
       username: attrs["username"],
       password: attrs["password"]
     }}
  end

  defp parse_sink(%{"type" => "redis_string"} = attrs, _resources) do
    {:ok,
     %{
       type: :redis_string,
       host: attrs["host"],
       port: attrs["port"],
       database: attrs["database"] || 0,
       tls: attrs["tls"] || false,
       username: attrs["username"],
       password: attrs["password"],
       expire_ms: attrs["expire_ms"]
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

  # Add parse_sink for typesense type
  defp parse_sink(%{"type" => "typesense"} = attrs, _resources) do
    {:ok,
     %{
       type: :typesense,
       endpoint_url: attrs["endpoint_url"],
       collection_name: attrs["collection_name"],
       api_key: attrs["api_key"],
       batch_size: attrs["batch_size"],
       timeout_seconds: attrs["timeout_seconds"]
     }}
  end

  # Add parse_sink for elasticsearch type
  defp parse_sink(%{"type" => "elasticsearch"} = attrs, _resources) do
    {:ok,
     %{
       type: :elasticsearch,
       endpoint_url: attrs["endpoint_url"],
       index_name: attrs["index_name"],
       auth_type: parse_auth_type(attrs["auth_type"]),
       auth_value: attrs["auth_value"],
       batch_size: attrs["batch_size"]
     }}
  end

  # Helper to parse table reference into schema and name
  def parse_table_reference(table_ref) do
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

  defp column_filters(nil, _table), do: {:ok, []}

  defp column_filters(filters, table) when is_list(filters) do
    filters
    |> Enum.reduce_while({:ok, []}, fn filter, {:ok, acc} ->
      case parse_column_filter(filter, table) do
        {:ok, column_filter} -> {:cont, {:ok, [column_filter | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, filters} -> {:ok, Enum.reverse(filters)}
      {:error, error} -> {:error, error}
    end
  end

  def parse_column_filter(%{"column_name" => column_name} = filter, table) do
    case Enum.find(table.columns, &(&1.name == column_name)) do
      nil ->
        {:error, Error.validation(summary: "Column '#{column_name}' not found in table '#{table.schema}.#{table.name}'")}

      column ->
        is_jsonb = filter["field_path"] != nil
        value_type = determine_value_type(filter, column)

        {:ok,
         Sequin.Consumers.SequenceFilter.ColumnFilter.from_external(%{
           "columnAttnum" => column.attnum,
           "operator" => filter["operator"],
           "valueType" => value_type,
           "value" => filter["comparison_value"],
           "isJsonb" => is_jsonb,
           "jsonbPath" => filter["field_path"]
         })}
    end
  end

  defp parse_transform_id(_account_id, nil), do: {:ok, nil}
  defp parse_transform_id(_account_id, "none"), do: {:ok, nil}

  defp parse_transform_id(account_id, function_name) do
    case Consumers.find_function(account_id, name: function_name) do
      {:ok, function} -> {:ok, function.id}
      {:error, %NotFoundError{}} -> {:error, Error.validation(summary: "Function '#{function_name}' not found.")}
    end
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

  defp find_sequence_by_name(%PostgresDatabase{} = database, table_name) do
    {schema, table_name} = parse_table_reference(table_name)

    with %PostgresDatabaseTable{} = table <- Enum.find(database.tables, &(&1.schema == schema and &1.name == table_name)),
         {:ok, %Sequence{} = sequence} <- find_or_create_sequence(database, table) do
      {:ok, sequence}
    else
      {:error, error} -> {:error, error}
      _ -> {:error, Error.not_found(entity: :sequence, params: %{database: database.name, table: table_name})}
    end
  end

  # This is a hack while sequences are required
  # Functions should not perform any database operations
  # TODO: Excise this once sequences are removed
  defp find_or_create_sequence(%PostgresDatabase{} = database, %PostgresDatabaseTable{} = table) do
    case Enum.find(database.sequences, &(&1.table_oid == table.oid)) do
      %Sequence{} = sequence ->
        {:ok, sequence}

      nil ->
        name = "#{database.name}.#{table.schema}.#{table.name}"

        Databases.create_sequence(database.account_id, %{
          name: name,
          postgres_database_id: database.id,
          table_oid: table.oid,
          table_schema: table.schema
        })
    end
  end

  defp group_column_attnums(nil, %PostgresDatabaseTable{} = table) do
    case PostgresDatabaseTable.default_group_column_attnums(table) do
      [] ->
        {:error,
         Error.validation(
           summary:
             "[group_column_names] No defaults found for table '#{table.schema}.#{table.name}'. If this table does not have primary keys, you must specify the group_column_names."
         )}

      attnums ->
        {:ok, attnums}
    end
  end

  defp group_column_attnums(column_names, table) when is_list(column_names) do
    column_names
    |> Enum.reduce_while({:ok, []}, fn column_name, {:ok, attnums} ->
      case Enum.find(table.columns, &(&1.name == column_name)) do
        nil ->
          {:halt,
           {:error,
            Error.validation(
              summary:
                "[group_column_names] Column '#{column_name}' does not exist for table '#{table.schema}.#{table.name}'"
            )}}

        column ->
          {:cont, {:ok, [column.attnum | attnums]}}
      end
    end)
    |> case do
      {:ok, attnums} -> {:ok, attnums |> Enum.sort() |> Enum.uniq()}
      {:error, error} -> {:error, error}
    end
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
         summary:
           "[sasl_mechanism] Invalid SASL mechanism '#{invalid}'. Must be one of: plain, scram_sha_256, scram_sha_512"
       )}

  defp env do
    Application.fetch_env!(:sequin, :env)
  end

  def from_external_backfill(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case key do
        "state" ->
          {:cont, {:ok, Map.put(acc, :state, value)}}

        # Disallow unknown fields
        unknown ->
          {:halt, {:error, Error.validation(summary: "Unknown field: #{unknown}")}}
      end
    end)
  end

  # Helper to parse auth_type
  defp parse_auth_type("api_key"), do: :api_key
  defp parse_auth_type("basic"), do: :basic
  defp parse_auth_type("bearer"), do: :bearer
  defp parse_auth_type(nil), do: :api_key
end
