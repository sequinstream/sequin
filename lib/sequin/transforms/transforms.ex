defmodule Sequin.Transforms do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Databases.PostgresDatabase
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

  def to_external(%SinkConsumer{sink: sink} = consumer, _show_sensitive) do
    consumer =
      consumer
      |> Repo.preload(sequence: [:postgres_database])
      |> SinkConsumer.preload_http_endpoint()

    table = Sequin.Enum.find!(consumer.sequence.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))
    filters = consumer.sequence_filter.column_filters || []

    %{
      name: consumer.name,
      database: consumer.sequence.postgres_database.name,
      status: consumer.status,
      max_deliver: consumer.max_deliver,
      group_column_names: group_column_names(consumer.sequence_filter.group_column_attnums, table),
      table: "#{table.schema}.#{table.name}",
      destination: to_external(sink),
      filters: Enum.map(filters, &format_filter(&1, table)),
      consumer_start: %{
        position: "beginning | end | from with value"
      }
    }
  end

  def to_external(%HttpPushSink{} = sink, _show_sensitive) do
    %{
      type: "webhook",
      http_endpoint: sink.http_endpoint.name
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

  def to_external(%GcpPubsubSink{} = sink, _show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "gcp_pubsub",
      project_id: sink.project_id,
      topic_id: sink.topic_id,
      use_emulator: sink.use_emulator,
      connection_url: sink.connection_url,
      credentials: "(credentials present) - sha256sum: #{sha256sum(sink.credentials)}"
    })
  end

  def to_external(%NatsSink{} = sink, show_sensitive) do
    Sequin.Map.reject_nil_values(%{
      type: "nats",
      host: sink.host,
      port: sink.port,
      username: sink.username,
      password: maybe_obfuscate(sink.password, show_sensitive)
    })
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
end
