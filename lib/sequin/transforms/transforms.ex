defmodule Sequin.Transforms do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo

  def to_external(%Account{} = account) do
    %{
      id: account.id,
      name: account.name
    }
  end

  def to_external(%User{} = user) do
    %{
      id: user.id,
      email: user.email,
      password: "********"
    }
  end

  def to_external(%PostgresDatabase{} = database) do
    database = Repo.preload(database, [:replication_slot])

    %{
      id: database.id,
      name: database.name,
      username: database.username,
      password: "********",
      hostname: database.hostname,
      database: database.database,
      slot_name: database.replication_slot.slot_name,
      publication_name: database.replication_slot.publication_name,
      port: database.port,
      pool_size: database.pool_size,
      ssl: database.ssl,
      ipv6: database.ipv6,
      use_local_tunnel: database.use_local_tunnel
    }
  end

  def to_external(%ColumnFilter{} = column_filter) do
    %{
      column_name: column_filter.column_name,
      operator: column_filter.operator,
      comparison_value: column_filter.value.value
    }
  end

  def to_external(%Sequence{} = sequence) do
    sequence = Repo.preload(sequence, [:postgres_database])

    %{
      id: sequence.id,
      name: sequence.name,
      table_schema: sequence.table_schema,
      table_name: sequence.table_name,
      sort_column_name: sequence.sort_column_name,
      database: sequence.postgres_database.name
    }
  end

  def to_external(%HttpEndpoint{host: "webhook.site"} = http_endpoint) do
    %{
      name: http_endpoint.name,
      "webhook.site": true
    }
  end

  def to_external(%HttpEndpoint{use_local_tunnel: true} = http_endpoint) do
    %{
      name: http_endpoint.name,
      local: true,
      path: http_endpoint.path,
      headers: http_endpoint.headers,
      encrypted_headers: encrypted_headers(http_endpoint)
    }
  end

  def to_external(%HttpEndpoint{} = http_endpoint) do
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
      headers: http_endpoint.headers,
      encrypted_headers: encrypted_headers(http_endpoint)
    }
  end

  def to_external(%WalPipeline{} = wal_pipeline) do
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

  def to_external(%SinkConsumer{type: :http_push} = consumer) do
    consumer =
      consumer
      |> Repo.preload(sequence: [:postgres_database])
      |> SinkConsumer.preload_http_endpoint()

    table = Sequin.Enum.find!(consumer.sequence.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))

    filters = consumer.sequence_filter.column_filters || []

    filters =
      Enum.map(filters, fn %ColumnFilter{} = filter ->
        column = Sequin.Enum.find!(table.columns, &(&1.attnum == filter.column_attnum))
        %ColumnFilter{filter | column_name: column.name}
      end)

    %{
      name: consumer.name,
      http_endpoint: consumer.sink.http_endpoint.name,
      sequence: consumer.sequence.name,
      status: consumer.status,
      max_deliver: consumer.max_deliver,
      consumer_start: %{
        position: "beginning | end | from with value"
      },
      group_column_attnums: consumer.sequence_filter.group_column_attnums || [],
      filters: Enum.map(filters, &to_external/1)
    }
  end

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
end
