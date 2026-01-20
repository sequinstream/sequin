defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.BenchmarkSink
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.Source
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Sinks.Gcp

  def sink_consumer_type do
    Enum.random(Consumers.SinkConsumer.types())
  end

  def sink_consumer(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    if Map.has_key?(attrs, :postgres_database) and Map.has_key?(attrs, :postgres_database_id) do
      raise ArgumentError, "Cannot specify both postgres_database and postgres_database_id"
    end

    postgres_database = Map.get(attrs, :postgres_database)

    type = attrs[:type] || get_in(attrs, [:sink, :type]) || sink_consumer_type()

    {sink_attrs, attrs} = Map.pop(attrs, :sink, %{})
    sink = sink(type, account_id, sink_attrs)

    {postgres_database_id, attrs} =
      if postgres_database do
        {postgres_database.id, attrs}
      else
        Map.pop_lazy(attrs, :postgres_database_id, fn ->
          DatabasesFactory.insert_postgres_database!(account_id: account_id).id
        end)
      end

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: postgres_database_id
        ).id
      end)

    merge_attributes(
      %SinkConsumer{
        id: Factory.uuid(),
        account_id: account_id,
        actions: [:insert, :update, :delete],
        ack_wait_ms: 30_000,
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        sink: sink,
        source: %Source{},
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        max_memory_mb: Enum.random(128..1024),
        name: Factory.unique_word(),
        replication_slot_id: replication_slot_id,
        partition_count: Enum.random(1..10),
        status: :active,
        legacy_transform: :none,
        timestamp_format: :iso8601
      },
      attrs
    )
  end

  def sink_consumer_attrs(attrs \\ []) do
    attrs
    |> sink_consumer()
    |> Map.update!(:sink, fn
      %GcpPubsubSink{} = sink ->
        sink
        |> Sequin.Map.from_ecto()
        |> Map.update!(:credentials, &Sequin.Map.from_ecto/1)

      sink ->
        Sequin.Map.from_ecto(sink)
    end)
    |> Map.update!(:source, &Sequin.Map.from_ecto/1)
    |> Sequin.Map.from_ecto()
  end

  def insert_sink_consumer!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {routing_mode, attrs} = Map.pop(attrs, :routing_mode, :static)

    attrs =
      attrs
      |> Map.put(:account_id, account_id)
      |> sink_consumer_attrs()
      |> Map.put(:routing_mode, to_string(routing_mode))

    case Consumers.create_sink_consumer(account_id, attrs, skip_lifecycle: true) do
      {:ok, consumer} ->
        Consumers.create_consumer_partition(consumer)
        consumer

      {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
        insert_sink_consumer!(attrs)
    end
  end

  defp sink(:azure_event_hub, _account_id, attrs) do
    merge_attributes(
      %AzureEventHubSink{
        type: :azure_event_hub,
        namespace: Factory.word(),
        event_hub_name: Factory.word(),
        shared_access_key_name: Factory.word(),
        shared_access_key: Factory.word(),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:http_push, account_id, attrs) do
    {http_endpoint_id, attrs} =
      Map.pop_lazy(attrs, :http_endpoint_id, fn ->
        ConsumersFactory.insert_http_endpoint!(account_id: account_id).id
      end)

    merge_attributes(
      %HttpPushSink{
        http_endpoint_id: http_endpoint_id,
        batch: true
      },
      attrs
    )
  end

  defp sink(:sqs, _account_id, attrs) do
    merge_attributes(
      %SqsSink{
        type: :sqs,
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/#{Factory.word()}",
        region: Enum.random(["us-east-1", "us-west-1", "us-west-2"]),
        access_key_id: Factory.word(),
        secret_access_key: Factory.word(),
        is_fifo: Enum.random([true, false]),
        routing_mode: :static
      },
      attrs
    )
  end

  defp sink(:meilisearch, _account_id, attrs) do
    merge_attributes(
      %MeilisearchSink{
        type: :meilisearch,
        endpoint_url: "http://127.0.0.1:7700",
        primary_key: "masterKey",
        index_name: Factory.word(),
        api_key: Factory.word(),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:kinesis, _account_id, attrs) do
    region = Enum.random(["us-east-1", "us-west-1", "us-west-2"])

    merge_attributes(
      %KinesisSink{
        type: :kinesis,
        stream_arn:
          :erlang.iolist_to_binary([
            "arn:aws:kinesis:",
            region,
            ":",
            "123456789012",
            ":stream/",
            Factory.word()
          ]),
        region: region,
        access_key_id: Factory.word(),
        secret_access_key: Factory.word(),
        routing_mode: :static
      },
      attrs
    )
  end

  defp sink(:s2, _account_id, attrs) do
    merge_attributes(
      %S2Sink{
        type: :s2,
        basin: "test-basin",
        stream: Factory.word(),
        access_token: Factory.word(),
        routing_mode: :static
      },
      attrs
    )
  end

  defp sink(:sns, _account_id, attrs) do
    merge_attributes(
      %SnsSink{
        type: :sns,
        topic_arn: "arn:aws:sns:us-east-1:123456789012:MyTopic",
        region: Enum.random(["us-east-1", "us-west-1", "us-west-2"]),
        access_key_id: Factory.word(),
        secret_access_key: Factory.word(),
        routing_mode: :static
      },
      attrs
    )
  end

  defp sink(:redis_stream, _account_id, attrs) do
    merge_attributes(
      %RedisStreamSink{
        type: :redis_stream,
        host: "localhost",
        port: 6379,
        database: 0,
        tls: false,
        stream_key: Factory.word(),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:nats, _account_id, attrs) do
    merge_attributes(
      %NatsSink{
        type: :nats,
        host: "localhost",
        port: 4222
      },
      attrs
    )
  end

  defp sink(:rabbitmq, _account_id, attrs) do
    merge_attributes(
      %RabbitMqSink{
        type: :rabbitmq,
        host: "localhost",
        port: 5672,
        exchange: "test-exchange",
        headers: %{},
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:kafka, _account_id, attrs) do
    merge_attributes(
      %KafkaSink{
        type: :kafka,
        hosts: "localhost:9092",
        topic: Factory.word(),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:sequin_stream, _account_id, attrs) do
    merge_attributes(%SequinStreamSink{type: :sequin_stream}, attrs)
  end

  defp sink(:benchmark, _account_id, attrs) do
    merge_attributes(
      %BenchmarkSink{
        type: :benchmark,
        partition_count: Map.get(attrs, :partition_count, System.schedulers_online())
      },
      attrs
    )
  end

  defp sink(:gcp_pubsub, _account_id, attrs) do
    merge_attributes(
      %GcpPubsubSink{
        type: :gcp_pubsub,
        project_id: "test-project-123",
        topic_id: "test-topic-#{Factory.word()}",
        credentials: gcp_credential(),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:elasticsearch, _account_id, attrs) do
    merge_attributes(
      %ElasticsearchSink{
        type: :elasticsearch,
        endpoint_url: "https://elasticsearch.example.com",
        index_name: "test-index-#{Factory.word()}",
        auth_type: Enum.random([:api_key, :basic, :bearer]),
        auth_value: Factory.word(),
        batch_size: Enum.random(50..500),
        routing_mode: "static"
      },
      attrs
    )
  end

  defp sink(:redis_string, _account_id, attrs) do
    merge_attributes(
      %RedisStringSink{
        type: :redis_string,
        host: "redis-string.example.com",
        port: 6379,
        database: 0,
        tls: false,
        username: Factory.word(),
        password: Factory.word(),
        expire_ms: Enum.random([nil, 1000, 10_000, 100_000])
      },
      attrs
    )
  end

  defp sink(:typesense, _account_id, attrs) do
    merge_attributes(
      %TypesenseSink{
        type: :typesense,
        endpoint_url: "https://localhost:8108",
        collection_name: Factory.word(),
        api_key: Factory.word(),
        batch_size: Enum.random(50..500),
        timeout_seconds: Enum.random(1..30),
        routing_mode: "static"
      },
      attrs
    )
  end

  def gcp_credential(attrs \\ []) do
    merge_attributes(
      %Gcp.Credentials{
        type: "service_account",
        project_id: "test-project-123",
        private_key_id: Factory.uuid(),
        private_key: Factory.rsa_key(),
        client_email: "#{Factory.unique_word()}@test-project-123.iam.gserviceaccount.com",
        client_id: Factory.uuid(),
        auth_uri: "https://accounts.google.com/o/oauth2/auth",
        token_uri: "https://oauth2.googleapis.com/token",
        auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
        client_x509_cert_url:
          "https://www.googleapis.com/robot/v1/metadata/x509/test@test-project-123.iam.gserviceaccount.com",
        universe_domain: "googleapis.com"
      },
      attrs
    )
  end

  def gcp_credential_attrs(attrs \\ []) do
    Sequin.Map.from_ecto(gcp_credential(attrs))
  end

  def source(attrs \\ []) do
    merge_attributes(
      %Source{},
      attrs
    )
  end

  def source_attrs(attrs \\ []) do
    attrs
    |> source()
    |> Sequin.Map.from_ecto(keep_nils: true)
  end

  def source_table(attrs \\ []) do
    merge_attributes(
      %Sequin.Consumers.SourceTable{},
      attrs
    )
  end

  def source_table_attrs(attrs \\ []) do
    attrs
    |> source_table()
    |> Sequin.Map.from_ecto(keep_nils: true)
  end

  # HttpEndpoint

  def http_endpoint(attrs \\ []) do
    merge_attributes(
      %HttpEndpoint{
        name: Factory.unique_word(),
        scheme: :https,
        host: "#{Factory.word()}.com",
        path: "/#{Factory.word()}",
        headers: %{"Content-Type" => "application/json"},
        encrypted_headers: %{"Authorization" => "Bearer #{Factory.word()}"},
        account_id: Factory.uuid()
      },
      attrs
    )
  end

  def http_endpoint_attrs(attrs \\ []) do
    attrs
    |> http_endpoint()
    |> Sequin.Map.from_ecto()
  end

  def insert_http_endpoint!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> http_endpoint_attrs()
    |> then(&HttpEndpoint.create_changeset(%HttpEndpoint{account_id: account_id}, &1))
    |> Repo.insert!()
  end

  # ConsumerEvent
  def consumer_event(attrs \\ []) do
    attrs = Map.new(attrs)

    {action, attrs} = Map.pop_lazy(attrs, :action, fn -> Enum.random([:insert, :update, :delete]) end)

    state = Map.get_lazy(attrs, :state, fn -> Enum.random([:available, :delivered]) end)
    not_visible_until = if state == :available, do: nil, else: Factory.timestamp()

    {record_pks, attrs} = Map.pop_lazy(attrs, :record_pks, fn -> [Faker.UUID.v4()] end)
    record_pks = Enum.map(record_pks, &to_string/1)

    merge_attributes(
      %ConsumerEvent{
        ack_id: Factory.uuid(),
        commit_lsn: Factory.unique_integer(),
        commit_idx: Enum.random(0..100),
        consumer_id: Factory.uuid(),
        data: consumer_event_data(action: action),
        deliver_count: Enum.random(0..10),
        group_id: Factory.unique_word(),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: not_visible_until,
        record_pks: record_pks,
        replication_message_trace_id: Factory.uuid(),
        payload_size_bytes: Enum.random(1..1000),
        state: state,
        ingested_at: Factory.timestamp(),
        table_oid: Enum.random(1..100_000)
      },
      attrs
    )
  end

  def consumer_event_data(attrs \\ []) do
    attrs = Map.new(attrs)
    {action, attrs} = Map.pop_lazy(attrs, :action, fn -> Enum.random([:insert, :update, :delete]) end)

    record = %{"column" => Factory.word()}
    changes = if action == :update, do: %{"column" => Factory.word()}

    merge_attributes(
      %ConsumerEventData{
        record: record,
        changes: changes,
        action: action,
        metadata: %ConsumerEventData.Metadata{
          database_name: Factory.postgres_object(),
          table_schema: Factory.postgres_object(),
          table_name: Factory.postgres_object(),
          commit_timestamp: Factory.timestamp(),
          commit_lsn: Factory.unique_integer(),
          commit_idx: Factory.unique_integer(),
          idempotency_key: Factory.uuid(),
          record_pks: [Factory.uuid()],
          consumer: %ConsumerEventData.Metadata.Sink{
            id: Factory.uuid(),
            name: Factory.word(),
            annotations: %{
              "test" => true
            }
          },
          database: %ConsumerEventData.Metadata.Database{
            id: Factory.uuid(),
            name: Factory.word(),
            database: Factory.postgres_object(),
            hostname: Factory.hostname(),
            annotations: %{}
          }
        }
      },
      attrs
    )
  end

  def consumer_event_data_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_event_data()
    |> Sequin.Map.from_ecto(keep_nils: true)
    |> Map.update!(:metadata, fn metadata ->
      metadata
      |> Sequin.Map.from_ecto()
      |> Map.update!(:consumer, fn consumer ->
        Sequin.Map.from_ecto(consumer)
      end)
      |> Map.update!(:database, fn database ->
        Sequin.Map.from_ecto(database)
      end)
    end)
  end

  def consumer_event_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_event()
    |> Map.update!(:data, fn
      data when is_struct(data) ->
        data
        |> Map.from_struct()
        |> consumer_event_data_attrs()

      data when is_map(data) ->
        consumer_event_data_attrs(data)
    end)
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_event!(attrs \\ []) do
    attrs = Map.new(attrs)

    {consumer_id, attrs} =
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_sink_consumer!().id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_event_attrs()
    |> then(&ConsumerEvent.create_changeset(%ConsumerEvent{}, &1))
    |> Repo.insert!()
  end

  def deliverable_consumer_message(attrs \\ []) do
    deliverable_consumer_event(attrs)
  end

  def deliverable_consumer_event(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.merge(%{state: :available, not_visible_until: nil})
    |> consumer_event()
  end

  def insert_deliverable_consumer_event!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.merge(%{state: :available, not_visible_until: nil})
    |> insert_consumer_event!()
  end

  def insert_consumer_message!(attrs \\ []) do
    insert_consumer_event!(attrs)
  end

  def insert_deliverable_consumer_message!(attrs \\ []) do
    insert_deliverable_consumer_event!(attrs)
  end

  def consumer_message(attrs \\ []) do
    consumer_event(attrs)
  end

  def consumer_message_data(attrs \\ []) do
    consumer_event_data(attrs)
  end

  def consumer_message_data_attrs(attrs \\ []) do
    consumer_event_data_attrs(attrs)
  end

  def backfill(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {sink_consumer_id, attrs} =
      Map.pop_lazy(attrs, :sink_consumer_id, fn ->
        insert_sink_consumer!(account_id: account_id).id
      end)

    {initial_min_cursor, attrs} =
      Map.pop_lazy(attrs, :initial_min_cursor, fn ->
        %{Factory.unique_integer() => Factory.timestamp()}
      end)

    merge_attributes(
      %Backfill{
        id: Factory.uuid(),
        account_id: account_id,
        sink_consumer_id: sink_consumer_id,
        initial_min_cursor: initial_min_cursor,
        state: Enum.random([:active, :paused, :completed, :cancelled]),
        rows_initial_count: Enum.random(1..1000),
        rows_processed_count: 0,
        rows_ingested_count: 0,
        completed_at: nil,
        canceled_at: nil,
        table_oid: Factory.unique_integer()
      },
      attrs
    )
  end

  def backfill_attrs(attrs \\ []) do
    attrs
    |> backfill()
    |> Sequin.Map.from_ecto()
  end

  def insert_backfill!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs =
      attrs
      |> Map.put(:account_id, account_id)
      |> backfill_attrs()

    case Consumers.create_backfill(attrs, skip_lifecycle: true) do
      {:ok, backfill} ->
        backfill

      {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
        insert_backfill!(attrs)
    end
  end

  def insert_active_backfill!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:state, :active)
    |> insert_backfill!()
  end

  def insert_completed_backfill!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:state, :completed)
    |> Map.put(:completed_at, DateTime.utc_now())
    |> insert_backfill!()
  end

  def insert_cancelled_backfill!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:state, :cancelled)
    |> Map.put(:canceled_at, DateTime.utc_now())
    |> insert_backfill!()
  end

  # Function
  def transform(attrs \\ []) do
    attrs = Map.new(attrs)

    {function_type, attrs} = Map.pop_lazy(attrs, :function_type, fn -> :path end)

    function_attrs =
      case function_type do
        :path -> path_transform()
      end

    merge_attributes(
      %Function{
        id: Factory.uuid(),
        account_id: Factory.uuid(),
        name: Factory.unique_word(),
        type: to_string(function_type),
        function: function_attrs
      },
      attrs
    )
  end

  # PathFunction
  def path_transform(attrs \\ []) do
    valid_paths = [
      "record",
      "changes",
      "action",
      "metadata",
      "record.id",
      "changes.name",
      "metadata.table_schema",
      "metadata.table_name",
      "metadata.commit_timestamp",
      "metadata.commit_lsn",
      "metadata.transaction_annotations",
      "metadata.sink",
      "metadata.transaction_annotations.user_id",
      "metadata.sink.id",
      "metadata.sink.name"
    ]

    merge_attributes(
      %Sequin.Consumers.PathFunction{
        type: :path,
        path: Enum.random(valid_paths)
      },
      attrs
    )
  end

  def path_transform_attrs(attrs \\ []) do
    attrs
    |> path_transform()
    |> Sequin.Map.from_ecto()
  end
end
