defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
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
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Sinks.Gcp
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.TestEventLog

  def sink_consumer(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    type =
      attrs[:type] || get_in(attrs, [:sink, :type]) ||
        Enum.random([:http_push, :redis, :sqs, :kafka, :sequin_stream, :gcp_pubsub, :nats, :rabbitmq])

    {sink_attrs, attrs} = Map.pop(attrs, :sink, %{})
    sink = sink(type, account_id, sink_attrs)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: postgres_database_id
        ).id
      end)

    {source_tables, attrs} =
      Map.pop_lazy(attrs, :source_tables, fn -> [source_table()] end)

    {sequence_id, attrs} =
      Map.pop_lazy(attrs, :sequence_id, fn ->
        DatabasesFactory.insert_sequence!(account_id: account_id, postgres_database_id: postgres_database_id).id
      end)

    {sequence_filter, attrs} =
      Map.pop_lazy(attrs, :sequence_filter, fn ->
        if sequence_id, do: sequence_filter_attrs()
      end)

    {message_kind, attrs} = Map.pop_lazy(attrs, :message_kind, fn -> Enum.random([:event, :record]) end)

    merge_attributes(
      %SinkConsumer{
        id: Factory.uuid(),
        account_id: account_id,
        ack_wait_ms: 30_000,
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        sink: sink,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        message_kind: message_kind,
        name: Factory.unique_word(),
        replication_slot_id: replication_slot_id,
        source_tables: source_tables,
        status: :active,
        sequence_id: sequence_id,
        sequence_filter: sequence_filter
      },
      attrs
    )
  end

  def sink_consumer_attrs(attrs \\ []) do
    attrs
    |> sink_consumer()
    |> Sequin.Map.from_ecto()
    |> Map.update!(:sink, &Sequin.Map.from_ecto/1)
    |> Map.update!(:source_tables, fn source_tables ->
      Enum.map(source_tables, fn source_table ->
        source_table
        |> Sequin.Map.from_ecto()
        |> Map.update!(:column_filters, fn column_filters ->
          Enum.map(column_filters, &Sequin.Map.from_ecto/1)
        end)
      end)
    end)
  end

  def insert_sink_consumer!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs =
      attrs
      |> Map.put(:account_id, account_id)
      |> sink_consumer_attrs()

    case Consumers.create_sink_consumer(account_id, attrs, skip_lifecycle: true) do
      {:ok, consumer} ->
        Consumers.create_consumer_partition(consumer)
        consumer

      {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
        insert_sink_consumer!(attrs)
    end
  end

  defp sink(:http_push, account_id, attrs) do
    {http_endpoint_id, attrs} =
      Map.pop_lazy(attrs, :http_endpoint_id, fn ->
        ConsumersFactory.insert_http_endpoint!(account_id: account_id).id
      end)

    merge_attributes(
      %HttpPushSink{
        http_endpoint_id: http_endpoint_id
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
        is_fifo: Enum.random([true, false])
      },
      attrs
    )
  end

  defp sink(:redis, _account_id, attrs) do
    merge_attributes(
      %RedisSink{
        type: :redis,
        host: "localhost",
        port: 6379,
        database: 0,
        tls: false,
        stream_key: Factory.word()
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
        exchange: "test-exchange"
      },
      attrs
    )
  end

  defp sink(:kafka, _account_id, attrs) do
    merge_attributes(
      %KafkaSink{
        type: :kafka,
        hosts: "localhost:9092",
        topic: Factory.word()
      },
      attrs
    )
  end

  defp sink(:sequin_stream, _account_id, attrs) do
    merge_attributes(%SequinStreamSink{type: :sequin_stream}, attrs)
  end

  defp sink(:gcp_pubsub, _account_id, attrs) do
    merge_attributes(
      %GcpPubsubSink{
        type: :gcp_pubsub,
        project_id: "test-project-123",
        topic_id: "test-topic-#{Factory.word()}",
        credentials: gcp_credential_attrs()
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

  def source_table(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequin.Consumers.SourceTable{
        oid: Factory.unique_integer(),
        actions: [:insert, :update, :delete],
        column_filters: [column_filter()],
        group_column_attnums: nil,
        sort_column_attnum: Factory.unique_integer()
      },
      attrs
    )
  end

  def source_table_attrs(attrs \\ []) do
    attrs
    |> source_table()
    |> Sequin.Map.from_ecto()
  end

  def column_filter(attrs \\ []) do
    attrs = Map.new(attrs)

    value_type =
      Map.get(
        attrs,
        :value_type,
        Enum.random([
          :string,
          :cistring,
          :number,
          :boolean,
          :null,
          :list
        ])
      )

    merge_attributes(
      %ColumnFilter{
        column_attnum: Factory.unique_integer(),
        operator: generate_operator(value_type),
        value: %{__type__: value_type, value: generate_value(value_type)}
      },
      Map.delete(attrs, :value_type)
    )
  end

  def column_filter_attrs(attrs \\ []) do
    attrs
    |> column_filter()
    |> Sequin.Map.from_ecto()
  end

  defp generate_value(:string), do: Faker.Lorem.sentence()
  defp generate_value(:cistring), do: Faker.Internet.email()
  defp generate_value(:number), do: Enum.random([Factory.integer(), Factory.float()])
  defp generate_value(:boolean), do: Factory.boolean()
  defp generate_value(:null), do: nil
  defp generate_value(:list), do: Enum.map(1..3, fn _ -> Factory.word() end)

  defp generate_operator(:null), do: Factory.one_of([:is_null, :not_null])
  defp generate_operator(:list), do: Factory.one_of([:in, :not_in])
  defp generate_operator(:boolean), do: Factory.one_of([:==, :!=])
  defp generate_operator(_), do: Factory.one_of([:==, :!=, :>, :<, :>=, :<=])
  # HttpEndpoint

  def http_endpoint(attrs \\ []) do
    merge_attributes(
      %HttpEndpoint{
        name: "Test-Endpoint",
        scheme: :https,
        host: "example.com",
        path: "/webhook",
        headers: %{"Content-Type" => "application/json"},
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
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        seq: Factory.unique_integer(),
        record_pks: record_pks,
        group_id: Factory.unique_word(),
        table_oid: Enum.random(1..100_000),
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: not_visible_until,
        state: state,
        data: consumer_event_data(action: action),
        replication_message_trace_id: Factory.uuid()
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
          consumer: %{}
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
      Sequin.Map.from_ecto(metadata)
    end)
  end

  def consumer_event_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_event()
    |> Map.update!(:data, fn data ->
      data |> Map.from_struct() |> consumer_event_data_attrs()
    end)
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_event!(attrs \\ []) do
    attrs = Map.new(attrs)

    {consumer_id, attrs} =
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_sink_consumer!(message_kind: :event).id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_event_attrs()
    |> then(&ConsumerEvent.create_changeset(%ConsumerEvent{}, &1))
    |> Repo.insert!()
  end

  # ConsumerRecord
  def consumer_record(attrs \\ []) do
    attrs = Map.new(attrs)

    state = Map.get_lazy(attrs, :state, fn -> Enum.random([:available, :acked, :delivered, :pending_redelivery]) end)
    not_visible_until = if state == :available, do: nil, else: Factory.timestamp()

    {record_pks, attrs} = Map.pop_lazy(attrs, :record_pks, fn -> [Faker.UUID.v4()] end)
    record_pks = Enum.map(record_pks, &to_string/1)

    merge_attributes(
      %ConsumerRecord{
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        seq: Factory.unique_integer(),
        record_pks: record_pks,
        group_id: Enum.join(record_pks, ","),
        table_oid: Enum.random(1..100_000),
        state: state,
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: not_visible_until,
        data: consumer_record_data(),
        replication_message_trace_id: Factory.uuid()
      },
      attrs
    )
  end

  def consumer_record_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_record()
    |> Map.update!(:data, fn data ->
      data |> Map.from_struct() |> consumer_record_data_attrs()
    end)
    |> Sequin.Map.from_ecto()
  end

  def insert_deliverable_consumer_record!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.merge(%{state: :available, not_visible_until: nil})
    |> insert_consumer_record!()
  end

  def insert_consumer_record!(attrs \\ []) do
    attrs = Map.new(attrs)

    {source_record, attrs} = Map.pop(attrs, :source_record)

    attrs =
      case source_record do
        %Character{} = character ->
          Map.merge(attrs, %{record_pks: Character.record_pks(character), table_oid: Character.table_oid()})

        %CharacterDetailed{} = character_detailed ->
          Map.merge(attrs, %{
            record_pks: CharacterDetailed.record_pks(character_detailed),
            table_oid: CharacterDetailed.table_oid()
          })

        %TestEventLog{} = event_log ->
          Map.merge(attrs, %{
            record_pks: TestEventLog.record_pks(event_log),
            table_oid: TestEventLog.table_oid()
          })

        # Feel free to add more source record types here
        # Or, accept a struct instead of an atom
        :character ->
          character = CharacterFactory.insert_character!(%{}, repo: Sequin.Repo)
          Map.merge(attrs, %{record_pks: Character.record_pks(character), table_oid: Character.table_oid()})

        :character_detailed ->
          character_detailed = CharacterFactory.insert_character_detailed!(%{}, repo: Sequin.Repo)

          Map.merge(attrs, %{
            record_pks: CharacterDetailed.record_pks(character_detailed),
            table_oid: CharacterDetailed.table_oid()
          })

        nil ->
          attrs
      end

    {consumer_id, attrs} =
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_sink_consumer!(message_kind: :record).id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_record_attrs()
    |> then(&ConsumerRecord.create_changeset(%ConsumerRecord{}, &1))
    |> Repo.insert!()
  end

  # ConsumerRecordData
  def consumer_record_data(attrs \\ []) do
    merge_attributes(
      %ConsumerRecordData{
        record: %{"column" => Factory.word()},
        metadata: %ConsumerRecordData.Metadata{
          database_name: Factory.postgres_object(),
          table_schema: Factory.postgres_object(),
          table_name: Factory.postgres_object(),
          commit_timestamp: Factory.timestamp(),
          consumer: %{}
        }
      },
      attrs
    )
  end

  def consumer_record_data_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_record_data()
    |> Sequin.Map.from_ecto(keep_nils: true)
    |> Map.update!(:metadata, fn metadata ->
      Sequin.Map.from_ecto(metadata)
    end)
  end

  def sequence_filter(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %SequenceFilter{
        actions: [:insert, :update, :delete],
        column_filters: [sequence_filter_column_filter()],
        group_column_attnums: [Enum.random([1, 2, 3])]
      },
      attrs
    )
  end

  def sequence_filter_attrs(attrs \\ []) do
    attrs
    |> sequence_filter()
    |> Sequin.Map.from_ecto(keep_nils: true)
    |> Map.update!(:column_filters, fn column_filters ->
      Enum.map(column_filters, &Sequin.Map.from_ecto/1)
    end)
  end

  def sequence_filter_column_filter(attrs \\ []) do
    attrs = Map.new(attrs)

    value_type = Map.get(attrs, :value_type, Enum.random([:string, :number, :boolean, :null, :list]))

    merge_attributes(
      %ColumnFilter{
        column_attnum: Factory.unique_integer(),
        operator: generate_operator(value_type),
        value: %{__type__: value_type, value: generate_value(value_type)},
        is_jsonb: false,
        jsonb_path: nil
      },
      Map.delete(attrs, :value_type)
    )
  end

  def sequence_filter_column_filter_attrs(attrs \\ []) do
    attrs
    |> sequence_filter_column_filter()
    |> Sequin.Map.from_ecto()
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
        state: Enum.random([:active, :completed, :cancelled]),
        rows_initial_count: Enum.random(1..1000),
        rows_processed_count: 0,
        rows_ingested_count: 0,
        completed_at: nil,
        canceled_at: nil
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
end
