defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Streams.HttpEndpoint

  # Consumer
  def consumer(attrs \\ []) do
    case Enum.random([:http_pull, :http_push]) do
      :http_pull -> http_pull_consumer(attrs)
      :http_push -> http_push_consumer(attrs)
    end
  end

  def consumer_attrs(attrs \\ []) do
    case Enum.random([:http_pull, :http_push]) do
      :http_pull -> http_pull_consumer_attrs(attrs)
      :http_push -> http_push_consumer_attrs(attrs)
    end
  end

  def insert_consumer!(attrs \\ []) do
    case Enum.random([:http_pull, :http_push]) do
      :http_pull -> insert_http_pull_consumer!(attrs)
      :http_push -> insert_http_push_consumer!(attrs)
    end
  end

  def http_push_consumer(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {http_endpoint_id, attrs} =
      Map.pop_lazy(attrs, :http_endpoint_id, fn ->
        ConsumersFactory.insert_http_endpoint!(account_id: account_id).id
      end)

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(account_id: account_id).id
      end)

    {source_tables, attrs} =
      Map.pop_lazy(attrs, :source_tables, fn -> [source_table()] end)

    merge_attributes(
      %HttpPushConsumer{
        account_id: account_id,
        ack_wait_ms: 30_000,
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        http_endpoint_id: http_endpoint_id,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        message_kind: Factory.one_of([:event, :record]),
        name: Factory.unique_word(),
        replication_slot_id: replication_slot_id,
        source_tables: source_tables,
        status: :active
      },
      attrs
    )
  end

  def http_push_consumer_attrs(attrs \\ []) do
    attrs
    |> http_push_consumer()
    |> Sequin.Map.from_ecto()
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

  def insert_http_push_consumer!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs =
      attrs
      |> Map.put(:account_id, account_id)
      |> http_push_consumer_attrs()

    {:ok, consumer} =
      Consumers.create_http_push_consumer_for_account_with_lifecycle(account_id, attrs, no_backfill: true)

    consumer
  end

  def http_pull_consumer(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(account_id: account_id).id
      end)

    {source_tables, attrs} =
      Map.pop_lazy(attrs, :source_tables, fn -> [source_table()] end)

    merge_attributes(
      %HttpPullConsumer{
        name: Factory.unique_word(),
        message_kind: Factory.one_of([:event, :record]),
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        ack_wait_ms: 30_000,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        account_id: account_id,
        replication_slot_id: replication_slot_id,
        source_tables: source_tables,
        status: :active
      },
      attrs
    )
  end

  def http_pull_consumer_attrs(attrs \\ []) do
    attrs
    |> http_pull_consumer()
    |> Sequin.Map.from_ecto()
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

  def insert_http_pull_consumer!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs =
      attrs
      |> Map.put(:account_id, account_id)
      |> http_pull_consumer_attrs()

    {:ok, consumer} =
      Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs, no_backfill: true)

    consumer
  end

  def source_table(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequin.Consumers.SourceTable{
        oid: Factory.unique_integer(),
        actions: [:insert, :update, :delete],
        column_filters: [column_filter()]
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

    value_type = Map.get(attrs, :value_type, Enum.random([:string, :integer, :float, :boolean]))
    value = generate_value(value_type)

    merge_attributes(
      %ColumnFilter{
        column_attnum: Factory.unique_integer(),
        operator: Factory.one_of([:==, :!=, :>, :>=, :<, :<=]),
        value: %{__type__: value_type, value: value}
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
  defp generate_value(:integer), do: Factory.integer()
  defp generate_value(:float), do: Factory.float()
  defp generate_value(:boolean), do: Factory.boolean()

  # HttpEndpoint

  def http_endpoint(attrs \\ []) do
    merge_attributes(
      %HttpEndpoint{
        name: "Test Endpoint",
        base_url: "https://example.com/webhook",
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
    |> then(&HttpEndpoint.changeset(%HttpEndpoint{}, &1))
    |> Repo.insert!()
  end

  # ConsumerEvent
  def consumer_event(attrs \\ []) do
    attrs = Map.new(attrs)

    {action, attrs} = Map.pop_lazy(attrs, :action, fn -> Enum.random([:insert, :update, :delete]) end)

    merge_attributes(
      %ConsumerEvent{
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        record_pks: [Faker.UUID.v4()],
        table_oid: Enum.random(1..100_000),
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: Enum.random([nil, Factory.timestamp()]),
        data: consumer_event_data(action: action)
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
        metadata: %{
          table_schema: Factory.postgres_object(),
          table_name: Factory.postgres_object(),
          commit_timestamp: Factory.timestamp()
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
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_consumer!(message_kind: :event).id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_event_attrs()
    |> then(&ConsumerEvent.changeset(%ConsumerEvent{}, &1))
    |> Repo.insert!()
  end

  # ConsumerRecord
  def consumer_record(attrs \\ []) do
    attrs = Map.new(attrs)

    state = Map.get_lazy(attrs, :state, fn -> Enum.random([:available, :acked, :delivered, :pending_redelivery]) end)
    not_visible_until = if state == :available, do: nil, else: Factory.timestamp()

    merge_attributes(
      %ConsumerRecord{
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        record_pks: [Faker.UUID.v4()],
        table_oid: Enum.random(1..100_000),
        state: state,
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: not_visible_until
      },
      attrs
    )
  end

  def consumer_record_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> consumer_record()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_record!(attrs \\ []) do
    attrs = Map.new(attrs)

    {consumer_id, attrs} =
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_consumer!(message_kind: :record).id end)

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
        metadata: %{
          table_schema: Factory.postgres_object(),
          table_name: Factory.postgres_object(),
          commit_timestamp: Factory.timestamp()
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
  end
end
