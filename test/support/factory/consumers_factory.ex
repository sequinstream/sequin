defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Repo

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
        StreamsFactory.insert_http_endpoint!(account_id: account_id).id
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
        status: :active
      },
      attrs
    )
  end

  def http_pull_consumer_attrs(attrs \\ []) do
    attrs
    |> http_pull_consumer()
    |> Sequin.Map.from_ecto()
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

  defp generate_value(:string), do: Faker.Lorem.sentence()
  defp generate_value(:integer), do: Factory.integer()
  defp generate_value(:float), do: Factory.float()
  defp generate_value(:boolean), do: Factory.boolean()

  # ConsumerEvent
  def consumer_event(attrs \\ []) do
    attrs = Map.new(attrs)

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
        data: %{
          "action" => Enum.random(["INSERT", "UPDATE", "DELETE"]),
          "data" => %{"column" => Faker.Lorem.word()}
        }
      },
      attrs
    )
  end

  def consumer_event_attrs(attrs \\ []) do
    attrs
    |> consumer_event()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_event!(attrs \\ []) do
    attrs = Map.new(attrs)

    {consumer_id, attrs} =
      Map.pop_lazy(attrs, :consumer_id, fn -> ConsumersFactory.insert_consumer!().id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_event_attrs()
    |> then(&ConsumerEvent.changeset(%ConsumerEvent{}, &1))
    |> Repo.insert!()
  end
end
