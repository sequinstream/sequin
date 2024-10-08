defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.RecordConsumerState
  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
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
        ConsumersFactory.insert_http_endpoint!(account_id: account_id).id
      end)

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(account_id: account_id).id
      end)

    {source_tables, attrs} =
      Map.pop_lazy(attrs, :source_tables, fn -> [source_table()] end)

    {message_kind, attrs} = Map.pop_lazy(attrs, :message_kind, fn -> Enum.random([:event, :record]) end)

    {record_consumer_state, attrs} =
      Map.pop_lazy(attrs, :record_consumer_state, fn ->
        if message_kind == :record, do: record_consumer_state_attrs()
      end)

    merge_attributes(
      %HttpPushConsumer{
        id: Factory.uuid(),
        account_id: account_id,
        ack_wait_ms: 30_000,
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        http_endpoint_id: http_endpoint_id,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        message_kind: message_kind,
        name: Factory.unique_word(),
        record_consumer_state: record_consumer_state,
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
      Consumers.create_http_push_consumer_for_account_with_lifecycle(account_id, attrs)

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

    {message_kind, attrs} = Map.pop_lazy(attrs, :message_kind, fn -> Enum.random([:event, :record]) end)

    {record_consumer_state, attrs} =
      Map.pop_lazy(attrs, :record_consumer_state, fn ->
        if message_kind == :record, do: record_consumer_state_attrs()
      end)

    merge_attributes(
      %HttpPullConsumer{
        id: Factory.uuid(),
        name: Factory.unique_word(),
        message_kind: message_kind,
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        ack_wait_ms: 30_000,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        account_id: account_id,
        record_consumer_state: record_consumer_state,
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

    case Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, attrs) do
      {:ok, consumer} ->
        consumer

      {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
        insert_http_pull_consumer!(attrs)
    end
  end

  def record_consumer_state_attrs(attrs \\ []) do
    attrs = Map.new(attrs)

    %RecordConsumerState{
      producer: Enum.random([:table_and_wal, :wal]),
      initial_min_cursor: %{Factory.unique_integer() => Factory.timestamp()}
    }
    |> merge_attributes(attrs)
    |> Sequin.Map.from_ecto()
  end

  def source_table(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequin.Consumers.SourceTable{
        oid: Factory.unique_integer(),
        actions: [:insert, :update, :delete],
        column_filters: [column_filter()],
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

    value_type = Map.get(attrs, :value_type, Enum.random([:string, :number, :boolean, :null, :list]))

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

    {record_pks, attrs} = Map.pop_lazy(attrs, :record_pks, fn -> [Faker.UUID.v4()] end)
    record_pks = Enum.map(record_pks, &to_string/1)

    merge_attributes(
      %ConsumerEvent{
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        record_pks: record_pks,
        table_oid: Enum.random(1..100_000),
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: Enum.random([nil, Factory.timestamp()]),
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
        metadata: %{
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
        record_pks: record_pks,
        table_oid: Enum.random(1..100_000),
        state: state,
        ack_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        not_visible_until: not_visible_until,
        replication_message_trace_id: Factory.uuid()
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
  end
end
