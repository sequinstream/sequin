defmodule Sequin.Factory.StreamsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers
  alias Sequin.Consumers.Consumer
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.HttpEndpoint
  alias Sequin.Streams.Message
  alias Sequin.Streams.SourceTable
  alias Sequin.Streams.SourceTableStreamTableColumnMapping
  alias Sequin.Streams.Stream
  alias Sequin.Streams.StreamTable
  alias Sequin.Streams.StreamTableColumn

  def message_data, do: Faker.String.base64(24)

  # ConsumerMessage

  def consumer_message(attrs \\ []) do
    attrs = Map.new(attrs)

    {state, attrs} =
      Map.pop_lazy(attrs, :state, fn ->
        Factory.one_of([:available, :delivered, :pending_redelivery])
      end)

    not_visible_until =
      unless state == :available do
        Factory.utc_datetime_usec()
      end

    merge_attributes(
      %ConsumerMessage{
        consumer_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        message_key: generate_key(parts: 3),
        message_seq: Enum.random(1..1000),
        not_visible_until: not_visible_until,
        state: state
      },
      attrs
    )
  end

  def consumer_message_attrs(attrs \\ []) do
    attrs
    |> consumer_message()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_message!(attrs \\ []) do
    attrs = Map.new(attrs)

    {message, attrs} = Map.pop(attrs, :message)

    message_attrs =
      if message do
        %{message_key: message.key, message_seq: message.seq}
      else
        %{}
      end

    message_attrs
    |> Map.merge(attrs)
    |> consumer_message()
    |> Repo.insert!()
  end

  def insert_consumer_message_with_message!(attrs \\ []) do
    attrs = Map.new(attrs)

    {message_stream_id, attrs} = Map.pop(attrs, :message_stream_id)

    {message, attrs} =
      Map.pop_lazy(attrs, :message, fn ->
        %{}
        |> Sequin.Map.put_if_present(:stream_id, message_stream_id)
        |> insert_message!()
      end)

    insert_consumer_message!(Map.put(attrs, :message, message))
  end

  # Message

  def message(attrs \\ []) do
    attrs = Map.new(attrs)

    {data, attrs} = Map.pop_lazy(attrs, :data, fn -> message_data() end)

    merge_attributes(
      %Message{
        stream_id: Factory.uuid(),
        data_hash: Base.encode64(:crypto.hash(:sha256, data)),
        data: data,
        seq: Postgres.sequence_nextval("#{Streams.stream_schema()}.messages_seq"),
        key: generate_key(parts: 3)
      },
      attrs
    )
  end

  def message_attrs(attrs \\ []) do
    attrs
    |> message()
    |> Sequin.Map.from_ecto()
  end

  def insert_message!(attrs \\ []) do
    attrs = Map.new(attrs)
    {stream_id, attrs} = Map.pop_lazy(attrs, :stream_id, fn -> insert_stream!().id end)

    attrs =
      attrs
      |> Map.put(:stream_id, stream_id)
      |> message_attrs()

    %Message{}
    |> Message.changeset(attrs)
    |> Repo.insert!()
  end

  # Consumer

  def consumer(attrs \\ []) do
    attrs = Map.new(attrs)

    {kind, attrs} = Map.pop_lazy(attrs, :kind, fn -> Factory.one_of([:pull, :push]) end)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {http_endpoint_id, attrs} =
      Map.pop_lazy(attrs, :http_endpoint_id, fn ->
        case kind do
          :push -> insert_http_endpoint!(account_id: account_id).id
          :pull -> nil
        end
      end)

    merge_attributes(
      %Consumer{
        name: Factory.unique_word(),
        backfill_completed_at: Enum.random([nil, Factory.timestamp()]),
        ack_wait_ms: 30_000,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        account_id: account_id,
        kind: kind,
        http_endpoint_id: http_endpoint_id,
        status: :active
      },
      attrs
    )
  end

  def consumer_attrs(attrs \\ []) do
    attrs
    |> consumer()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {stream_id, attrs} =
      Map.pop_lazy(attrs, :stream_id, fn -> insert_stream!(account_id: account_id).id end)

    attrs =
      attrs
      |> Map.put(:stream_id, stream_id)
      |> Map.put(:account_id, account_id)
      |> consumer_attrs()

    {:ok, consumer} =
      Consumers.create_consumer_for_account_with_lifecycle(account_id, attrs, no_backfill: true)

    consumer
  end

  # Stream

  def stream(attrs \\ []) do
    merge_attributes(
      %Stream{
        name: Factory.unique_word(),
        account_id: Factory.uuid()
      },
      attrs
    )
  end

  def stream_attrs(attrs \\ []) do
    attrs
    |> stream()
    |> Sequin.Map.from_ecto()
  end

  def insert_stream!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {:ok, stream} =
      Streams.create_stream_for_account_with_lifecycle(account_id, stream_attrs(attrs))

    stream
  end

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

  # StreamTableColumn

  def stream_table_column(attrs \\ []) do
    merge_attributes(
      %StreamTableColumn{
        id: Factory.uuid(),
        name: Faker.Lorem.word(),
        type: Enum.random(StreamTableColumn.column_types()),
        is_conflict_key: Enum.random([true, false]),
        stream_table_id: Factory.uuid()
      },
      attrs
    )
  end

  def stream_table_column_attrs(attrs \\ []) do
    attrs
    |> stream_table_column()
    |> Sequin.Map.from_ecto()
  end

  def insert_stream_table_column!(attrs \\ []) do
    attrs = Map.new(attrs)

    {stream_table_id, attrs} =
      Map.pop_lazy(attrs, :stream_table_id, fn -> insert_stream_table!().id end)

    attrs
    |> Map.put(:stream_table_id, stream_table_id)
    |> stream_table_column_attrs()
    |> then(&StreamTableColumn.create_changeset(%StreamTableColumn{}, &1))
    |> Repo.insert!()
  end

  # StreamTable

  def stream_table(attrs \\ []) do
    attrs = Map.new(attrs)

    {insert_mode, attrs} = Map.pop_lazy(attrs, :insert_mode, fn -> Enum.random([:append, :upsert]) end)
    {stream_columns, attrs} = Map.pop(attrs, :stream_columns)
    {stream_column_count, attrs} = Map.pop(attrs, :stream_column_count, 3)

    stream_columns =
      stream_columns ||
        for n <- 1..stream_column_count do
          is_conflict_key =
            cond do
              # Ensure at least one column is a primary key
              insert_mode == :upsert and n == 1 -> true
              insert_mode == :upsert -> Enum.random([true, false])
              true -> false
            end

          stream_table_column(is_conflict_key: is_conflict_key)
        end

    merge_attributes(
      %StreamTable{
        id: Factory.uuid(),
        name: Factory.unique_word(),
        table_schema_name: Factory.unique_postgres_object(),
        table_name: Factory.unique_postgres_object(),
        retention_policy: %{},
        insert_mode: insert_mode,
        account_id: Factory.uuid(),
        source_postgres_database_id: Factory.uuid(),
        source_replication_slot_id: Factory.uuid(),
        columns: stream_columns
      },
      attrs
    )
  end

  def stream_table_attrs(attrs \\ []) do
    attrs
    |> stream_table()
    |> Map.update(:columns, [], fn columns ->
      Enum.map(columns, &Sequin.Map.from_ecto/1)
    end)
    |> Sequin.Map.from_ecto()
  end

  def insert_stream_table!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {source_postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :source_postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    {source_replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :source_replication_slot_id, fn ->
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: source_postgres_database_id
        ).id
      end)

    attrs
    |> Map.merge(%{
      source_postgres_database_id: source_postgres_database_id,
      source_replication_slot_id: source_replication_slot_id
    })
    |> stream_table_attrs()
    |> then(&StreamTable.create_changeset(%StreamTable{account_id: account_id}, &1))
    |> Repo.insert!()
  end

  # SourceTable

  def source_table(attrs \\ []) do
    merge_attributes(
      %SourceTable{
        schema_oid: Enum.random(1..100_000),
        oid: Enum.random(1..100_000),
        schema_name: Factory.unique_postgres_object(),
        name: Factory.unique_postgres_object(),
        account_id: Factory.uuid(),
        postgres_database_id: Factory.uuid()
      },
      attrs
    )
  end

  def source_table_attrs(attrs \\ []) do
    attrs
    |> source_table()
    |> Sequin.Map.from_ecto()
  end

  def insert_source_table!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    attrs
    |> Map.merge(%{
      account_id: account_id,
      postgres_database_id: postgres_database_id
    })
    |> source_table_attrs()
    |> then(&SourceTable.changeset(%SourceTable{}, &1))
    |> Repo.insert!()
  end

  # SourceTableStreamTableColumnMapping

  def source_table_stream_table_column_mapping(attrs \\ []) do
    mapping_type = Enum.random([:record_field, :metadata])

    mapping_field =
      case mapping_type do
        :record_field -> Faker.Lorem.word()
        :metadata -> Enum.random(["action", "table_name"])
      end

    merge_attributes(
      %SourceTableStreamTableColumnMapping{
        source_table_id: Factory.uuid(),
        stream_column_id: Factory.uuid(),
        mapping: %SourceTableStreamTableColumnMapping.Mapping{
          type: mapping_type,
          field_name: mapping_field
        }
      },
      attrs
    )
  end

  def source_table_stream_table_column_mapping_attrs(attrs \\ []) do
    attrs
    |> source_table_stream_table_column_mapping()
    |> Map.update!(:mapping, &Sequin.Map.from_ecto/1)
    |> Sequin.Map.from_ecto()
  end

  def insert_source_table_stream_table_column_mapping!(attrs \\ []) do
    attrs = Map.new(attrs)

    {source_table_id, attrs} =
      Map.pop_lazy(attrs, :source_table_id, fn -> insert_source_table!().id end)

    {stream_column_id, attrs} =
      Map.pop_lazy(attrs, :stream_column_id, fn -> insert_stream_table_column!().id end)

    attrs
    |> Map.merge(%{
      source_table_id: source_table_id,
      stream_column_id: stream_column_id
    })
    |> source_table_stream_table_column_mapping_attrs()
    |> then(&SourceTableStreamTableColumnMapping.changeset(%SourceTableStreamTableColumnMapping{}, &1))
    |> Repo.insert!()
  end

  defp generate_key(parts: parts) when parts > 1 do
    s = Enum.map_join(1..(parts - 1), ".", fn _ -> Faker.Lorem.word() end)
    "#{s}.#{:erlang.unique_integer([:positive])}"
  end
end
