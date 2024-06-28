defmodule Sequin.Factory.StreamsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerState
  alias Sequin.Streams.Message
  alias Sequin.Streams.OutstandingMessage
  alias Sequin.Streams.Stream

  def message_data, do: Faker.String.base64(24)

  # OutstandingMessage

  def outstanding_message(attrs \\ []) do
    attrs = Map.new(attrs)
    {state, attrs} = Map.pop_lazy(attrs, :state, fn -> Factory.one_of([:delivered, :available, :pending_redelivery]) end)

    not_visible_until =
      unless state == :available do
        Factory.utc_datetime_usec()
      end

    merge_attributes(
      %OutstandingMessage{
        consumer_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        message_key: Factory.uuid(),
        message_seq: Enum.random(1..1000),
        message_stream_id: Factory.uuid(),
        not_visible_until: not_visible_until,
        state: state
      },
      attrs
    )
  end

  def outstanding_message_attrs(attrs \\ []) do
    attrs
    |> outstanding_message()
    |> Sequin.Map.from_ecto()
  end

  def insert_outstanding_message!(attrs \\ []) do
    attrs = Map.new(attrs)

    {message, attrs} = Map.pop(attrs, :message)

    message_attrs =
      if message do
        %{message_key: message.key, message_stream_id: message.stream_id, message_seq: message.seq}
      else
        %{}
      end

    message_attrs
    |> Map.merge(attrs)
    |> outstanding_message()
    |> Repo.insert!()
  end

  # Message

  def message(attrs \\ []) do
    attrs = Map.new(attrs)

    {data, attrs} = Map.pop_lazy(attrs, :data, fn -> message_data() end)

    merge_attributes(
      %Message{
        key: Factory.uuid(),
        stream_id: Factory.uuid(),
        data_hash: Base.encode64(:crypto.hash(:sha256, data)),
        data: data,
        seq: Postgres.sequence_nextval("streams.messages_seq")
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

    attrs
    |> Map.put(:stream_id, stream_id)
    |> message()
    |> Repo.insert!()
  end

  # ConsumerState

  def get_consumer_state(attrs \\ []) do
    merge_attributes(
      %ConsumerState{
        consumer_id: Factory.uuid(),
        message_seq_cursor: Enum.random(1..1000)
      },
      attrs
    )
  end

  def get_consumer_state_attrs(attrs \\ []) do
    attrs
    |> get_consumer_state()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_state!(attrs \\ []) do
    attrs = Map.new(attrs)

    {consumer_id, attrs} = Map.pop_lazy(attrs, :consumer_id, fn -> insert_consumer!().id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> get_consumer_state()
    |> Repo.insert!()
  end

  # Consumer

  def consumer(attrs \\ []) do
    merge_attributes(
      %Consumer{
        ack_wait_ms: 30_000,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        stream_id: Factory.uuid(),
        account_id: Factory.uuid()
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

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)
    {stream_id, attrs} = Map.pop_lazy(attrs, :stream_id, fn -> insert_stream!(account_id: account_id).id end)

    attrs
    |> Map.put(:stream_id, stream_id)
    |> Map.put(:account_id, account_id)
    |> consumer()
    |> Repo.insert!()
  end

  # Stream

  def stream(attrs \\ []) do
    merge_attributes(
      %Stream{
        idx: Enum.random(1..1000),
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

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {:ok, stream} =
      attrs
      |> Map.put(:account_id, account_id)
      |> stream_attrs()
      |> Streams.create_with_lifecycle()

    stream
  end
end
