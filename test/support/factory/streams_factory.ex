defmodule Sequin.Factory.StreamsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Repo
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerState
  alias Sequin.Streams.Message
  alias Sequin.Streams.OutstandingMessage
  alias Sequin.Streams.Stream

  # OutstandingMessage

  def outstanding_message(attrs \\ []) do
    merge_attributes(
      %OutstandingMessage{
        consumer_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        message_key: Factory.uuid(),
        message_seq: Enum.random(1..1000),
        message_stream_id: Factory.uuid(),
        not_visible_until: Factory.timestamp(),
        state: Factory.one_of([:delivered, :available, :pending_redelivery])
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
    attrs
    |> outstanding_message()
    |> Repo.insert!()
  end

  # Message

  def message(attrs \\ []) do
    merge_attributes(
      %Message{
        key: Factory.uuid(),
        stream_id: Factory.uuid(),
        data_hash: Faker.Lorem.sentence(),
        data: Jason.encode!(%{sample: "data"}),
        seq: Enum.random(1..1000)
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
    attrs
    |> message()
    |> Repo.insert!()
  end

  # ConsumerState

  def consumer_state(attrs \\ []) do
    merge_attributes(
      %ConsumerState{
        consumer_id: Factory.uuid(),
        message_seq_cursor: Enum.random(1..1000)
      },
      attrs
    )
  end

  def consumer_state_attrs(attrs \\ []) do
    attrs
    |> consumer_state()
    |> Sequin.Map.from_ecto()
  end

  def insert_consumer_state!(attrs \\ []) do
    attrs
    |> consumer_state()
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
    attrs
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
    attrs
    |> stream()
    |> Repo.insert!()
  end
end
