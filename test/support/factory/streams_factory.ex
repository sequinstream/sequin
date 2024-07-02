defmodule Sequin.Factory.StreamsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.Message
  alias Sequin.Streams.Stream

  def message_data, do: Faker.String.base64(24)

  # ConsumerMessage

  def consumer_message(attrs \\ []) do
    attrs = Map.new(attrs)
    {state, attrs} = Map.pop_lazy(attrs, :state, fn -> Factory.one_of([:delivered, :available, :pending_redelivery]) end)

    not_visible_until =
      unless state == :available do
        Factory.utc_datetime_usec()
      end

    merge_attributes(
      %ConsumerMessage{
        consumer_id: Factory.uuid(),
        deliver_count: Enum.random(0..10),
        last_delivered_at: Factory.timestamp(),
        message_subject: generate_subject(parts: 3),
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
        %{message_subject: message.subject, message_seq: message.seq}
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
        seq: Postgres.sequence_nextval("streams.messages_seq"),
        subject: generate_subject(parts: 3)
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
    merge_attributes(
      %Consumer{
        slug: generate_slug(),
        ack_wait_ms: 30_000,
        max_ack_pending: 10_000,
        max_deliver: Enum.random(1..100),
        max_waiting: 20,
        stream_id: Factory.uuid(),
        account_id: Factory.uuid(),
        filter_subject: generate_subject(parts: 3)
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

    attrs =
      attrs
      |> Map.put(:stream_id, stream_id)
      |> Map.put(:account_id, account_id)
      |> consumer_attrs()

    {:ok, consumer} = Streams.create_consumer_for_account_with_lifecycle(account_id, attrs)
    consumer
  end

  # Stream

  def stream(attrs \\ []) do
    merge_attributes(
      %Stream{
        slug: generate_slug(),
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
      Streams.create_stream_for_account_with_lifecycle(account_id, stream_attrs(attrs))

    stream
  end

  defp generate_subject(parts: parts) do
    Enum.map_join(1..parts, ".", fn _ -> Faker.Lorem.word() end)
  end

  defp generate_slug do
    "#{Faker.Lorem.word()}_#{:erlang.unique_integer([:positive])}"
  end
end
