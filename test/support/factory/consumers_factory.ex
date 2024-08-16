defmodule Sequin.Factory.ConsumersFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Factory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Repo

  # ConsumerEvent
  def consumer_event(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %ConsumerEvent{
        consumer_id: Factory.uuid(),
        commit_lsn: Enum.random(1..1_000_000),
        record_pks: %{id: Faker.UUID.v4()},
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
      Map.pop_lazy(attrs, :consumer_id, fn -> StreamsFactory.insert_consumer!().id end)

    attrs
    |> Map.put(:consumer_id, consumer_id)
    |> consumer_event_attrs()
    |> then(&ConsumerEvent.changeset(%ConsumerEvent{}, &1))
    |> Repo.insert!()
  end
end
