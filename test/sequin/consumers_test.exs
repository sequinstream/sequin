defmodule Sequin.ConsumersTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.StreamsFactory

  describe "insert_consumer_events/2" do
    setup do
      consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      %{consumer: consumer}
    end

    test "inserts batch of consumer events", %{consumer: consumer} do
      events = for _ <- 1..3, do: ConsumersFactory.consumer_event_attrs(%{consumer_id: consumer.id})

      assert {:ok, 3} = Consumers.insert_consumer_events(consumer.id, events)

      inserted_events = Repo.all(ConsumerEvent)
      assert length(inserted_events) == 3

      assert_lists_equal(inserted_events, events, &assert_maps_equal(&1, &2, [:consumer_id, :commit_lsn, :data]))
    end
  end

  describe "list_consumer_events_for_consumer/2" do
    setup do
      consumer1 = StreamsFactory.insert_consumer!(message_kind: :event)
      consumer2 = StreamsFactory.insert_consumer!(message_kind: :event)

      events1 =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(consumer_id: consumer1.id)
        end

      events2 = [ConsumersFactory.insert_consumer_event!(consumer_id: consumer2.id)]

      %{consumer1: consumer1, consumer2: consumer2, events1: events1, events2: events2}
    end

    test "returns events only for the specified consumer", %{consumer1: consumer1, consumer2: consumer2} do
      events1 = Consumers.list_consumer_events_for_consumer(consumer1.id)
      assert length(events1) == 3
      assert Enum.all?(events1, &(&1.consumer_id == consumer1.id))

      events2 = Consumers.list_consumer_events_for_consumer(consumer2.id)
      assert length(events2) == 1
      assert Enum.all?(events2, &(&1.consumer_id == consumer2.id))
    end

    test "filters events by is_deliverable", %{consumer1: consumer1, events1: events1} do
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer1.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      )

      deliverable_events = Consumers.list_consumer_events_for_consumer(consumer1.id, is_deliverable: true)
      assert length(deliverable_events) == 3
      assert_lists_equal(deliverable_events, events1, &(&1.id == &2.id))

      non_deliverable_events = Consumers.list_consumer_events_for_consumer(consumer1.id, is_deliverable: false)
      assert length(non_deliverable_events) == 1
      assert hd(non_deliverable_events).not_visible_until
    end

    test "limits the number of returned events", %{consumer1: consumer1} do
      limited_events = Consumers.list_consumer_events_for_consumer(consumer1.id, limit: 2)
      assert length(limited_events) == 2
    end

    test "orders events by specified criteria", %{consumer1: consumer1} do
      events_asc = Consumers.list_consumer_events_for_consumer(consumer1.id, order_by: [asc: :id])
      events_desc = Consumers.list_consumer_events_for_consumer(consumer1.id, order_by: [desc: :id])

      assert Enum.map(events_asc, & &1.id) == Enum.sort(Enum.map(events_asc, & &1.id))
      assert Enum.map(events_desc, & &1.id) == Enum.sort(Enum.map(events_desc, & &1.id), :desc)
    end
  end

  describe "receive_for_consumer/2" do
    setup do
      consumer = StreamsFactory.insert_consumer!(max_ack_pending: 1_000, message_kind: :event)
      %{consumer: consumer}
    end

    test "returns nothing if consumer_events is empty", %{consumer: consumer} do
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers available outstanding events", %{consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil, deliver_count: 0)
      ack_wait_ms = consumer.ack_wait_ms

      assert {:ok, [delivered_event]} = Consumers.receive_for_consumer(consumer)
      not_visible_until = DateTime.add(DateTime.utc_now(), ack_wait_ms - 1000, :millisecond)
      assert delivered_event.ack_id == event.ack_id
      assert delivered_event.id == event.id
      updated_event = Repo.get_by(ConsumerEvent, id: event.id)
      assert DateTime.after?(updated_event.not_visible_until, not_visible_until)
      assert updated_event.deliver_count == 1
      assert updated_event.last_delivered_at
    end

    test "redelivers expired outstanding events", %{consumer: consumer} do
      event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), -1, :second),
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -30, :second)
        )

      assert {:ok, [redelivered_event]} = Consumers.receive_for_consumer(consumer)
      assert redelivered_event.id == event.id
      assert redelivered_event.ack_id == event.ack_id
      updated_event = Repo.get_by(ConsumerEvent, id: event.id)
      assert DateTime.compare(updated_event.not_visible_until, event.not_visible_until) != :eq
      assert updated_event.deliver_count == 2
      assert DateTime.compare(updated_event.last_delivered_at, event.last_delivered_at) != :eq
    end

    test "does not redeliver unexpired outstanding events", %{consumer: consumer} do
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      )

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers only up to batch_size", %{consumer: consumer} do
      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      assert length(Repo.all(ConsumerEvent)) == 3
    end

    test "does not deliver outstanding events for another consumer", %{consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id, not_visible_until: nil)

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "with a mix of available and unavailable events, delivers only available outstanding events", %{
      consumer: consumer
    } do
      available =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
        end

      redeliver =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(
            consumer_id: consumer.id,
            not_visible_until: DateTime.add(DateTime.utc_now(), -30, :second)
          )
        end

      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      assert {:ok, events} = Consumers.receive_for_consumer(consumer)
      assert length(events) == length(available ++ redeliver)
      assert_lists_equal(events, available ++ redeliver, &assert_maps_equal(&1, &2, [:consumer_id, :id]))
    end

    test "does not deliver events if there is an outstanding event with same record_pks and table_oid", %{
      consumer: consumer
    } do
      # Create an outstanding event
      outstanding_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
          record_pks: %{"id" => 1},
          table_oid: 12_345
        )

      # Create an event with the same record_pks and table_oid
      same_combo_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: outstanding_event.record_pks,
          table_oid: outstanding_event.table_oid
        )

      # Create a different event
      different_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: %{"id" => 2},
          table_oid: 67_890
        )

      # Attempt to receive events
      assert {:ok, delivered_events} = Consumers.receive_for_consumer(consumer)

      # Check that only the different event was delivered
      assert length(delivered_events) == 1
      assert hd(delivered_events).id == different_event.id

      # Verify that the same_combo_event was not delivered
      refute Repo.get_by(ConsumerEvent, id: same_combo_event.id).not_visible_until
    end

    test "delivers events according to id asc", %{consumer: consumer} do
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      _event3 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      delivered_ids = Enum.map(delivered, & &1.id)
      assert_lists_equal(delivered_ids, [event1.id, event2.id])
    end

    test "respects a consumer's max_ack_pending", %{consumer: consumer} do
      max_ack_pending = 3
      consumer = %{consumer | max_ack_pending: max_ack_pending}

      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer)
      assert length(delivered) == 1
      assert List.first(delivered).id == event.id
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end
  end

  describe "receive_for_consumer with concurrent workers" do
    setup do
      consumer = StreamsFactory.insert_consumer!(max_ack_pending: 100, message_kind: :event)

      for _ <- 1..10 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      {:ok, consumer: consumer}
    end

    test "ensures unique events are received by concurrent workers", %{consumer: consumer} do
      tasks =
        Enum.map(1..20, fn _ ->
          Task.async(fn ->
            Consumers.receive_for_consumer(consumer, batch_size: 1)
          end)
        end)

      results = Task.await_many(tasks, 5000)

      {successful, empty} =
        Enum.reduce(results, {[], []}, fn result, {successful, empty} ->
          case result do
            {:ok, [event]} -> {[event | successful], empty}
            {:ok, []} -> {successful, ["empty" | empty]}
          end
        end)

      assert length(successful) == 10
      assert length(empty) == 10

      unique_events = Enum.uniq_by(successful, & &1.id)
      assert length(unique_events) == 10
    end
  end

  describe "ConsumerEvent, ack_messages/2" do
    setup do
      consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      %{consumer: consumer}
    end

    test "acks delivered events and ignores non-existent event_ids", %{consumer: consumer} do
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())
      non_existent_id = Factory.uuid()

      :ok = Consumers.ack_messages(consumer, [event1.ack_id, event2.ack_id, non_existent_id])

      assert Repo.all(ConsumerEvent) == []
    end

    test "ignores events with different consumer_id", %{consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())

      event2 =
        ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id, not_visible_until: DateTime.utc_now())

      :ok = Consumers.ack_messages(consumer, [event1.ack_id, event2.ack_id])

      remaining_events = Repo.all(ConsumerEvent)
      assert length(remaining_events) == 1
      assert List.first(remaining_events).id == event2.id
    end
  end

  describe "ConsumerEvent, nack_messages/2" do
    setup do
      consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      %{consumer: consumer}
    end

    test "nacks events and ignores unknown event ID", %{consumer: consumer} do
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())
      non_existent_id = Factory.uuid()

      :ok = Consumers.nack_messages(consumer, [event1.ack_id, event2.ack_id, non_existent_id])

      updated_event1 = Repo.get_by(ConsumerEvent, id: event1.id)
      updated_event2 = Repo.get_by(ConsumerEvent, id: event2.id)

      assert is_nil(updated_event1.not_visible_until)
      assert is_nil(updated_event2.not_visible_until)
    end

    test "does not nack events belonging to another consumer", %{consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(message_kind: :event)
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: DateTime.utc_now())

      event2 =
        ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id, not_visible_until: DateTime.utc_now())

      :ok = Consumers.nack_messages(consumer, [event1.ack_id, event2.ack_id])

      updated_event1 = Repo.get_by(ConsumerEvent, id: event1.id)
      updated_event2 = Repo.get_by(ConsumerEvent, id: event2.id)

      assert is_nil(updated_event1.not_visible_until)
      assert DateTime.compare(updated_event2.not_visible_until, event2.not_visible_until) == :eq
    end
  end
end
