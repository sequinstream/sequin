defmodule Sequin.ConsumersTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.BooleanValue
  alias Sequin.Consumers.SequenceFilter.DateTimeValue
  alias Sequin.Consumers.SequenceFilter.ListValue
  alias Sequin.Consumers.SequenceFilter.NullValue
  alias Sequin.Consumers.SequenceFilter.NumberValue
  alias Sequin.Consumers.SequenceFilter.StringValue
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.Sequence
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterMultiPK

  describe "receive_for_consumer/2 with event message kind" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(max_ack_pending: 1_000, message_kind: :event)
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
      other_consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
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
          record_pks: [1],
          group_id: nil,
          table_oid: 12_345
        )

      # Create an event with the same record_pks and table_oid
      same_combo_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: outstanding_event.record_pks,
          group_id: outstanding_event.group_id,
          table_oid: outstanding_event.table_oid
        )

      # Create a different event
      different_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: [2],
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

    test "delivers event when it does not share a group_id with outstanding events", %{consumer: consumer} do
      # Insert an outstanding event
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
        group_id: "group_1"
      )

      # Insert a deliverable event with a different group_id
      deliverable_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: "group_2"
        )

      assert {:ok, [delivered_event]} = Consumers.receive_for_consumer(consumer)
      assert delivered_event.id == deliverable_event.id
      assert delivered_event.group_id == "group_2"
    end

    test "delivers only the first of multiple available events with a shared group_id", %{consumer: consumer} do
      event_attrs = [
        consumer_id: consumer.id,
        not_visible_until: nil,
        group_id: "shared_group",
        table_oid: Character.table_oid()
      ]

      ConsumersFactory.insert_consumer_event!(event_attrs)
      ConsumersFactory.insert_consumer_event!(event_attrs)
      ConsumersFactory.insert_consumer_event!(event_attrs)

      assert {:ok, [delivered_event]} = Consumers.receive_for_consumer(consumer)
      assert delivered_event.group_id == "shared_group"
    end

    test "does not deliver event when it shares a group_id with an outstanding event", %{consumer: consumer} do
      # Insert an outstanding event
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
        group_id: "shared_group",
        table_oid: 12_345
      )

      # Insert a deliverable event with the same group_id
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: nil,
        group_id: "shared_group",
        table_oid: 12_345
      )

      # Insert another deliverable event with a different group_id
      different_group_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: "different_group",
          table_oid: 12_345
        )

      assert {:ok, [delivered_event]} = Consumers.receive_for_consumer(consumer)
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
      assert delivered_event.id == different_group_event.id
      assert delivered_event.group_id == "different_group"

      # Verify that the event with the shared group_id was not delivered
      events = ConsumerEvent |> Repo.all() |> Enum.filter(&(&1.group_id == "shared_group"))
      assert length(events) == 2
      assert Enum.any?(events, &is_nil(&1.not_visible_until))
    end

    test "delivers multiple events with null group_ids unless they share record_pks and table_oid", %{consumer: consumer} do
      # Create events with null group_ids but different record_pks/table_oid
      event1 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: nil,
          record_pks: [1],
          table_oid: 12_345
        )

      event2 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: nil,
          record_pks: [2],
          table_oid: 12_345
        )

      # Create an event with same record_pks/table_oid as event1
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: nil,
        group_id: nil,
        record_pks: event1.record_pks,
        table_oid: event1.table_oid
      )

      assert {:ok, delivered_events} = Consumers.receive_for_consumer(consumer)
      assert length(delivered_events) == 2

      delivered_ids = delivered_events |> Enum.map(& &1.id) |> Enum.sort()
      expected_ids = Enum.sort([event1.id, event2.id])
      assert delivered_ids == expected_ids
    end

    test "delivers events with null group_ids alongside events with non-null group_ids", %{consumer: consumer} do
      # Create an event with a group_id
      event1 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: "group_1"
        )

      # Create an event with null group_id
      event2 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          group_id: nil,
          record_pks: [1],
          table_oid: 12_345
        )

      assert {:ok, delivered_events} = Consumers.receive_for_consumer(consumer)
      assert length(delivered_events) == 2

      delivered_ids = delivered_events |> Enum.map(& &1.id) |> Enum.sort()
      expected_ids = Enum.sort([event1.id, event2.id])
      assert delivered_ids == expected_ids
    end
  end

  describe "receive_for_consumer/2 with message_kind: :record" do
    setup do
      database = DatabasesFactory.insert_configured_postgres_database!(tables: [])
      # Load tables from actual database
      {:ok, _tables} = Databases.tables(database)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: database.id,
          account_id: database.account_id
        )

      source_tables = [
        ConsumersFactory.source_table(
          oid: Character.table_oid(),
          column_filters: []
        ),
        ConsumersFactory.source_table(
          oid: CharacterDetailed.table_oid(),
          column_filters: []
        ),
        ConsumersFactory.source_table(
          oid: CharacterMultiPK.table_oid(),
          column_filters: []
        )
      ]

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          max_ack_pending: 1_000,
          account_id: database.account_id,
          replication_slot_id: slot.id,
          source_tables: source_tables
        )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence])

      ConnectionCache.cache_connection(consumer.postgres_database, Repo)

      %{consumer: consumer, database: database}
    end

    test "returns nothing if consumer_records is empty", %{consumer: consumer} do
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers available outstanding records", %{consumer: consumer} do
      record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available, deliver_count: 0)
      ack_wait_ms = consumer.ack_wait_ms

      assert {:ok, [delivered_record]} = Consumers.receive_for_consumer(consumer)
      not_visible_until = DateTime.add(DateTime.utc_now(), ack_wait_ms - 1000, :millisecond)
      assert delivered_record.ack_id == record.ack_id
      assert delivered_record.id == record.id
      updated_record = Repo.get_by(ConsumerRecord, id: record.id)
      assert DateTime.after?(updated_record.not_visible_until, not_visible_until)
      assert updated_record.deliver_count == 1
      assert updated_record.last_delivered_at
      assert updated_record.state == :delivered
    end

    test "redelivers expired outstanding records", %{consumer: consumer} do
      record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), -1, :second),
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -30, :second)
        )

      assert {:ok, [redelivered_record]} = Consumers.receive_for_consumer(consumer)
      assert redelivered_record.id == record.id
      assert redelivered_record.ack_id == record.ack_id
      updated_record = Repo.get_by(ConsumerRecord, id: record.id)
      assert DateTime.compare(updated_record.not_visible_until, record.not_visible_until) != :eq
      assert updated_record.deliver_count == 2
      assert DateTime.compare(updated_record.last_delivered_at, record.last_delivered_at) != :eq
      assert updated_record.state == :delivered
    end

    test "does not redeliver unexpired outstanding records", %{consumer: consumer} do
      ConsumersFactory.insert_consumer_record!(
        consumer_id: consumer.id,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      )

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers only up to batch_size", %{consumer: consumer} do
      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      assert length(Repo.all(ConsumerRecord)) == 3
    end

    test "does not deliver outstanding records for another consumer", %{consumer: consumer} do
      other_consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      ConsumersFactory.insert_consumer_record!(consumer_id: other_consumer.id, state: :available)

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "with a mix of available and unavailable records, delivers only available outstanding records", %{
      consumer: consumer
    } do
      available =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
        end

      redeliver =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_record!(
            consumer_id: consumer.id,
            state: :delivered,
            not_visible_until: DateTime.add(DateTime.utc_now(), -30, :second)
          )
        end

      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      assert {:ok, records} = Consumers.receive_for_consumer(consumer)
      assert length(records) == length(available ++ redeliver)
      assert_lists_equal(records, available ++ redeliver, &assert_maps_equal(&1, &2, [:consumer_id, :id]))
    end

    test "delivers only the first of multiple records with shared group_id", %{consumer: consumer} do
      record_attrs = [
        consumer_id: consumer.id,
        state: :available,
        group_id: "shared_group",
        table_oid: Factory.integer()
      ]

      ConsumersFactory.insert_consumer_record!(record_attrs)
      ConsumersFactory.insert_consumer_record!(record_attrs)
      ConsumersFactory.insert_consumer_record!(record_attrs)

      assert {:ok, [delivered_record]} = Consumers.receive_for_consumer(consumer)
      assert delivered_record.group_id == "shared_group"
    end

    test "delivers records according to id asc", %{consumer: consumer} do
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      _record3 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      delivered_ids = Enum.map(delivered, & &1.id)
      assert_lists_equal(delivered_ids, [record1.id, record2.id])
    end

    test "respects a consumer's max_ack_pending", %{consumer: consumer} do
      max_ack_pending = 3
      consumer = %{consumer | max_ack_pending: max_ack_pending}

      record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer)
      assert length(delivered) == 1
      assert List.first(delivered).id == record.id
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "fetches source data for delivered records", %{consumer: consumer} do
      record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)

      assert {:ok, [delivered_record]} = Consumers.receive_for_consumer(consumer)
      assert delivered_record.id == record.id
      assert delivered_record.data.record == record.data.record
      assert delivered_record.data.metadata.table_name == record.data.metadata.table_name
      assert delivered_record.data.metadata.table_schema == record.data.metadata.table_schema
      assert delivered_record.data.metadata.database_name == record.data.metadata.database_name
    end

    test "delivers record when it does not share a group_id with outstanding records", %{consumer: consumer} do
      # Insert an outstanding record
      ConsumersFactory.insert_consumer_record!(
        consumer_id: consumer.id,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
        group_id: "group_1"
      )

      # Insert a deliverable record with a different group_id
      deliverable_record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :available,
          group_id: "group_2"
        )

      assert {:ok, [delivered_record]} = Consumers.receive_for_consumer(consumer)
      assert delivered_record.id == deliverable_record.id
      assert delivered_record.group_id == "group_2"
    end

    test "does not deliver record when it shares a group_id with an outstanding record", %{consumer: consumer} do
      table_oid = Factory.integer()
      # Insert an outstanding record
      ConsumersFactory.insert_consumer_record!(
        consumer_id: consumer.id,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
        group_id: "shared_group",
        table_oid: table_oid
      )

      # Insert a deliverable record with the same group_id
      ConsumersFactory.insert_consumer_record!(
        consumer_id: consumer.id,
        state: :available,
        group_id: "shared_group",
        table_oid: table_oid
      )

      # Insert another deliverable record with a different group_id
      different_group_record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :available,
          group_id: "different_group",
          table_oid: table_oid
        )

      assert {:ok, [delivered_record]} = Consumers.receive_for_consumer(consumer)
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
      assert delivered_record.id == different_group_record.id
      assert delivered_record.group_id == "different_group"

      # Verify that the record with the shared group_id was not delivered
      records = ConsumerRecord |> Repo.all() |> Enum.filter(&(&1.group_id == "shared_group"))
      assert length(records) == 2
      assert Enum.any?(records, &(&1.state == :available))
    end
  end

  describe "ack_messages/2" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()
      {:ok, consumer: consumer}
    end

    test "acknowledges records" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      records =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_record!(
            consumer_id: consumer.id,
            state: :delivered
          )
        end

      ack_ids = Enum.map(records, & &1.ack_id)

      assert {:ok, 3} = Consumers.ack_messages(consumer, ack_ids)

      assert Repo.all(ConsumerRecord) == []
    end

    test "acknowledges events" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)

      events =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(
            consumer_id: consumer.id,
            not_visible_until: DateTime.utc_now()
          )
        end

      ack_ids = Enum.map(events, & &1.ack_id)

      assert {:ok, 3} = Consumers.ack_messages(consumer, ack_ids)

      assert Repo.all(ConsumerEvent) == []
    end

    test "silently ignores non-existent ack_ids" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      valid_record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      non_existent_ack_id = UUID.uuid4()

      assert {:ok, 1} = Consumers.ack_messages(consumer, [valid_record.ack_id, non_existent_ack_id])
      assert Repo.all(ConsumerRecord) == []
    end

    test "handles empty ack_ids list", %{consumer: consumer} do
      assert {:ok, 0} = Consumers.ack_messages(consumer, [])
    end

    test "acknowledges only records/events for the given consumer" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      other_consumer = ConsumersFactory.insert_sink_consumer!(max_ack_pending: 100, message_kind: :record)

      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: other_consumer.id, state: :delivered)

      assert {:ok, 1} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id])

      assert [ignore] = Repo.all(ConsumerRecord)
      assert ignore.id == record2.id
    end

    test "acknowledged messages are stored in redis" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)

      assert {:ok, 2} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id])

      assert {:ok, messages} = AcknowledgedMessages.fetch_messages(consumer.id)
      assert length(messages) == 2
      assert Enum.all?(messages, &(&1.consumer_id == consumer.id))
    end
  end

  describe "matches_record?/3" do
    @table_oid 12_345
    setup do
      consumer =
        ConsumersFactory.sink_consumer(
          sequence: %Sequence{table_oid: @table_oid},
          sequence_filter: %SequenceFilter{
            actions: [:insert, :update, :delete],
            column_filters: [
              ConsumersFactory.sequence_filter_column_filter(
                column_attnum: 1,
                operator: :==,
                value: %StringValue{value: "test_value"}
              ),
              ConsumersFactory.sequence_filter_column_filter(
                column_attnum: 2,
                operator: :>,
                value: %NumberValue{value: 10}
              )
            ]
          }
        )

      {:ok, consumer: consumer}
    end

    test "matches when all column filters match", %{consumer: consumer} do
      record = %{
        1 => "test_value",
        2 => 15
      }

      assert Consumers.matches_record?(consumer, @table_oid, record)
    end

    test "does not match when any column filter doesn't match", %{consumer: consumer} do
      record1 = %{
        1 => "wrong_value",
        2 => 15
      }

      record2 = %{
        1 => "test_value",
        2 => 5
      }

      refute Consumers.matches_record?(consumer, @table_oid, record1)
      refute Consumers.matches_record?(consumer, @table_oid, record2)
    end

    test "matches when no column filters are present" do
      consumer =
        ConsumersFactory.sink_consumer(
          sequence: %Sequence{table_oid: @table_oid},
          sequence_filter: %SequenceFilter{
            actions: [:insert, :update, :delete],
            column_filters: []
          }
        )

      record = %{
        1 => "any_value",
        2 => 100
      }

      assert Consumers.matches_record?(consumer, @table_oid, record)
    end
  end

  describe "HttpEndpoint.url/1" do
    test "returns correct URL for standard endpoint" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          port: 8080,
          path: "/webhook",
          query: "param=value",
          fragment: "section"
        )

      assert HttpEndpoint.url(http_endpoint) == "https://example.com:8080/webhook?param=value#section"
    end

    test "returns correct URL when port is not specified" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          path: "/webhook"
        )

      assert HttpEndpoint.url(http_endpoint) == "https://example.com/webhook"
    end

    test "returns modified URL when using a local tunnel" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          path: "/webhook",
          use_local_tunnel: true
        )

      expected_url = "http://#{Application.fetch_env!(:sequin, :portal_hostname)}:#{http_endpoint.port}/webhook"
      assert HttpEndpoint.url(http_endpoint) == expected_url
    end
  end

  describe "create_http_endpoint_for_account/2" do
    test "creates a http_endpoint with valid attributes" do
      account = AccountsFactory.insert_account!()

      valid_attrs = %{
        name: "TestEndpoint",
        scheme: :https,
        host: "example.com",
        port: 443,
        path: "/webhook",
        headers: %{"Content-Type" => "application/json"},
        use_local_tunnel: false
      }

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint(account.id, valid_attrs)
      assert http_endpoint.name == "TestEndpoint"
      assert http_endpoint.scheme == :https
      assert http_endpoint.host == "example.com"
      assert http_endpoint.port == 443
      assert http_endpoint.path == "/webhook"
      assert http_endpoint.headers == %{"Content-Type" => "application/json"}
      refute http_endpoint.use_local_tunnel
    end

    test "creates a http_endpoint with local tunnel" do
      account = AccountsFactory.insert_account!()

      valid_attrs = %{
        name: "my-endpoint",
        use_local_tunnel: true
      }

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint(account.id, valid_attrs)
      assert http_endpoint.name == "my-endpoint"
      assert http_endpoint.use_local_tunnel
      assert http_endpoint.port
      refute http_endpoint.host
    end

    test "returns error changeset with invalid attributes" do
      account = AccountsFactory.insert_account!()

      invalid_attrs = %{
        name: nil,
        scheme: :invalid,
        host: "",
        port: -1,
        headers: "invalid"
      }

      assert {:error, %Ecto.Changeset{}} = Consumers.create_http_endpoint(account.id, invalid_attrs)
    end
  end

  describe "update_http_endpoint/2" do
    test "updates the http_endpoint with valid attributes" do
      http_endpoint = ConsumersFactory.insert_http_endpoint!()

      update_attrs = %{
        name: "update-endpoint",
        scheme: :http,
        host: "updated.example.com",
        port: 8080,
        path: "/updated",
        headers: %{"Authorization" => "Bearer token"},
        use_local_tunnel: false
      }

      assert {:ok, %HttpEndpoint{} = updated_endpoint} = Consumers.update_http_endpoint(http_endpoint, update_attrs)
      assert updated_endpoint.name == "update-endpoint"
      assert updated_endpoint.scheme == :http
      assert updated_endpoint.host == "updated.example.com"
      assert updated_endpoint.port == 8080
      assert updated_endpoint.path == "/updated"
      assert updated_endpoint.headers == %{"Authorization" => "Bearer token"}
    end

    test "returns error changeset with invalid attributes" do
      http_endpoint = ConsumersFactory.insert_http_endpoint!()

      invalid_attrs = %{
        name: nil,
        scheme: :invalid,
        host: "",
        port: -1,
        headers: "invalid"
      }

      assert {:error, %Ecto.Changeset{}} = Consumers.update_http_endpoint(http_endpoint, invalid_attrs)
    end
  end

  describe "matches_message/2" do
    test "matches when action is in allowed actions" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update],
              column_filters: []
            )
          ]
        )

      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      update_message = ReplicationFactory.postgres_message(action: :update, table_oid: table_oid)
      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)

      assert Consumers.matches_message?(consumer, insert_message)
      assert Consumers.matches_message?(consumer, update_message)
      refute Consumers.matches_message?(consumer, delete_message)
    end

    test "matches message with correct oid and ignores message with incorrect oid" do
      matching_oid = Sequin.Factory.unique_integer()
      non_matching_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: matching_oid,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      matching_message = ReplicationFactory.postgres_message(action: :insert, table_oid: matching_oid)
      non_matching_message = ReplicationFactory.postgres_message(action: :insert, table_oid: non_matching_oid)

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not match when action is not in allowed actions" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update],
              column_filters: []
            )
          ]
        )

      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)
      refute Consumers.matches_message?(consumer, delete_message)

      # Test with a message that has a matching OID but disallowed action
      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      assert Consumers.matches_message?(consumer, insert_message)
    end

    test "matches when no column filters are present" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      update_message = ReplicationFactory.postgres_message(action: :update, table_oid: table_oid)
      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)

      assert Consumers.matches_message?(consumer, insert_message)
      assert Consumers.matches_message?(consumer, update_message)
      assert Consumers.matches_message?(consumer, delete_message)
    end

    test "matches when all column filters match" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not match when any column filter doesn't match" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 3,
                  operator: :!=,
                  value: %StringValue{value: "excluded_value"}
                )
              ]
            )
          ]
        )

      non_matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: "some_value")
          ]
        )

      non_matching_message2 =
        ReplicationFactory.postgres_message(
          action: :update,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5),
            ReplicationFactory.field(column_attnum: 3, value: "some_value")
          ]
        )

      non_matching_message3 =
        ReplicationFactory.postgres_message(
          action: :delete,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: "excluded_value")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message1)
      refute Consumers.matches_message?(consumer, non_matching_message2)
      refute Consumers.matches_message?(consumer, non_matching_message3)
    end

    test "equality operator (==) matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %NumberValue{value: 123}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 123)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 456)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for boolean values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %NumberValue{value: 123}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 456)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 123)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for boolean values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than operator (>) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than operator (>) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than operator (<) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than operator (<) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than or equal to operator (>=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>=,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than or equal to operator (>=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than or equal to operator (<=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<=,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than or equal to operator (<=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "in operator matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: ["value1", "value2"]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value1")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value3")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "in operator matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: [1, 2, 3]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 2)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 4)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_in operator matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_in,
                  value: %ListValue{value: ["value1", "value2"]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value3")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value1")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_in operator matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_in,
                  value: %ListValue{value: [1, 2, 3]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 4)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 2)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "is_null operator matches for null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :is_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "is_null operator does not match for non-null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :is_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_null operator matches for non-null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_null operator does not match for null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly compares datetimes with after and before operators" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %DateTimeValue{value: ~U[2022-03-31 12:00:00Z]}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :<,
                  value: %DateTimeValue{value: ~U[2022-04-02 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-04-01 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-01 12:00:00Z])
          ]
        )

      non_matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-03-30 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-01 12:00:00Z])
          ]
        )

      non_matching_message2 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-04-01 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-03 12:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message1)
      refute Consumers.matches_message?(consumer, non_matching_message2)
    end

    test "matches with multiple source tables" do
      table_oid1 = Sequin.Factory.unique_integer()
      table_oid2 = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid1,
              actions: [:insert, :update, :delete],
              column_filters: []
            ),
            ConsumersFactory.source_table(
              oid: table_oid2,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid1
        )

      matching_message2 =
        ReplicationFactory.postgres_message(
          action: :update,
          table_oid: table_oid2
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :delete,
          table_oid: Sequin.Factory.unique_integer()
        )

      assert Consumers.matches_message?(consumer, matching_message1)
      assert Consumers.matches_message?(consumer, matching_message2)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches with multiple column filters" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles missing fields gracefully" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: []
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches case-sensitive string comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "TestValue"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "TestValue")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "testvalue")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles whitespace in string comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: " test value "}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: " test value ")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles timezone-aware datetime comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly handles boolean true/false values in comparisons" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "properly handles empty lists in 'in' and 'not_in' operators" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: [:in_value]}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :not_in,
                  value: %ListValue{value: [:not_in_value]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: :in_value),
            ReplicationFactory.field(column_attnum: 2, value: :in_value)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: :not_in_value),
            ReplicationFactory.field(column_attnum: 2, value: :not_in_value)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly compares values at the boundaries of ranges" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :<,
                  value: %NumberValue{value: 20}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 3,
                  operator: :>=,
                  value: %NumberValue{value: 30}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 4,
                  operator: :<=,
                  value: %NumberValue{value: 40}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: 30),
            ReplicationFactory.field(column_attnum: 4, value: 40)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 10),
            ReplicationFactory.field(column_attnum: 2, value: 20),
            ReplicationFactory.field(column_attnum: 3, value: 29),
            ReplicationFactory.field(column_attnum: 4, value: 41)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches JSONB top-level field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"},
                  jsonb_path: "top_level_key"
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"top_level_key" => "test_value"})
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"top_level_key" => "wrong_value"})
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches JSONB nested field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10},
                  jsonb_path: "nested.field"
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"field" => 15}})
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"field" => 5}})
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles missing JSONB nested field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"},
                  jsonb_path: "nested.non_existent"
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"other_field" => "test_value"}})
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not support traversing JSONB array elements" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: ["value1", "value2"]},
                  jsonb_path: "array.0"
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"array" => ["value1", "other"]}),
            ReplicationFactory.field(column_attnum: 2, value: "other")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end
  end

  describe "consumer_features/1" do
    test "returns legacy_event_transform feature when conditions are met" do
      account = AccountsFactory.account(features: ["legacy_event_transform"])
      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == [legacy_event_transform: true]
    end

    test "does not return legacy_event_transform feature when account doesn't have the feature" do
      account = AccountsFactory.account(features: [], inserted_at: DateTime.utc_now())
      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == []
    end

    test "returns legacy_event_singleton_transform when account is old enough" do
      account = AccountsFactory.account(features: [], inserted_at: ~D[2024-11-01])

      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == [{:legacy_event_singleton_transform, true}]
    end
  end

  describe "upsert_failed_messages/2" do
    test "inserts a new failed message" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      assert {:ok, [inserted_msg]} = Consumers.upsert_failed_messages(consumer, [msg])

      assert inserted_msg.ack_id == msg.ack_id
      assert DateTime.after?(inserted_msg.not_visible_until, DateTime.utc_now())
    end

    test "updates existing failed message" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      existing_msg =
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), -30, :second)
        )

      assert {:ok, [updated_msg]} = Consumers.upsert_failed_messages(consumer, [existing_msg])

      assert DateTime.after?(updated_msg.not_visible_until, existing_msg.not_visible_until)
    end
  end

  describe "upsert_consumer_messages/1" do
    test "inserts a new message" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      assert {:ok, [inserted_msg]} = Consumers.upsert_consumer_messages(consumer, [msg])

      assert inserted_msg.ack_id == msg.ack_id
    end

    test "updates existing message" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      existing_msg =
        ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)

      updated_attrs = %{
        existing_msg
        | not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      }

      assert {:ok, [updated_msg]} = Consumers.upsert_consumer_messages(consumer, [updated_attrs])

      refute updated_msg.not_visible_until == existing_msg.not_visible_until
    end
  end
end
