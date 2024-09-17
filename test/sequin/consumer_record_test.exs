defmodule Sequin.ConsumersTest.ConsumerRecordTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Factory.ConsumersFactory

  describe "consumer_record" do
    test "reload/1 reloads a consumer record" do
      consumer_record = ConsumersFactory.insert_consumer_record!()
      reloaded = Consumers.reload(consumer_record)
      assert reloaded.id == consumer_record.id
      assert reloaded.consumer_id == consumer_record.consumer_id
    end

    test "get_consumer_record/2 returns the consumer record with given consumer_id and id" do
      consumer_record = ConsumersFactory.insert_consumer_record!()
      {:ok, fetched_record} = Consumers.get_consumer_record(consumer_record.consumer_id, consumer_record.id)
      assert fetched_record.id == consumer_record.id
    end

    test "get_consumer_record/2 returns error when consumer record does not exist" do
      assert {:error, _} = Consumers.get_consumer_record(Ecto.UUID.generate(), 123)
    end

    test "get_consumer_record!/2 returns the consumer record with given consumer_id and id" do
      consumer_record = ConsumersFactory.insert_consumer_record!()
      fetched_record = Consumers.get_consumer_record!(consumer_record.consumer_id, consumer_record.id)
      assert fetched_record.id == consumer_record.id
    end

    test "get_consumer_record!/2 raises error when consumer record does not exist" do
      assert_raise Sequin.Error.NotFoundError, fn ->
        Consumers.get_consumer_record!(Ecto.UUID.generate(), 123)
      end
    end

    test "list_consumer_records_for_consumer/2 returns all consumer records for a consumer" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      consumer_record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id)
      consumer_record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id)
      consumer_record3 = ConsumersFactory.insert_consumer_record!()

      records = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert length(records) == 2
      assert Enum.any?(records, fn r -> r.id == consumer_record1.id end)
      assert Enum.any?(records, fn r -> r.id == consumer_record2.id end)
      refute Enum.any?(records, fn r -> r.id == consumer_record3.id end)
    end

    test "list_consumer_records_for_consumer/2 filters by deliverable state" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      now = DateTime.utc_now()
      past = DateTime.add(now, -60, :second)
      future = DateTime.add(now, 60, :second)

      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, not_visible_until: nil)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, not_visible_until: past)
      record3 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, not_visible_until: future)

      deliverable_records = Consumers.list_consumer_records_for_consumer(consumer.id, is_deliverable: true)
      assert length(deliverable_records) == 2
      assert Enum.any?(deliverable_records, fn r -> r.id == record1.id end)
      assert Enum.any?(deliverable_records, fn r -> r.id == record2.id end)
      refute Enum.any?(deliverable_records, fn r -> r.id == record3.id end)

      non_deliverable_records = Consumers.list_consumer_records_for_consumer(consumer.id, is_deliverable: false)
      assert length(non_deliverable_records) == 1
      assert Enum.any?(non_deliverable_records, fn r -> r.id == record3.id end)
    end

    test "list_consumer_records_for_consumer/2 respects limit parameter" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      Enum.each(1..5, fn _ -> ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id) end)

      records = Consumers.list_consumer_records_for_consumer(consumer.id, limit: 3)
      assert length(records) == 3
    end

    test "list_consumer_records_for_consumer/2 respects order_by parameter" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id)

      [first, second] = Consumers.list_consumer_records_for_consumer(consumer.id, order_by: [asc: :id])
      assert first.id == record1.id
      assert second.id == record2.id

      [first, second] = Consumers.list_consumer_records_for_consumer(consumer.id, order_by: [desc: :id])
      assert first.id == record2.id
      assert second.id == record1.id
    end

    test "ack_messages/2 deletes non-pending_redelivery records and marks pending_redelivery as available" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :pending_redelivery)
      record3 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      record4 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :pending_redelivery)

      :ok = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id, record3.ack_id])

      assert Repo.get_by(ConsumerRecord, id: record1.id) == nil
      assert Repo.get_by(ConsumerRecord, id: record3.id) == nil

      updated_record2 = Consumers.reload(record2)
      assert updated_record2.state == :available

      # record4 should remain unchanged
      updated_record4 = Consumers.reload(record4)
      assert updated_record4.state == :pending_redelivery
    end

    test "nack_messages/2 marks consumer records as available and resets not_visible_until" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      future = DateTime.add(DateTime.utc_now(), 3600, :second)

      record1 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered, not_visible_until: future)

      record2 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered, not_visible_until: future)

      record3 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered, not_visible_until: future)

      :ok = Consumers.nack_messages(consumer, [record1.ack_id, record2.ack_id])

      updated_record1 = Consumers.reload(record1)
      updated_record2 = Consumers.reload(record2)
      updated_record3 = Consumers.reload(record3)

      assert is_nil(updated_record1.not_visible_until)
      assert is_nil(updated_record2.not_visible_until)
      assert updated_record3.state == :delivered
      assert updated_record3.not_visible_until == future
    end
  end

  describe "insert_consumer_records/1" do
    test "inserts a single new record" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      record_attrs = ConsumersFactory.consumer_record_attrs(consumer_id: consumer.id, state: :delivered)

      assert {:ok, 1} = Consumers.insert_consumer_records([record_attrs])

      inserted_record = Repo.get_by(ConsumerRecord, consumer_id: consumer.id)
      assert inserted_record.consumer_id == record_attrs.consumer_id
      assert inserted_record.commit_lsn == record_attrs.commit_lsn
      assert inserted_record.record_pks == record_attrs.record_pks
      assert inserted_record.table_oid == record_attrs.table_oid
      assert inserted_record.state == record_attrs.state
      assert inserted_record.ack_id
      assert inserted_record.deliver_count == record_attrs.deliver_count
      assert inserted_record.last_delivered_at == record_attrs.last_delivered_at
      assert inserted_record.not_visible_until == record_attrs.not_visible_until
      assert %DateTime{} = inserted_record.inserted_at
      assert %DateTime{} = inserted_record.updated_at
    end

    test "inserts multiple new records" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      records = Enum.map(1..3, fn _ -> ConsumersFactory.consumer_record_attrs(consumer_id: consumer.id) end)

      assert {:ok, 3} = Consumers.insert_consumer_records(records)

      inserted_records = Repo.all(ConsumerRecord)
      assert length(inserted_records) == 3
    end

    test "upserts a record that already exists" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      existing_record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)

      updated_attrs = %{
        consumer_id: existing_record.consumer_id,
        record_pks: existing_record.record_pks,
        table_oid: existing_record.table_oid,
        commit_lsn: existing_record.commit_lsn + 1,
        state: :available
      }

      assert {:ok, 1} = Consumers.insert_consumer_records([updated_attrs])

      updated_record = Consumers.reload(existing_record)
      assert updated_record.commit_lsn == updated_attrs.commit_lsn
      assert updated_record.state == :available
    end

    test "upserts multiple records, some new and some existing" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      existing_record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id)
      new_record = ConsumersFactory.consumer_record_attrs(consumer_id: consumer.id)

      updated_attrs = %{
        consumer_id: existing_record.consumer_id,
        record_pks: existing_record.record_pks,
        table_oid: existing_record.table_oid,
        commit_lsn: existing_record.commit_lsn + 1,
        state: :available
      }

      assert {:ok, 2} = Consumers.insert_consumer_records([updated_attrs, new_record])

      updated_existing = Consumers.reload(existing_record)
      assert updated_existing.commit_lsn == updated_attrs.commit_lsn

      assert Repo.aggregate(ConsumerRecord, :count, :id) == 2
    end

    test "state transitions" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)

      records = [
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available),
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered),
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :pending_redelivery),
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :acked)
      ]

      update_attrs =
        Enum.map(records, fn record ->
          %{
            consumer_id: record.consumer_id,
            record_pks: record.record_pks,
            table_oid: record.table_oid,
            commit_lsn: record.commit_lsn + 1,
            state: :available
          }
        end)

      assert {:ok, 4} = Consumers.insert_consumer_records(update_attrs)

      updated_records = Enum.map(records, &Consumers.reload/1)

      assert Enum.map(updated_records, & &1.state) == [
               :available,
               :pending_redelivery,
               :pending_redelivery,
               :available
             ]
    end
  end

  describe "delete_consumer_records/1" do
    test "deletes matching records" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["1"])
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["2"])
      record3 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["3"])

      assert {:ok, 2} = Consumers.delete_consumer_records([record1, record2])

      assert Repo.get_by(ConsumerRecord, id: record1.id) == nil
      assert Repo.get_by(ConsumerRecord, id: record2.id) == nil
      assert Repo.get_by(ConsumerRecord, id: record3.id) != nil
    end

    test "doesn't delete records with non-matching consumer_id or table_oid" do
      consumer1 = ConsumersFactory.insert_consumer!(message_kind: :record)
      consumer2 = ConsumersFactory.insert_consumer!(message_kind: :record)
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer1.id, table_oid: 1000, record_pks: ["1"])
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer2.id, table_oid: 1000, record_pks: ["2"])

      assert {:ok, 0} = Consumers.delete_consumer_records([%{record1 | consumer_id: consumer2.id}])
      assert {:ok, 0} = Consumers.delete_consumer_records([%{record1 | table_oid: 1001}])

      assert Repo.get_by(ConsumerRecord, id: record1.id) != nil
      assert Repo.get_by(ConsumerRecord, id: record2.id) != nil
    end

    test "deletes records with multi-column primary keys respecting order" do
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record)

      record1 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["1", "a"])

      record1_other_table =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 2000, record_pks: ["1", "a"])

      record2 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["a", "1"])

      record3 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["2", "b"])

      record3_other_table =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 2000, record_pks: ["2", "b"])

      record4 =
        ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, table_oid: 1000, record_pks: ["b", "2"])

      assert {:ok, 3} =
               Consumers.delete_consumer_records([
                 record1,
                 record2,
                 record3_other_table
               ])

      refute Repo.get_by(ConsumerRecord, id: record1.id)
      refute Repo.get_by(ConsumerRecord, id: record2.id)
      refute Repo.get_by(ConsumerRecord, id: record3_other_table.id)
      assert Repo.get_by(ConsumerRecord, id: record3.id)
      assert Repo.get_by(ConsumerRecord, id: record4.id)
      assert Repo.get_by(ConsumerRecord, id: record1_other_table.id)
    end

    test "deletes nothing when given an empty list" do
      assert {:ok, 0} = Consumers.delete_consumer_records([])
    end
  end
end
