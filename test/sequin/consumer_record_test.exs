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
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
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
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
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
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      Enum.each(1..5, fn _ -> ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id) end)

      records = Consumers.list_consumer_records_for_consumer(consumer.id, limit: 3)
      assert length(records) == 3
    end

    test "list_consumer_records_for_consumer/2 respects order_by parameter" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
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
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :pending_redelivery)
      record3 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)
      record4 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :pending_redelivery)

      assert {:ok, 3} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id, record3.ack_id])

      assert Repo.get_by(ConsumerRecord, id: record1.id) == nil
      assert Repo.get_by(ConsumerRecord, id: record3.id) == nil

      updated_record2 = Consumers.reload(record2)
      assert updated_record2.state == :available

      # record4 should remain unchanged
      updated_record4 = Consumers.reload(record4)
      assert updated_record4.state == :pending_redelivery
    end
  end
end
