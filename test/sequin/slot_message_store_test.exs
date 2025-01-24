defmodule Sequin.SlotMessageStoreTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.TestSupport.Models.Character

  describe "Event Sink Consumer - message handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts, delivers, nacks, and acks messages", %{consumer: consumer} do
      # Create test events
      events = [
        ConsumersFactory.consumer_event(%{
          consumer_id: consumer.id,
          record_pks: ["1"],
          deliver_count: 0
        }),
        ConsumersFactory.consumer_event(%{
          consumer_id: consumer.id,
          record_pks: ["2"],
          deliver_count: 0
        })
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, events)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2)
      assert length(delivered) == 2
      assert Enum.all?(delivered, &(&1.state == :delivered))
      assert Enum.all?(delivered, &(&1.deliver_count == 1))

      # For acks
      ack_ids = Enum.map(delivered, & &1.ack_id)
      # For nacks
      ack_ids_with_not_visible_until = Map.new(ack_ids, &{&1, DateTime.utc_now()})

      # Nack messages
      {:ok, 2} = SlotMessageStore.nack(consumer.id, ack_ids_with_not_visible_until)
      # Produce messages, both are re-delivered
      {:ok, redelivered} = SlotMessageStore.produce(consumer.id, 2)
      assert length(redelivered) == 2
      assert Enum.all?(redelivered, &(&1.state == :delivered))
      assert Enum.all?(redelivered, &(&1.deliver_count == 2))

      # Acknowledge messages
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.ack(consumer, ack_ids)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2)
    end
  end

  describe "Record Sink Consumer - message handling" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)
      ConnectionCache.cache_connection(database, Sequin.Repo)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          message_kind: :record,
          postgres_database_id: database.id
        )

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts, delivers, nacks, and acks messages", %{consumer: consumer} do
      character_table_oid = Character.table_oid()
      character_1 = CharacterFactory.insert_character!()
      character_2 = CharacterFactory.insert_character!()

      # Create test records
      records = [
        ConsumersFactory.consumer_record(%{
          consumer_id: consumer.id,
          record_pks: [character_1.id],
          table_oid: character_table_oid,
          deliver_count: 0
        }),
        ConsumersFactory.consumer_record(%{
          consumer_id: consumer.id,
          record_pks: [character_2.id],
          table_oid: character_table_oid,
          deliver_count: 0
        })
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, records)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2)
      assert length(delivered) == 2
      assert Enum.all?(delivered, &(&1.state == :delivered))
      assert Enum.all?(delivered, &(&1.deliver_count == 1))

      # For acks
      ack_ids = Enum.map(delivered, & &1.ack_id)
      # For nacks
      ack_ids_with_not_visible_until = Map.new(ack_ids, &{&1, DateTime.utc_now()})

      # Nack messages
      {:ok, 2} = SlotMessageStore.nack(consumer.id, ack_ids_with_not_visible_until)
      # Produce messages, both are re-delivered
      {:ok, redelivered} = SlotMessageStore.produce(consumer.id, 2)
      assert length(redelivered) == 2
      assert Enum.all?(redelivered, &(&1.state == :delivered))
      assert Enum.all?(redelivered, &(&1.deliver_count == 2))

      # Acknowledge messages
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.ack(consumer, ack_ids)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2)
    end
  end
end
