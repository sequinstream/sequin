defmodule Sequin.ConsumersRuntime.RedisPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisSink
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.ConsumersRuntime.RedisPipeline
  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Sinks.RedisMock
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed

  describe "events are sent to Redis" do
    setup do
      account = AccountsFactory.insert_account!()

      postgres_database =
        DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)

      replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: postgres_database.id
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          postgres_database_id: postgres_database.id,
          account_id: account.id,
          table_oid: Character.table_oid()
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :redis,
          message_kind: :record,
          batch_size: 10,
          sequence_filter: ConsumersFactory.sequence_filter_attrs(group_column_attnums: [1]),
          sequence_id: sequence.id,
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "events are sent to Redis", %{consumer: consumer} do
      test_pid = self()

      record = CharacterFactory.character_attrs()

      record =
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record))

      Mox.expect(RedisMock, :send_messages, fn sink, redis_messages ->
        send(test_pid, {:redis_request, sink, redis_messages})
        :ok
      end)

      start_pipeline!(consumer)

      ref = send_test_events(consumer, [record])
      assert_receive {:ack, ^ref, [%{data: [%ConsumerRecord{}]}], []}, 1_000
      assert_receive {:redis_request, %RedisSink{}, [%ConsumerRecordData{}]}, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer} do
      test_pid = self()

      record1 = CharacterFactory.character_attrs()
      record2 = CharacterFactory.character_attrs()

      event1 =
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record1))

      event2 =
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record2))

      Mox.expect(RedisMock, :send_messages, fn sink, redis_messages ->
        send(test_pid, {:redis_request, sink, redis_messages})
        :ok
      end)

      start_pipeline!(consumer)

      ref = send_test_events(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: [%ConsumerRecord{}, %ConsumerRecord{}]}], []}, 1_000
      assert_receive {:redis_request, _sink, _redis_messages}, 1_000
    end

    @tag capture_log: true
    test "failed Redis requests result in failed events", %{consumer: consumer} do
      Mox.expect(Sequin.Sinks.RedisMock, :send_messages, fn _sink, _redis_messages ->
        {:error, Sequin.Error.service(service: :redis, code: "batch_error", message: "Redis batch send failed")}
      end)

      start_pipeline!(consumer)

      ref = send_test_events(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end
  end

  describe "messages flow from postgres to Redis end-to-end" do
    setup do
      account = AccountsFactory.insert_account!()

      database = DatabasesFactory.configured_postgres_database(account_id: account.id)
      ConnectionCache.cache_connection(database, Sequin.Repo)

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          postgres_database: database,
          table_oid: CharacterDetailed.table_oid()
        )

      replication = ReplicationFactory.postgres_replication(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.sink_consumer(
          id: UUID.uuid4(),
          account_id: account.id,
          type: :redis,
          replication_slot_id: replication.id,
          sequence_id: sequence.id,
          sequence: sequence,
          message_kind: :record,
          postgres_database: database
        )

      :ok = Consumers.create_consumer_partition(consumer)

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to Redis", %{consumer: consumer} do
      test_pid = self()

      Mox.expect(Sequin.Sinks.RedisMock, :send_messages, fn sink, redis_messages ->
        send(test_pid, {:redis_request, sink, redis_messages})
        :ok
      end)

      consumer_record = ConsumersFactory.deliverable_consumer_record(consumer_id: consumer.id)

      start_supervised!({SlotMessageStore, [consumer: consumer, test_pid: test_pid, skip_load_from_postgres?: true]})
      SlotMessageStore.put_messages(consumer.id, [consumer_record])

      start_supervised!({RedisPipeline, [consumer: consumer, test_pid: test_pid]})

      assert_receive {:redis_request, _sink, _redis_messages}, 5_000
      assert_receive {ConsumerProducer, :ack_finished, [_successful], []}, 5_000
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {RedisPipeline,
       [
         consumer: consumer,
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )
  end

  defp send_test_events(consumer, events \\ nil) do
    events =
      events ||
        [ConsumersFactory.insert_deliverable_consumer_record!(consumer_id: consumer.id, source_record: :character)]

    Broadway.test_message(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    RedisPipeline.via_tuple(consumer.id)
  end
end
