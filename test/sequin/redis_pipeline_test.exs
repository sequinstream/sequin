defmodule Sequin.Runtime.RedisPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Sinks.RedisMock

  describe "events are sent to Redis" do
    setup do
      account = AccountsFactory.insert_account!()

      postgres_database =
        DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)

      ConnectionCache.cache_connection(postgres_database, Sequin.Repo)

      replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: postgres_database.id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :redis_stream,
          message_kind: :record,
          batch_size: 10,
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

      ref = send_test_event(consumer, record)
      assert_receive {:ack, ^ref, [%{data: %ConsumerRecord{}}], []}, 1_000
      assert_receive {:redis_request, %SinkConsumer{sink: %RedisStreamSink{}}, [data]}, 1_000
      assert is_map(data)
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

      Mox.expect(RedisMock, :send_messages, 1, fn sink, redis_messages ->
        send(test_pid, {:redis_request, sink, redis_messages})
        :ok
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: %ConsumerRecord{}}, %{data: %ConsumerRecord{}}], []}, 1_000
      assert_receive {:redis_request, _sink, _redis_messages}, 1_000
    end

    @tag capture_log: true
    test "failed Redis requests result in failed events", %{consumer: consumer} do
      Mox.expect(Sequin.Sinks.RedisMock, :send_messages, fn _sink, _redis_messages ->
        {:error, Sequin.Error.service(service: :redis, code: "batch_error", message: "Redis batch send failed")}
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer)
      assert_receive({:ack, ^ref, [], [_failed]}, 2_000)
    end
  end

  describe "messages flow from postgres to Redis end-to-end" do
    setup do
      account = AccountsFactory.insert_account!()

      database =
        DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)

      ConnectionCache.cache_connection(database, Sequin.Repo)

      replication =
        ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          id: UUID.uuid4(),
          account_id: account.id,
          type: :redis_stream,
          replication_slot_id: replication.id,
          message_kind: :record,
          postgres_database: database
        )

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to Redis", %{consumer: consumer} do
      test_pid = self()

      Mox.expect(Sequin.Sinks.RedisMock, :send_messages, fn sink, redis_messages ->
        send(test_pid, {:redis_request, sink, redis_messages})
        :ok
      end)

      consumer_record = ConsumersFactory.deliverable_consumer_record(consumer_id: consumer.id)

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid, persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [consumer_record])

      start_supervised!({SinkPipeline, [consumer_id: consumer.id, test_pid: test_pid]})

      assert_receive {:redis_request, _sink, _redis_messages}, 5_000
      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer_id: consumer.id,
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )
  end

  defp send_test_event(consumer, event \\ nil) do
    events =
      event ||
        ConsumersFactory.insert_deliverable_consumer_record!(consumer_id: consumer.id, source_record: :character)

    Broadway.test_message(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
