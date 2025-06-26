defmodule Sequin.Runtime.SqsPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  describe "events are sent to SQS" do
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
          type: :sqs,
          message_kind: :record,
          batch_size: 10,
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "events are sent to SQS", %{consumer: consumer} do
      test_pid = self()

      record = CharacterFactory.character_attrs()

      record =
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record))

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sqs_request, conn})
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "msg1"}], "Failed" => []})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, record)
      assert_receive {:ack, ^ref, [%{data: %ConsumerRecord{}}], []}, 1_000
      assert_receive {:sqs_request, _conn}, 1_000
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

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sqs_request, conn})

        Req.Test.json(conn, %{
          "Successful" => [
            %{"Id" => "1", "MessageId" => "msg1"},
            %{"Id" => "2", "MessageId" => "msg2"}
          ],
          "Failed" => []
        })
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: %ConsumerRecord{}}, %{data: %ConsumerRecord{}}], []}, 1_000
      assert_receive {:sqs_request, _conn}, 1_000
    end

    @tag capture_log: true
    test "failed SQS requests result in failed events", %{consumer: consumer} do
      Req.Test.stub(HttpClient, fn conn ->
        Req.Test.json(conn, %{
          "Failed" => [
            %{
              "Id" => "1",
              "Code" => "InternalError",
              "Message" => "Internal Error occurred"
            }
          ],
          "Successful" => []
        })
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end
  end

  describe "messages flow from postgres to SQS end-to-end" do
    setup do
      account = AccountsFactory.insert_account!()

      database =
        DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)

      ConnectionCache.cache_connection(database, Sequin.Repo)

      replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: database.id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :sqs,
          replication_slot_id: replication.id,
          message_kind: :record
        )

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to SQS", %{consumer: consumer} do
      test_pid = self()

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sqs_request, conn})
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "msg1"}], "Failed" => []})
      end)

      consumer_record = ConsumersFactory.deliverable_consumer_record(consumer_id: consumer.id)

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid, persisted_mode?: false]}
      )

      message_batch = ConsumersFactory.consumer_message_batch([consumer_record])
      SlotMessageStore.put_message_batch(consumer, message_batch)

      start_pipeline!(consumer, dummy_producer: false)

      assert_receive {:sqs_request, _conn}, 1_000
      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000
    end
  end

  defp start_pipeline!(consumer, opts \\ []) do
    {dummy_producer, _opts} = Keyword.pop(opts, :dummy_producer, true)

    opts = [
      consumer_id: consumer.id,
      test_pid: self()
    ]

    opts =
      if dummy_producer do
        Keyword.put(opts, :producer, Broadway.DummyProducer)
      else
        opts
      end

    start_supervised!({SinkPipeline, opts})
  end

  defp send_test_event(consumer, event \\ nil) do
    event =
      event ||
        ConsumersFactory.insert_deliverable_consumer_record!(consumer_id: consumer.id, source_record: :character)

    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test_topic", headers: []})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
