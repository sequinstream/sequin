defmodule Sequin.ConsumersRuntime.SqsPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.ConsumersRuntime.SqsPipeline
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Test.Support.Models.Character
  alias Sequin.Test.Support.Models.CharacterDetailed

  describe "events are sent to SQS" do
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
        ConsumersFactory.insert_destination_consumer!(
          account_id: account.id,
          type: :sqs,
          message_kind: :record,
          batch_size: 10,
          sequence_filter: ConsumersFactory.sequence_filter_attrs(group_column_attnums: [1]),
          sequence_id: sequence.id,
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

      ref = send_test_events(consumer, [record])
      assert_receive {:ack, ^ref, [%{data: [%ConsumerRecord{}]}], []}, 1_000
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

      ref = send_test_events(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: [%ConsumerRecord{}, %ConsumerRecord{}]}], []}, 1_000
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

      ref = send_test_events(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end
  end

  describe "messages flow from postgres to SQS end-to-end" do
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
        ConsumersFactory.destination_consumer(
          id: UUID.uuid4(),
          account_id: account.id,
          type: :sqs,
          replication_slot_id: replication.id,
          sequence_id: sequence.id,
          sequence: sequence,
          message_kind: :record,
          postgres_database: database
        )

      :ok = Consumers.create_consumer_partition(consumer)

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to SQS", %{consumer: consumer} do
      test_pid = self()

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sqs_request, conn})
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "msg1"}], "Failed" => []})
      end)

      consumer_record =
        ConsumersFactory.insert_deliverable_consumer_record!(
          consumer_id: consumer.id,
          source_record: :character_detailed
        )

      start_supervised!({SqsPipeline, [consumer: consumer, test_pid: test_pid]})

      assert_receive {:sqs_request, _conn}, 5_000
      assert_receive {ConsumerProducer, :ack_finished, [_successful], []}, 5_000

      refute Consumers.reload(consumer_record)
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SqsPipeline,
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
    SqsPipeline.via_tuple(consumer.id)
  end
end
