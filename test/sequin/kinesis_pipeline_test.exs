defmodule Sequin.Runtime.KinesisPipelineTest do
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

  describe "events are sent to Kinesis" do
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
          type: :kinesis,
          message_kind: :record,
          batch_size: 10,
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "events are sent to Kinesis", %{consumer: consumer} do
      test_pid = self()

      record = CharacterFactory.character_attrs()

      event =
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record))

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:kinesis_request, conn})
        Req.Test.json(conn, %{"FailedRecordCount" => 0, "Records" => []})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %ConsumerRecord{}}], []}, 1_000
      assert_receive {:kinesis_request, _}, 1_000
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

  defp send_test_event(consumer, event) do
    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
