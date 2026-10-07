defmodule Sequin.Runtime.KinesisPipelineTaskRoleTest do
  # Not async: the refresh interval is set through the application env
  use Sequin.DataCase, async: false

  alias Sequin.Aws.HttpClient
  alias Sequin.AwsMock
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.AwsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.KinesisPipeline
  alias Sequin.Runtime.SinkPipeline

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
        batch_size: 10,
        replication_slot_id: replication.id
      )

    {:ok, consumer} =
      Consumers.update_sink_consumer(consumer, %{
        sink: %{
          type: :kinesis,
          stream_arn: "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
          region: "us-east-1",
          use_task_role: true,
          routing_mode: :static
        }
      })

    # Each call to get_client returns a new set of temporary credentials,
    # as aws_credentials does after refreshing an expiring token
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    Mox.stub(AwsMock, :get_client, fn "us-east-1" ->
      n = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})

      {:ok,
       "task-role-key-#{n}"
       |> AWS.Client.create("task-role-secret-#{n}", "us-east-1")
       |> Map.put(:session_token, "task-role-token-#{n}")}
    end)

    test_pid = self()

    Req.Test.stub(HttpClient, fn conn ->
      {_key, auth} = Enum.find(conn.req_headers, fn {key, _value} -> key == "authorization" end)
      [_, access_key] = Regex.run(~r/Credential=([^\/]+)\//, auth)
      send(test_pid, {:kinesis_access_key, access_key})
      Req.Test.json(conn, AwsFactory.kinesis_put_records_response_success())
    end)

    previous_env = Application.get_env(:sequin, KinesisPipeline)
    on_exit(fn -> restore_env(previous_env) end)

    {:ok, %{consumer: consumer}}
  end

  test "rebuilds the client with fresh task role credentials before delivering", %{consumer: consumer} do
    Application.put_env(:sequin, KinesisPipeline, task_role_client_refresh_seconds: 0)

    start_pipeline!(consumer)

    ref1 = send_test_event(consumer)
    assert_receive {:ack, ^ref1, [%{data: %ConsumerEvent{}}], []}, 1_000
    assert_receive {:kinesis_access_key, key1}, 1_000

    ref2 = send_test_event(consumer)
    assert_receive {:ack, ^ref2, [%{data: %ConsumerEvent{}}], []}, 1_000
    assert_receive {:kinesis_access_key, key2}, 1_000

    # The credentials obtained on init are replaced on every batch
    assert key1 != key2
  end

  test "delivers with task role credentials while the client is still fresh", %{consumer: consumer} do
    Application.put_env(:sequin, KinesisPipeline, task_role_client_refresh_seconds: 3600)

    start_pipeline!(consumer)

    ref = send_test_batch(consumer, [deliverable_event(consumer), deliverable_event(consumer)])
    assert_receive {:ack, ^ref, [_, _], []}, 1_000
    assert_receive {:kinesis_access_key, "task-role-key-" <> _}, 1_000
  end

  defp restore_env(nil), do: Application.delete_env(:sequin, KinesisPipeline)
  defp restore_env(env), do: Application.put_env(:sequin, KinesisPipeline, env)

  defp deliverable_event(consumer) do
    [consumer_id: consumer.id]
    |> ConsumersFactory.insert_deliverable_consumer_event!()
    |> Map.put(:data, ConsumersFactory.consumer_event_data(record: CharacterFactory.character_attrs()))
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

  defp send_test_event(consumer) do
    Broadway.test_message(broadway(consumer), deliverable_event(consumer), metadata: %{topic: "test", headers: []})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events, metadata: %{topic: "test", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
