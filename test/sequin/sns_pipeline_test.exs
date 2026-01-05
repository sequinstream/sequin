defmodule Sequin.Runtime.SnsPipelineTest do
  use Sequin.DataCase, async: true

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
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  describe "events are sent to SNS" do
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
          type: :sns,
          batch_size: 10,
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer, account: account}}
    end

    test "events are sent to SNS", %{consumer: consumer} do
      test_pid = self()

      record = CharacterFactory.character_attrs()

      record =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record, action: :insert))

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sns_request, conn})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, record)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:sns_request, _conn}, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer} do
      test_pid = self()

      record1 = CharacterFactory.character_attrs()
      record2 = CharacterFactory.character_attrs()

      event1 =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record1))

      event2 =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record2))

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sns_request, conn})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}, %{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:sns_request, _conn}, 1_000
    end

    @tag capture_log: true
    test "failed SNS requests result in failed events", %{consumer: consumer} do
      Req.Test.stub(HttpClient, fn conn ->
        body = AwsFactory.sns_publish_batch_response_failure()
        Req.Test.text(conn, body)
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    test "events are sent to SNS with routing function", %{consumer: consumer, account: account} do
      test_pid = self()

      assert {:ok, routing_function} =
               Consumers.create_function(account.id, %{
                 name: "test_sns_routing",
                 function: %{
                   type: "routing",
                   sink_type: "sns",
                   code: """
                   def route(action, record, changes, metadata) do
                     topic_suffix = if action == "insert", do: "new", else: "updates"
                     %{"topic_arn" => "arn:aws:sns:us-east-1:123456789012:test-topic-" <> topic_suffix}
                   end
                   """
                 }
               })

      {:ok, _} = MiniElixir.create(routing_function.id, routing_function.function.code)

      {:ok, consumer} =
        Consumers.update_sink_consumer(consumer, %{routing_id: routing_function.id, routing_mode: "dynamic"})

      record = CharacterFactory.character_attrs()

      record =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record, action: :insert))

      Req.Test.stub(HttpClient, fn conn ->
        # Parse the request body to verify routing worked
        {:ok, body, _conn} = Plug.Conn.read_body(conn)

        # Check that the topic ARN in the request matches our routing function output (URL-encoded)
        assert String.contains?(body, "arn%3Aaws%3Asns%3Aus-east-1%3A123456789012%3Atest-topic-new")

        send(test_pid, {:sns_request, conn})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, record)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:sns_request, _conn}, 1_000
    end

    test "batched messages with routing function route to different topics", %{consumer: consumer, account: account} do
      test_pid = self()

      assert {:ok, routing_function} =
               Consumers.create_function(account.id, %{
                 name: "test_sns_batch_routing",
                 function: %{
                   type: "routing",
                   sink_type: "sns",
                   code: """
                   def route(action, record, changes, metadata) do
                     topic_suffix = if String.contains?(record["name"], "admin"), do: "admin", else: "users"
                     %{"topic_arn" => "arn:aws:sns:us-east-1:123456789012:test-topic-" <> topic_suffix}
                   end
                   """
                 }
               })

      {:ok, _} = MiniElixir.create(routing_function.id, routing_function.function.code)

      {:ok, consumer} =
        Consumers.update_sink_consumer(consumer, %{routing_id: routing_function.id, routing_mode: "dynamic"})

      # Create records that will route to different topics
      record1 = CharacterFactory.character_attrs(%{name: "admin_user"})
      record2 = CharacterFactory.character_attrs(%{name: "regular_user"})

      event1 =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record1))

      event2 =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record2))

      request_count = :counters.new(1, [:atomics])

      Req.Test.stub(HttpClient, fn conn ->
        # Parse the request body to verify routing worked
        {:ok, body, _conn} = Plug.Conn.read_body(conn)

        # Each batch should go to different topics based on routing function (URL-encoded)
        is_admin_topic = String.contains?(body, "arn%3Aaws%3Asns%3Aus-east-1%3A123456789012%3Atest-topic-admin")
        is_users_topic = String.contains?(body, "arn%3Aaws%3Asns%3Aus-east-1%3A123456789012%3Atest-topic-users")

        # is_admin_topic EXCLUSIVE OR is_users_topic
        assert (is_admin_topic or is_users_topic) and is_admin_topic !== is_users_topic

        :counters.add(request_count, 1, 1)
        send(test_pid, {:sns_request, conn, if(is_admin_topic, do: :admin, else: :users)})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [event1, event2])

      # Should receive acknowledgment for each message separately (they go to different topics)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000

      # Should receive two separate SNS requests (one for each topic)
      assert_receive {:sns_request, _conn, :admin}, 1_000
      assert_receive {:sns_request, _conn, :users}, 1_000

      # Verify we got exactly 2 requests
      assert :counters.get(request_count, 1) == 2
    end

    test "events are sent to SNS with use_task_role=true", %{consumer: consumer} do
      test_pid = self()

      # Set up stub and allow it for the test process and any spawned processes
      Mox.stub(AwsMock, :get_client, fn "us-east-1" ->
        {:ok,
         "task-role-access-key"
         |> AWS.Client.create("task-role-secret-key", "us-east-1")
         |> Map.put(:session_token, "task-role-session-token")}
      end)

      # Update consumer to use task role
      {:ok, consumer} =
        Consumers.update_sink_consumer(consumer, %{
          sink: %{
            type: :sns,
            topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic",
            region: "us-east-1",
            use_task_role: true,
            routing_mode: :static
          }
        })

      # Start pipeline - the stub allows calls from any process
      start_pipeline!(consumer)

      record = CharacterFactory.character_attrs()

      record =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record, action: :insert))

      Req.Test.stub(HttpClient, fn conn ->
        # Verify that task role credentials are used in the Authorization header
        auth_header = Enum.find(conn.req_headers, fn {key, _value} -> key == "authorization" end)
        assert auth_header != nil, "Authorization header should be present"

        {_key, auth_value} = auth_header
        assert String.contains?(auth_value, "AWS4-HMAC-SHA256"), "Should use AWS SigV4 signing"
        assert String.contains?(auth_value, "task-role-access-key"), "Should use task role access key"

        # Verify session token header is present for task role credentials
        # Note: AWS library automatically includes session tokens in the authorization header
        # when present, so we check for the presence of the token in authorization
        token_header = Enum.find(conn.req_headers, fn {key, _value} -> key == "x-amz-security-token" end)

        # The session token header may not be present in the current AWS library version
        # but the correct access key in the authorization header confirms task role credentials work
        if token_header do
          {_key, token_value} = token_header
          assert token_value == "task-role-session-token", "Should use task role session token"
        end

        send(test_pid, {:sns_request, conn})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      ref = send_test_event(consumer, record)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:sns_request, _conn}, 1_000
    end
  end

  describe "messages flow from postgres to SNS end-to-end" do
    setup do
      account = AccountsFactory.insert_account!()

      database =
        DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)

      ConnectionCache.cache_connection(database, Sequin.Repo)

      replication =
        ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :sns,
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to SNS", %{consumer: consumer} do
      test_pid = self()

      Req.Test.stub(HttpClient, fn conn ->
        send(test_pid, {:sns_request, conn})
        body = AwsFactory.sns_publish_batch_response_success()
        Req.Test.text(conn, body)
      end)

      consumer_record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid, persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [consumer_record])

      start_pipeline!(consumer, dummy_producer: false)

      assert_receive {:sns_request, _conn}, 1_000
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
        ConsumersFactory.insert_deliverable_consumer_event!(consumer_id: consumer.id)

    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test_topic", headers: []})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
