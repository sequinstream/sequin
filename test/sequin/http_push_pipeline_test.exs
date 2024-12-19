defmodule Sequin.ConsumersRuntime.HttpPushPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.ConsumersRuntime.HttpPushPipeline
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.TestEventLogFactory
  alias Sequin.Test.Support.Models.CharacterDetailed

  describe "events are sent to the HTTP endpoint" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          message_kind: :event
        )

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    test "events are sent to the HTTP endpoint", %{consumer: consumer, http_endpoint: http_endpoint} do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        %{"data" => [event_json]} = Jason.decode!(req.body)

        assert_maps_equal(
          event_json,
          %{
            "record" => event.data.record,
            "changes" => nil,
            "action" => "insert"
          },
          ["record", "changes", "action"]
        )

        assert_maps_equal(
          event_json["metadata"],
          %{
            "table_name" => event.data.metadata.table_name,
            "table_schema" => event.data.metadata.table_schema,
            "commit_timestamp" => DateTime.to_iso8601(event.data.metadata.commit_timestamp)
          },
          ["table", "schema", "commit_timestamp"]
        )

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_events(consumer, [event])
      assert_receive {:ack, ^ref, [%{data: [%{data: %{action: :insert}}]}], []}, 1_000
      assert_receive :sent, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer, http_endpoint: http_endpoint} do
      test_pid = self()

      # Create multiple events
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :update)

      # Set batch size to 2
      consumer = %{consumer | batch_size: 2}

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        %{"data" => [json1, json2]} = Jason.decode!(req.body)

        assert json1["action"] == "insert"
        assert json2["action"] == "update"

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_events(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: [%{data: %{action: :insert}}, %{data: %{action: :update}}]}], []}, 1_000
      assert_receive :sent, 1_000
    end

    test "legacy_event_singleton_transform sends unwrapped single messages", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        json = Jason.decode!(req.body)

        # Should NOT be wrapped in a list
        refute is_list(json)
        assert json["action"] == "insert"

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      # Start pipeline with legacy_event_singleton_transform enabled
      start_supervised!(
        {HttpPushPipeline,
         [
           consumer: consumer,
           req_opts: [adapter: adapter],
           test_pid: test_pid,
           features: [legacy_event_singleton_transform: true]
         ]}
      )

      ref = send_test_events(consumer, [event])
      assert_receive {:ack, ^ref, [%{data: [%{data: %{action: :insert}}]}], []}, 1_000
      assert_receive :sent, 1_000
    end

    @tag capture_log: true
    test "a non-200 response results in a failed event", %{consumer: consumer} do
      adapter = fn req ->
        {req, Req.Response.new(status: 500)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_events(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    @tag capture_log: true
    test "a transport error/timeout results in a failed event", %{consumer: consumer} do
      adapter = fn req ->
        {req, %Mint.TransportError{reason: :timeout}}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_events(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    @tag capture_log: true
    test "request fails with a short receive_timeout", %{consumer: consumer} do
      ack_wait_ms = consumer.ack_wait_ms

      adapter = fn %Req.Request{} = req ->
        assert req.options.receive_timeout == ack_wait_ms
        {req, %Mint.TransportError{reason: :timeout}}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_events(consumer)

      assert_receive {:ack, ^ref, [], [_failed]}, 2000
    end
  end

  describe "messages flow from postgres to http end-to-end" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.http_endpoint(account_id: account.id, id: UUID.uuid4())

      database = DatabasesFactory.postgres_database(account_id: account.id)
      sequence = DatabasesFactory.sequence(postgres_database_id: database.id)
      replication = ReplicationFactory.postgres_replication(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.sink_consumer(
          id: UUID.uuid4(),
          account_id: account.id,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id, http_endpoint: http_endpoint},
          replication_slot_id: replication.id,
          sequence_id: sequence.id,
          message_kind: :event
        )

      :ok = Consumers.create_consumer_partition(consumer)

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to http endpoint", %{consumer: consumer} do
      test_pid = self()

      # Mock the HTTP adapter
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      # Insert a consumer record
      record = %{
        "id" => Faker.UUID.v4(),
        "name" => "John Doe",
        "email" => "john@example.com"
      }

      consumer_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          record_pks: [record["id"]],
          data:
            ConsumersFactory.consumer_record_data(
              record: record,
              metadata: %{
                table_schema: "public",
                table_name: "users",
                commit_timestamp: DateTime.utc_now()
              }
            )
        )

      # Start the pipeline
      start_supervised!({HttpPushPipeline, [consumer: consumer, req_opts: [adapter: adapter], test_pid: test_pid]})

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 1_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(consumer.sink.http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      assert json["record"] == record
      assert json["metadata"]["table_name"] == "users"
      assert json["metadata"]["table_schema"] == "public"

      assert_receive {ConsumerProducer, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      refute Consumers.reload(consumer_event)
    end

    @tag capture_log: true
    test "failed messages are nacked with correct not_visible_until using exponential backoff with jitter", %{
      consumer: consumer
    } do
      test_pid = self()
      deliver_count1 = 3
      deliver_count2 = 5

      # Insert consumer_events with specific deliver_counts
      event1 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          action: :insert,
          deliver_count: deliver_count1,
          not_visible_until: nil
        )

      event2 =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          action: :insert,
          deliver_count: deliver_count2,
          not_visible_until: nil
        )

      # Capture the current time before the push attempt
      initial_time = DateTime.utc_now()

      # Define the adapter to simulate a failure
      adapter = fn %Req.Request{} = req ->
        send(test_pid, :sent)
        {req, Req.Response.new(status: 500)}
      end

      # Start the pipeline with the failing adapter
      start_supervised!({HttpPushPipeline, [consumer: consumer, req_opts: [adapter: adapter], test_pid: test_pid]})

      # Send the test events
      send_test_events(consumer, [event1])
      send_test_events(consumer, [event2])

      # Assert that the ack receives the failed events
      assert_receive :sent, 1_000
      assert_receive :sent, 1_000
      assert_receive {ConsumerProducer, :ack_finished, [], [_failed1]}, 2_000
      assert_receive {ConsumerProducer, :ack_finished, [], [_failed2]}, 2_000

      # Reload the events from the database to check not_visible_until
      updated_event1 = Consumers.reload(event1)
      updated_event2 = Consumers.reload(event2)

      assert updated_event1.not_visible_until
      assert updated_event2.not_visible_until

      refute updated_event1.not_visible_until == event1.not_visible_until
      refute updated_event2.not_visible_until == event2.not_visible_until

      # Calculate the difference between initial_time and not_visible_until
      diff_ms1 = DateTime.diff(updated_event1.not_visible_until, initial_time, :millisecond)
      diff_ms2 = DateTime.diff(updated_event2.not_visible_until, initial_time, :millisecond)

      refute updated_event1.not_visible_until == updated_event2.not_visible_until

      # Ensure the backoff is increasing
      assert diff_ms2 > diff_ms1
    end
  end

  describe "messages flow from postgres to http end-to-end for message_kind=record" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.http_endpoint(account_id: account.id, id: UUID.uuid4())

      database = DatabasesFactory.postgres_database(account_id: account.id, tables: :character_tables)
      ConnectionCache.cache_connection(database, Repo)
      sequence = DatabasesFactory.sequence(postgres_database_id: database.id)
      replication = ReplicationFactory.postgres_replication(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.sink_consumer(
          id: UUID.uuid4(),
          account_id: account.id,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id, http_endpoint: http_endpoint},
          replication_slot_id: replication.id,
          postgres_database: database,
          postgres_database_id: database.id,
          sequence_id: sequence.id,
          message_kind: :record
        )

      :ok = Consumers.create_consumer_partition(consumer)

      {:ok, %{consumer: consumer}}
    end

    test "messages are sent from postgres to http endpoint", %{consumer: consumer} do
      test_pid = self()

      # Mock the HTTP adapter
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      # Insert a detailed character record
      character = CharacterFactory.insert_character_detailed!()

      # Insert a consumer record referencing the character
      consumer_record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          record_pks: [character.id],
          table_oid: CharacterDetailed.table_oid(),
          state: :available
        )

      # Start the pipeline
      start_supervised!({HttpPushPipeline, [consumer: consumer, req_opts: [adapter: adapter], test_pid: test_pid]})

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 5_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(consumer.sink.http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      # Assert the record data matches
      assert json["record"]["id"] == character.id
      assert json["record"]["name"] == character.name
      assert json["record"]["age"] == character.age
      assert json["record"]["height"] == character.height
      assert json["record"]["is_hero"] == character.is_hero
      assert json["record"]["biography"] == character.biography
      assert json["record"]["avatar"] == "\\x" <> Base.encode16(character.avatar, case: :lower)

      # Assert metadata
      assert json["metadata"]["table_name"] == "characters_detailed"
      assert json["metadata"]["table_schema"] == "public"

      assert_receive {ConsumerProducer, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      refute Consumers.reload(consumer_record)
    end

    test "legacy event transform is applied when feature flag is enabled", %{consumer: consumer} do
      test_pid = self()

      # Mock the HTTP adapter
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      source_record = TestEventLogFactory.insert_test_event_log!(%{}, repo: Repo)

      consumer_event =
        ConsumersFactory.insert_deliverable_consumer_record!(
          consumer_id: consumer.id,
          source_record: source_record
        )

      # Start the pipeline with legacy_event_transform feature enabled
      start_supervised!(
        {HttpPushPipeline,
         [
           consumer: consumer,
           req_opts: [adapter: adapter],
           test_pid: test_pid,
           features: [legacy_event_transform: true]
         ]}
      )

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 5_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(consumer.sink.http_endpoint)
      json = Jason.decode!(req.body)

      # Assert the transformed structure
      assert json["record"] == source_record.record

      assert json["metadata"]["table_name"] == source_record.source_table_name
      assert json["metadata"]["table_schema"] == source_record.source_table_schema
      assert json["metadata"]["consumer"]["id"] == consumer.id
      assert json["metadata"]["consumer"]["name"] == consumer.name
      assert json["action"] == source_record.action
      assert json["changes"] == source_record.changes

      assert_receive {ConsumerProducer, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      refute Consumers.reload(consumer_event)
    end
  end

  defp start_pipeline!(consumer, adapter) do
    start_supervised!(
      {HttpPushPipeline,
       [
         consumer: consumer,
         req_opts: [adapter: adapter],
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )
  end

  defp send_test_events(consumer, events \\ nil) do
    events = events || [ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)]
    Broadway.test_message(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    HttpPushPipeline.via_tuple(consumer.id)
  end
end
