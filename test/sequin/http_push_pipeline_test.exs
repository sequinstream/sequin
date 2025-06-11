defmodule Sequin.Runtime.HttpPushPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.TestSupport.Models.CharacterDetailed

  describe "events are sent to the HTTP endpoint" do
    setup ctx do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      batch = Map.get(ctx, :batch, true)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id, batch: batch},
          message_kind: :event,
          legacy_transform: ctx[:legacy_transform] || :none
        )

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    @tag legacy_transform: :none
    test "events are sent to the HTTP endpoint without transforms", %{consumer: consumer, http_endpoint: http_endpoint} do
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

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive :sent, 1_000
    end

    @tag legacy_transform: :record_only
    test "events are sent to the HTTP endpoint with record-only transform", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        %{"data" => [event_json]} = Jason.decode!(req.body)

        assert event_json == event.data.record

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive :sent, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer, http_endpoint: http_endpoint} do
      test_pid = self()

      # Create multiple events
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :update)

      # Set batch size to 2
      Consumers.update_sink_consumer(consumer, %{batch_size: 2})

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        %{"data" => data_list} = Jason.decode!(req.body)
        assert length(data_list) == 2

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_batch(consumer, [event1, event2])

      expected = [
        %{data: %{data: %{action: :insert}}},
        %{data: %{data: %{action: :update}}}
      ]

      assert_receive {:ack, ^ref, received, []}, 1_000

      assert_lists_equal(received, expected, fn a, b ->
        assert a.data.data.action == b.data.data.action
      end)

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
      start_pipeline!(consumer, adapter, features: [legacy_event_singleton_transform: true])

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive :sent, 1_000
    end

    test "when all messages are rejected due to idempotency, the pipeline does not invoke the adapter", %{
      consumer: consumer
    } do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      # Mark the message as already delivered
      wal_cursor = %{commit_lsn: event.commit_lsn, commit_idx: event.commit_idx, commit_timestamp: event.commit_timestamp}
      MessageLedgers.wal_cursors_delivered(consumer.id, [wal_cursor])

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      event = %ConsumerEvent{event | payload_size_bytes: 1000}
      SlotMessageStore.put_messages(consumer, [event])

      # Start the pipeline
      start_supervised!({SinkPipeline, [consumer_id: consumer.id, req_opts: [adapter: adapter], test_pid: test_pid]})

      # Verify that no HTTP request was made since the message was rejected
      refute_receive {:http_request, _req}, 200

      # Verify that the ack handler is never called
      refute_receive {SinkPipeline, :ack_finished, [], []}, 200

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end

    test "when some messages are rejected due to idempotency, the pipeline invokes the adapter for the remaining messages",
         %{
           consumer: consumer
         } do
      test_pid = self()
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :update)

      # Mark the first message as already delivered
      wal_cursor = %{
        commit_lsn: event1.commit_lsn,
        commit_idx: event1.commit_idx,
        commit_timestamp: event1.commit_timestamp
      }

      MessageLedgers.wal_cursors_delivered(consumer.id, [wal_cursor])

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      event1 = %ConsumerEvent{event1 | payload_size_bytes: 1000}
      event2 = %ConsumerEvent{event2 | payload_size_bytes: 1000}
      SlotMessageStore.put_messages(consumer, [event1, event2])

      # Start the pipeline
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Verify that the second message was sent
      assert_receive {:http_request, _req}, 1_000

      # Verify that the message was marked as already succeeded
      assert_receive {SinkPipeline, :ack_finished, [_ack_id], []}, 1_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end

    @tag batch: false
    test "events are sent to the HTTP endpoint with batch=false", %{
      consumer: consumer
    } do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      adapter = fn %Req.Request{} = req ->
        event_data_as_json = event.data |> Jason.encode!() |> Jason.decode!()
        assert Jason.decode!(req.body) == event_data_as_json

        send(test_pid, :sent)
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive :sent, 1_000
    end

    @tag capture_log: true
    test "a non-200 response results in a failed event", %{consumer: consumer} do
      adapter = fn req ->
        {req, Req.Response.new(status: 500)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_event(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    @tag capture_log: true
    test "a transport error/timeout results in a failed event", %{consumer: consumer} do
      adapter = fn req ->
        {req, %Mint.TransportError{reason: :timeout}}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_event(consumer)
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

      ref = send_test_event(consumer)

      assert_receive {:ack, ^ref, [], [_failed]}, 2000
    end
  end

  describe "messages flow from SlotMessageStore to http end-to-end" do
    setup ctx do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      replication =
        ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          id: UUID.uuid4(),
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          replication_slot_id: replication.id,
          message_kind: :event,
          legacy_transform: ctx[:legacy_transform] || :none
        )

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    @tag legacy_transform: :none
    test "messages are sent from postgres to http endpoint without transforms", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
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
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          record_pks: [record["id"]],
          data:
            ConsumersFactory.consumer_event_data(
              record: record,
              metadata: %ConsumerEventData.Metadata{
                database_name: "postgres",
                table_schema: "public",
                table_name: "users",
                commit_timestamp: DateTime.utc_now(),
                commit_lsn: Factory.unique_integer(),
                consumer: %ConsumerEventData.Metadata.Sink{
                  id: consumer.id,
                  name: consumer.name
                },
                transaction_annotations: nil
              }
            )
        )

      start_supervised({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]})
      SlotMessageStore.put_messages(consumer, [consumer_event])

      # Start the pipeline
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 1_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      assert json["record"] == record
      assert json["metadata"]["table_name"] == "users"
      assert json["metadata"]["table_schema"] == "public"

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end

    @tag legacy_transform: :record_only
    test "messages are sent from postgres to http endpoint with record-only transform", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
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
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          record_pks: [record["id"]],
          data:
            ConsumersFactory.consumer_event_data(
              record: record,
              metadata: %{
                database_name: "postgres",
                table_schema: "public",
                table_name: "users",
                commit_timestamp: DateTime.utc_now(),
                consumer: %{
                  id: consumer.id,
                  name: consumer.name
                }
              }
            )
        )

      start_supervised({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]})
      SlotMessageStore.put_messages(consumer, [consumer_event])

      # Start the pipeline
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 1_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      assert json["name"] == "John Doe"
      refute json["metadata"]

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
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
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :insert,
          deliver_count: deliver_count1
        )

      event2 =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :insert,
          deliver_count: deliver_count2
        )

      # Capture the current time before the push attempt
      initial_time = DateTime.utc_now()

      # Define the adapter to simulate a failure
      adapter = fn %Req.Request{} = req ->
        send(test_pid, :sent)
        {req, Req.Response.new(status: 500)}
      end

      start_supervised!({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self()]})

      expect_uuid4(fn -> event1.ack_id end)
      expect_uuid4(fn -> event2.ack_id end)

      SlotMessageStore.put_messages(consumer, [event1, event2])

      # Start the pipeline with the failing adapter
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Assert that the ack receives the failed events
      assert_receive :sent, 1_000
      assert_receive :sent, 1_000
      assert_receive {SinkPipeline, :ack_finished, [], [_failed1]}, 2_000
      assert_receive {SinkPipeline, :ack_finished, [], [_failed2]}, 2_000

      # Reload the events from the database to check not_visible_until
      messages = SlotMessageStore.peek_messages(consumer, 10)

      updated_event1 =
        Sequin.Enum.find!(messages, &(&1.commit_lsn == event1.commit_lsn and &1.commit_idx == event1.commit_idx))

      updated_event2 =
        Sequin.Enum.find!(messages, &(&1.commit_lsn == event2.commit_lsn and &1.commit_idx == event2.commit_idx))

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

    test "commits are removed from MessageLedgers", %{consumer: consumer} do
      event = ConsumersFactory.consumer_event(consumer_id: consumer.id, action: :insert, not_visible_until: nil)

      wal_cursor = %{
        commit_lsn: event.commit_lsn,
        commit_idx: event.commit_idx,
        commit_timestamp: DateTime.utc_now()
      }

      assert :ok = MessageLedgers.wal_cursors_ingested(consumer.id, [wal_cursor])
      assert {:ok, 1} = MessageLedgers.count_commit_verification_set(consumer.id)

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [event])

      adapter = fn req -> {req, Req.Response.new(status: 200)} end
      start_pipeline!(consumer, adapter, dummy_producer: false)

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 1_000

      assert {:ok, 0} = MessageLedgers.count_commit_verification_set(consumer.id)
    end
  end

  describe "messages flow from SlotMessageStore to http end-to-end for message_kind=record" do
    setup ctx do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)
      ConnectionCache.cache_connection(database, Repo)

      replication =
        ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          id: UUID.uuid4(),
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          replication_slot_id: replication.id,
          postgres_database: database,
          message_kind: :record,
          legacy_transform: ctx[:legacy_transform] || :none
        )

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    @tag legacy_transform: :none
    test "messages are sent from postgres to http endpoint without transforms", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
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
        ConsumersFactory.consumer_record(
          consumer_id: consumer.id,
          record_pks: [character.id],
          table_oid: CharacterDetailed.table_oid(),
          state: :available,
          data: %ConsumerRecordData{
            record: %{name: "character_name"},
            metadata: %ConsumerRecordData.Metadata{
              database_name: "postgres",
              table_schema: "public",
              table_name: "characters_detailed",
              commit_timestamp: DateTime.utc_now(),
              commit_lsn: Factory.unique_integer(),
              consumer: %ConsumerRecordData.Metadata.Sink{
                id: consumer.id,
                name: consumer.name
              }
            }
          }
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [consumer_record])

      # Start the pipeline
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 5_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      assert json["record"]["name"] == "character_name"

      # Assert metadata
      assert json["metadata"]["table_name"] == "characters_detailed"
      assert json["metadata"]["table_schema"] == "public"

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end

    @tag legacy_transform: :record_only
    test "messages are sent from postgres to http endpoint with record-only transform", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
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
        ConsumersFactory.consumer_record(
          consumer_id: consumer.id,
          record_pks: [character.id],
          table_oid: CharacterDetailed.table_oid(),
          state: :available,
          data: %ConsumerRecordData{
            record: %{name: "character_name"},
            metadata: %{
              database_name: "postgres",
              table_schema: "public",
              table_name: "characters_detailed",
              commit_timestamp: DateTime.utc_now(),
              consumer: %{
                id: consumer.id,
                name: consumer.name
              }
            }
          }
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [consumer_record])

      # Start the pipeline
      start_pipeline!(consumer, adapter, dummy_producer: false)

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 5_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
      %{"data" => [json]} = Jason.decode!(req.body)

      assert json["name"] == "character_name"
      refute json["metadata"]

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end

    test "legacy event transform is applied when feature flag is enabled", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()

      # Mock the HTTP adapter
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      record =
        ConsumersFactory.consumer_record(
          consumer_id: consumer.id,
          data:
            ConsumersFactory.consumer_record_data(
              record: %{
                "action" => "insert",
                "changes" => nil,
                "committed_at" => DateTime.utc_now(),
                "record" => %{
                  "id" => "123",
                  "name" => "John Doe"
                },
                "source_table_name" => "characters_detailed",
                "source_table_schema" => "public"
              }
            )
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      SlotMessageStore.put_messages(consumer, [record])

      # Start the pipeline with legacy_event_transform feature enabled
      start_pipeline!(consumer, adapter, features: [legacy_event_transform: true], dummy_producer: false)

      # Wait for the message to be processed
      assert_receive {:http_request, req}, 5_000

      # Assert the request details
      assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
      json = Jason.decode!(req.body)

      # Assert the transformed structure
      assert json["record"] == %{
               "id" => "123",
               "name" => "John Doe"
             }

      assert json["metadata"]["table_name"] == "characters_detailed"
      assert json["metadata"]["table_schema"] == "public"
      assert json["metadata"]["consumer"]["id"] == consumer.id
      assert json["metadata"]["consumer"]["name"] == consumer.name
      assert json["action"] == "insert"
      assert json["changes"] == nil

      assert_receive {SinkPipeline, :ack_finished, [_successful], []}, 5_000

      # Verify that the consumer record has been processed (deleted on ack)
      assert [] == SlotMessageStore.peek_messages(consumer, 10)
    end
  end

  describe "timestamp formatting" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          message_kind: :event,
          timestamp_format: :unix_microsecond,
          legacy_transform: :none
        )

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    test "timestamps are converted to unix format when timestamp_format is set to unix", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()
      timestamp = DateTime.from_naive!(~N[2023-01-01 00:00:00], "Etc/UTC")

      # Use a fixed timestamp for predictable test results
      event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          action: :insert,
          data: %ConsumerEventData{
            record: %{
              "id" => "123",
              "created_at" => timestamp,
              "nested" => %{
                "updated_at" => timestamp
              }
            },
            changes: %{
              "updated_at" => timestamp
            },
            action: :insert,
            metadata: %{
              table_name: "users",
              table_schema: "public",
              commit_timestamp: timestamp,
              commit_lsn: 123_456,
              commit_idx: 0,
              database_name: "postgres",
              consumer: %{
                id: consumer.id,
                name: consumer.name
              }
            }
          }
        )

      expected_unix_timestamp = DateTime.to_unix(timestamp, :microsecond)

      adapter = fn %Req.Request{} = req ->
        assert to_string(req.url) == HttpEndpoint.url(http_endpoint)
        %{"data" => [event_json]} = Jason.decode!(req.body)

        send(test_pid, {:sent, event_json})
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive {:sent, event_json}, 1_000

      # Verify record field timestamps are in unix format
      assert event_json["record"]["created_at"] == expected_unix_timestamp
      assert event_json["record"]["nested"]["updated_at"] == expected_unix_timestamp
      assert event_json["changes"]["updated_at"] == expected_unix_timestamp
    end
  end

  describe "HttpPushSink with via_sqs enabled" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      # Configure via SQS for testing
      via_config = %{
        main_queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        region: "us-east-1",
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key"
      }

      stub_application_get_env(fn
        :sequin, Sequin.Consumers.HttpPushSink -> [via_sqs_for_new_sinks?: true]
        :sequin, Sequin.Runtime.HttpPushSqsPipeline -> [sqs: via_config]
        app, key -> Application.get_env(app, key)
      end)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{
            type: :http_push,
            http_endpoint_id: http_endpoint.id,
            batch: false,
            via_sqs: true
          },
          message_kind: :event
        )

      assert consumer.sink.via_sqs == true

      consumer = Repo.preload(consumer, [:transform, :routing])

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint, account: account, via_config: via_config}}
    end

    test "sends message to SQS with correct payload format", %{consumer: consumer, via_config: via_config} do
      test_pid = self()

      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      # Stub AWS HTTP client for SQS
      Req.Test.stub(HttpClient, fn %Plug.Conn{} = conn ->
        send(test_pid, {:aws_req, conn})

        # Mock successful SQS response
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "test-msg-id"}]})
      end)

      # Create a dummy adapter for the pipeline - it should never be called
      # since we're routing to SQS
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:direct_http_req, req})
        {req, Req.Response.new(status: 200)}
      end

      # Send a test event into the pipeline
      start_pipeline!(consumer, adapter)
      ref = send_test_event(consumer, event)

      # Assert SQS request was made
      assert_receive {:aws_req, %Plug.Conn{} = conn}, 1_000

      # Assert direct HTTP request was NOT made
      refute_received {:direct_http_req, _}

      # 1. Correct SQS service endpoint URL
      assert via_config.main_queue_url =~ conn.host

      # 2. Correct HTTP method (POST for SQS)
      assert conn.method == "POST"

      # 3. Check content type for AWS API
      assert {_, content_type} = List.keyfind(conn.req_headers, "content-type", 0)
      assert content_type == "application/x-amz-json-1.0"

      # 4. Decode the request body
      decoded_body = conn |> Plug.Conn.read_body() |> elem(1) |> Jason.decode!()

      # 5. Verify the SQS SendMessageBatch structure
      assert %{
               "Entries" => [
                 %{
                   "Id" => _id,
                   "MessageAttributes" => %{},
                   "MessageBody" => message_body
                 }
               ]
             } = decoded_body

      # 6. The MessageBody should be a JSON string with Base64-encoded binary data
      %{"data" => body_base64} = Jason.decode!(message_body)
      binary_data = Base.decode64!(body_base64)

      # Deserialize the binary back to a term
      deserialized_event = :erlang.binary_to_term(binary_data)

      # Verify it's akin to a ConsumerEvent struct with the expected data
      assert deserialized_event.id == event.id
      assert deserialized_event.consumer_id == consumer.id
      assert deserialized_event.data.action == :insert

      # Assert ack for the message (indicating successful handoff to SQS)
      assert_receive {:ack, ^ref, [_acked_message], []}, 1_000
    end

    test "handles SQS unretryable failure by ack'ing the message", %{consumer: consumer} do
      test_pid = self()

      # Create multiple events for the batch
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      Req.Test.expect(HttpClient, 1, fn %Plug.Conn{} = conn ->
        conn
        |> Plug.Conn.put_status(400)
        |> Req.Test.json(%{
          "__type" => "com.amazonaws.sqs#BatchRequestTooLong",
          "message" => "Batch requests cannot be longer than 262144 bytes. You have sent 1336187 bytes."
        })
      end)

      # Create a dummy adapter for the pipeline - it should never be called
      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:direct_http_req, req})
        {req, Req.Response.new(status: 200)}
      end

      # Start the pipeline and send the batch
      start_pipeline!(consumer, adapter)
      ref = send_test_batch(consumer, [event1])

      assert_receive {:ack, ^ref, [_msg], []}, 1_000

      # Assert direct HTTP request was NOT made
      refute_received {:direct_http_req, _}
    end

    test "batches sqs messages", %{consumer: consumer} do
      test_pid = self()

      # Stub AWS HTTP client for SQS
      Req.Test.stub(HttpClient, fn %Plug.Conn{} = conn ->
        send(test_pid, {:aws_req, conn})

        # Mock successful SQS response
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "test-msg-id"}]})
      end)

      # Create multiple events for the batch
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:direct_http_req, req})
        {req, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)
      ref = send_test_batch(consumer, [event1, event2])

      # Assert that the messages succeeded in a batch
      assert_receive {:ack, ^ref, [_msg1, _msg2], []}, 1_000
    end
  end

  defp start_pipeline!(consumer, adapter, opts \\ []) do
    {dummy_producer, opts} = Keyword.pop(opts, :dummy_producer, true)

    opts =
      Keyword.merge(
        [
          consumer_id: consumer.id,
          req_opts: [adapter: adapter],
          test_pid: self()
        ],
        opts
      )

    opts =
      if dummy_producer do
        Keyword.put(opts, :producer, Broadway.DummyProducer)
      else
        opts
      end

    start_supervised!({SinkPipeline, opts})
  end

  defp send_test_event(consumer, event \\ nil) do
    event = event || ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)
    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test_topic", headers: []})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
