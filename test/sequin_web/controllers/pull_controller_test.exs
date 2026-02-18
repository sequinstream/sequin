defmodule SequinWeb.PullControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.TestSupport.Models.Character

  setup :authenticated_conn

  @one_day_ago DateTime.add(DateTime.utc_now(), -24, :hour)

  setup %{account: account} = ctx do
    other_account = AccountsFactory.insert_account!()
    db = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)
    ConnectionCache.cache_connection(db, Sequin.Repo)
    rep_slot = ReplicationFactory.insert_postgres_replication!(postgres_database_id: db.id, account_id: account.id)

    consumer =
      ConsumersFactory.insert_sink_consumer!(
        account_id: account.id,
        backfill_completed_at: @one_day_ago,
        replication_slot_id: rep_slot.id,
        sink: %{type: :sequin_stream},
        legacy_transform: ctx[:legacy_transform] || :none
      )

    other_consumer =
      ConsumersFactory.insert_sink_consumer!(
        account_id: other_account.id,
        sink: %{type: :sequin_stream},
        legacy_transform: ctx[:legacy_transform] || :none
      )

    start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
    start_supervised!({SlotMessageStoreSupervisor, consumer_id: other_consumer.id, test_pid: self()})

    %{consumer: consumer, other_consumer: other_consumer}
  end

  describe "receive" do
    test "returns 404 if trying to pull for another account's consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      conn = get(conn, ~p"/api/sequin_streams/#{other_consumer.id}/receive")
      assert json_response(conn, 404)
    end

    test "returns empty list if no messages to return", %{conn: conn, consumer: consumer} do
      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => []} = json_response(conn, 200)
    end

    @tag legacy_transform: :none
    test "returns available messages if mix of available and delivered (no transform)", %{
      conn: conn,
      consumer: consumer
    } do
      record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)

      delivered_record =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )

      SlotMessageStore.put_messages(consumer, [record, delivered_record])

      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["data"]["record"]["column"] == record.data.record["column"]
    end

    @tag legacy_transform: :record_only
    test "returns available messages if mix of available and delivered (record-only transform)", %{
      conn: conn,
      consumer: consumer
    } do
      record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)

      delivered_record =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )

      SlotMessageStore.put_messages(consumer, [record, delivered_record])

      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["data"]["column"] == record.data.record["column"]
    end

    test "returns an available message by consumer name", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
      expect_uuid4(fn -> record.ack_id end)
      SlotMessageStore.put_messages(consumer, [record])

      conn = get(conn, ~p"/api/sequin_streams/#{consumer.name}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)
      assert message["ack_id"] == record.ack_id
    end

    test "respects batch_size parameter", %{conn: conn, consumer: consumer} do
      for _ <- 1..3 do
        record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
        SlotMessageStore.put_messages(consumer, [record])
      end

      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", batch_size: 1)
      assert %{"data" => messages} = json_response(conn, 200)
      assert length(messages) == 1
    end

    test "applies enrichment function to messages", %{conn: conn, account: account} do
      # Create a character in the test database
      character = CharacterFactory.insert_character!()

      # Create database and replication slot
      db = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: :character_tables)
      ConnectionCache.cache_connection(db, Sequin.Repo)
      rep_slot = ReplicationFactory.insert_postgres_replication!(postgres_database_id: db.id, account_id: account.id)

      # Create an enrichment function that looks up character data
      enrichment_function =
        FunctionsFactory.insert_enrichment_function!(
          account_id: account.id,
          function_attrs: [
            code: """
            SELECT id, 'enriched_' || name as enriched_name
            FROM "Characters"
            WHERE id = ANY($1::int[])
            """
          ]
        )

      # Create consumer with enrichment
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          backfill_completed_at: DateTime.add(DateTime.utc_now(), -24, :hour),
          replication_slot_id: rep_slot.id,
          sink: %{type: :sequin_stream},
          enrichment_id: enrichment_function.id,
          source: %{
            include_table_oids: [Character.table_oid()]
          }
        )

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Create a message for the character
      event =
        ConsumersFactory.deliverable_consumer_event(
          consumer_id: consumer.id,
          table_oid: Character.table_oid(),
          record_pks: [character.id],
          data: ConsumersFactory.consumer_event_data(record: %{"id" => character.id, "name" => character.name})
        )

      SlotMessageStore.put_messages(consumer, [event])

      # Fetch the message and verify enrichment
      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => [message]} = json_response(conn, 200)

      # Verify enrichment data is present
      assert message["data"]["metadata"]["enrichment"] == %{
               "id" => character.id,
               "enriched_name" => "enriched_#{character.name}"
             }
    end
  end

  describe "receive, wait_for behavior" do
    test "returns immediately when messages are available", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
      SlotMessageStore.put_messages(consumer, [record])

      assert_elapsed_under(120, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", wait_for: 5000)
        assert %{"data" => [_message]} = json_response(conn, 200)
      end)
    end

    test "waits up to specified time when no messages available", %{conn: conn, consumer: consumer} do
      assert_elapsed_at_least(100, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", wait_for: 101)
        assert %{"data" => []} = json_response(conn, 200)
      end)
    end

    test "returns early when messages become available during wait", %{
      conn: conn,
      consumer: consumer
    } do
      Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
        # Wait briefly then insert a message
        Process.sleep(10)
        record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
        SlotMessageStore.put_messages(consumer, [record])
      end)

      assert_elapsed_under(1000, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", wait_for: 5000)
        assert %{"data" => [_message]} = json_response(conn, 200)
      end)
    end
  end

  describe "receive, batch size behavior with wait" do
    test "returns immediately when full batch is available", %{conn: conn, consumer: consumer} do
      # Insert 3 messages
      for _ <- 1..3 do
        record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
        SlotMessageStore.put_messages(consumer, [record])
      end

      assert_elapsed_under(500, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", max_batch_size: 3, wait_for: 5000)
        assert %{"data" => messages} = json_response(conn, 200)
        assert length(messages) == 3
      end)
    end

    test "returns partial batch if any available", %{conn: conn, consumer: consumer} do
      # Insert just 1 message when max_batch_size is 3
      record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
      SlotMessageStore.put_messages(consumer, [record])

      assert_elapsed_under(100, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", max_batch_size: 3, wait_for: 5000)
        assert %{"data" => messages} = json_response(conn, 200)
        assert length(messages) == 1
      end)
    end

    test "returns as soon as any messages are available during wait", %{conn: conn, consumer: consumer} do
      Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
        Process.sleep(10)
        record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
        SlotMessageStore.put_messages(consumer, [record])
      end)

      assert_elapsed_under(1000, fn ->
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", max_batch_size: 3, wait_for: 5000)
        assert %{"data" => messages} = json_response(conn, 200)
        assert length(messages) == 1
      end)
    end

    test "supports legacy batch_size parameter for backwards compatibility", %{conn: conn, consumer: consumer} do
      for _ <- 1..3 do
        record = ConsumersFactory.deliverable_consumer_event(consumer_id: consumer.id)
        SlotMessageStore.put_messages(consumer, [record])
      end

      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", batch_size: 2)
      assert %{"data" => messages} = json_response(conn, 200)
      assert length(messages) == 2
    end
  end

  describe "receive, wait_for, and batch_size parameter validation" do
    test "rejects invalid wait_for values", %{conn: conn, consumer: consumer} do
      invalid_values = [-1, "abc", 1_000_000]

      for value <- invalid_values do
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", wait_for: value)
        assert json_response(conn, 400)
      end
    end

    test "rejects invalid legacy batch_size values", %{conn: conn, consumer: consumer} do
      invalid_values = [0, -1, "abc", 10_001]

      for value <- invalid_values do
        conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive", batch_size: value)
        assert json_response(conn, 400)
      end
    end
  end

  describe "ack" do
    test "successfully acks a message", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.insert_deliverable_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/ack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      # Verify the message can't be pulled again
      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => []} = json_response(conn, 200)

      # Verify it's gone from consumer_events
      assert Consumers.list_consumer_events_for_consumer(consumer.id) == []
    end

    test "successfully acks a message by consumer name", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.insert_deliverable_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.name}/ack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
    end

    test "allows acking a message twice", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.insert_deliverable_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/ack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/ack", ack_ids: [record.ack_id])
      assert json_response(conn, 200) == %{"success" => true}
    end

    test "returns 404 when acking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      record = ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id)

      conn = post(conn, ~p"/api/sequin_streams/#{other_consumer.id}/ack", ack_ids: [record.ack_id])
      assert json_response(conn, 404)
    end
  end

  describe "nack" do
    test "successfully nacks a message", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.consumer_event(consumer_id: consumer.id)
      expect_uuid4(fn -> record.ack_id end)
      SlotMessageStore.put_messages(consumer, [record])

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/nack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
      # Verify the message reappears
      conn = get(conn, ~p"/api/sequin_streams/#{consumer.id}/receive")
      assert %{"data" => [nacked_message]} = json_response(conn, 200)
      assert nacked_message["ack_id"] == record.ack_id
    end

    test "successfully nacks a message by consumer name", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.name}/nack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}
    end

    test "allows nacking a message twice", %{conn: conn, consumer: consumer} do
      record = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      res_conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/nack", ack_ids: [record.ack_id])
      assert json_response(res_conn, 200) == %{"success" => true}

      conn = post(conn, ~p"/api/sequin_streams/#{consumer.id}/nack", ack_ids: [record.ack_id])
      assert json_response(conn, 200) == %{"success" => true}
    end

    test "returns 404 when nacking a message belonging to another consumer", %{
      conn: conn,
      other_consumer: other_consumer
    } do
      record = ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id)

      conn = post(conn, ~p"/api/sequin_streams/#{other_consumer.id}/nack", ack_ids: [record.ack_id])
      assert json_response(conn, 404)
    end
  end

  defp assert_elapsed_under(elapsed, fun, fun_desc \\ "function") do
    {time, value} = :timer.tc(fun, :millisecond)
    assert time < elapsed, "Expected #{fun_desc} to complete in #{elapsed}ms, but it took #{time}ms"
    value
  end

  defp assert_elapsed_at_least(elapsed, fun, fun_desc \\ "function") do
    {time, value} = :timer.tc(fun, :millisecond)
    assert time >= elapsed, "Expected #{fun_desc} to complete in at least #{elapsed}ms, but it took #{time}ms"
    value
  end
end
