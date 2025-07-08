# test/sequin/wal_pipeline_server_test.exs

defmodule Sequin.Runtime.WalPipelineServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Runtime.WalPipelineServer
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterMultiPK
  alias Sequin.TestSupport.Models.TestEventLog
  alias Sequin.TestSupport.Models.TestEventLogV0

  setup do
    account = AccountsFactory.insert_account!()
    source_database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)

    ConnectionCache.cache_connection(source_database, Sequin.Repo)

    destination_database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)

    replication_slot =
      ReplicationFactory.insert_postgres_replication!(
        account_id: account.id,
        postgres_database_id: source_database.id
      )

    wal_pipeline =
      ReplicationFactory.insert_wal_pipeline!(
        status: :active,
        account_id: account.id,
        replication_slot_id: replication_slot.id,
        destination_database_id: destination_database.id,
        destination_oid: TestEventLog.table_oid()
      )

    wal_pipeline_v0 =
      ReplicationFactory.insert_wal_pipeline!(
        status: :active,
        account_id: account.id,
        replication_slot_id: replication_slot.id,
        destination_database_id: destination_database.id,
        destination_oid: TestEventLogV0.table_oid()
      )

    {:ok,
     %{
       source_database: source_database,
       destination_database: destination_database,
       replication_slot: replication_slot,
       wal_pipeline: wal_pipeline,
       wal_pipeline_v0: wal_pipeline_v0
     }}
  end

  describe "WalPipelineServer" do
    test "processes WAL events, writes them to the destination table, and updates health", %{
      wal_pipeline: wal_pipeline
    } do
      # Simulate initial health
      Health.put_event(wal_pipeline, %Health.Event{slug: :messages_filtered})
      Health.put_event(wal_pipeline, %Health.Event{slug: :messages_ingested})

      commit_lsn = ReplicationFactory.commit_lsn()

      # Insert some WAL events
      wal_events =
        Enum.map(1..5, fn n ->
          ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id, commit_lsn: commit_lsn, commit_idx: n)
        end)

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self(),
           batch_size: 2
         ]}
      )

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 2}, 1000
      assert_receive {WalPipelineServer, :wrote_events, 2}, 1000
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify that the events were written to the destination table
      logs = Repo.all(TestEventLog)
      assert length(logs) == 5

      as_logs =
        Enum.map(wal_events, fn wal_event ->
          %{
            seq: wal_event.commit_lsn + wal_event.commit_idx,
            source_table_oid: wal_event.source_table_oid,
            source_table_schema: wal_event.source_table_schema,
            source_table_name: wal_event.source_table_name,
            record_pk: hd(wal_event.record_pks),
            action: to_string(wal_event.action),
            changes: wal_event.changes,
            committed_at: wal_event.committed_at,
            record: wal_event.record
          }
        end)

      # Assert that the WAL events were properly mapped to TestEventLog
      assert_lists_equal(as_logs, logs, fn log1, log2 ->
        assert_maps_equal(log1, log2, [
          :seq,
          :record_pk,
          :action,
          :changes,
          :source_table_oid,
          :source_table_schema,
          :source_table_name,
          # Inscrutable timezone/dt issue
          # :committed_at,
          :record
        ])
      end)

      # Verify that the health status is updated to healthy
      {:ok, health} = Health.health(wal_pipeline)
      assert health.status == :healthy
      assert Enum.all?(health.checks, &(&1.status == :healthy))
    end

    @tag capture_log: true
    test "updates health to error when writing to destination fails", %{
      wal_pipeline: wal_pipeline
    } do
      # Insert a WAL event
      ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id)

      # Mess up the destination table
      Repo.query!("alter table #{TestEventLog.table_name()} add column foo text not null")

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self(),
           batch_size: 1
         ]}
      )

      assert_receive {WalPipelineServer, :write_failed, _reason}, 1000

      # Verify that the health status is updated to error
      {:ok, health} = Health.health(wal_pipeline)
      assert health.status == :error
      assert Enum.any?(health.checks, &(&1.status == :error))
    end

    test "processes WAL events on PubSub notification", %{
      wal_pipeline: wal_pipeline
    } do
      start_supervised!({WalPipelineServer,
       [
         replication_slot_id: wal_pipeline.replication_slot_id,
         destination_oid: wal_pipeline.destination_oid,
         destination_database_id: wal_pipeline.destination_database_id,
         wal_pipeline_ids: [wal_pipeline.id],
         test_pid: self(),
         batch_size: 2,
         # arbitrary delay
         interval_ms: to_timeout(minute: 1)
       ]})

      # Assert that we get :no_events initially
      assert_receive {WalPipelineServer, :no_events}, 1000

      # Insert a WAL event
      wal_event = ReplicationFactory.wal_event_attrs(wal_pipeline_id: wal_pipeline.id)
      Replication.insert_wal_events([wal_event])

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify that the event was written to the destination table
      logs = Repo.all(TestEventLog)
      assert length(logs) == 1

      # Verify that the WAL event was deleted after processing
      assert Replication.list_wal_events(wal_pipeline.id) == []
    end

    test "processes WAL events for v0 event log table", %{
      wal_pipeline_v0: wal_pipeline_v0
    } do
      # Insert a couple WAL events
      wal_event = ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline_v0.id)

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline_v0.replication_slot_id,
           destination_oid: wal_pipeline_v0.destination_oid,
           destination_database_id: wal_pipeline_v0.destination_database_id,
           wal_pipeline_ids: [wal_pipeline_v0.id],
           test_pid: self()
         ]}
      )

      # Wait for processing
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify events were written to the v0 table
      logs = Repo.all(TestEventLogV0)
      assert length(logs) == 1
      log = hd(logs)

      # Verify basic fields match
      assert log.seq == wal_event.commit_lsn + wal_event.commit_idx
      assert log.record_pk == hd(wal_event.record_pks)
      assert log.action == to_string(wal_event.action)
    end
  end

  describe "TOAST handling" do
    test "loads unchanged toast values when processing WAL events", %{
      wal_pipeline: wal_pipeline
    } do
      character = CharacterFactory.insert_character!(planet: "Tatooine")

      # Insert a WAL event with an unchanged toast value
      ReplicationFactory.insert_wal_event!(
        wal_pipeline_id: wal_pipeline.id,
        record: %{
          "id" => character.id,
          "planet" => :unchanged_toast
        },
        source_table_schema: "public",
        source_table_name: "characters",
        source_table_oid: Character.table_oid(),
        record_pks: [to_string(character.id)]
      )

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self()
         ]}
      )

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify that the TOAST value was loaded correctly
      [log] = Repo.all(TestEventLog)

      assert log.record["planet"] == "Tatooine",
             "Expected TOAST value to be loaded found #{inspect(log.record["description"])}"
    end

    test "handles multiple TOAST values in a single batch", %{
      wal_pipeline: wal_pipeline
    } do
      character1 = CharacterFactory.insert_character!(planet: "Tatooine1")
      character2 = CharacterFactory.insert_character!(planet: "Tatooine2")

      # Insert WAL events with unchanged toast values
      Enum.each(
        [character1, character2],
        fn character ->
          ReplicationFactory.insert_wal_event!(
            wal_pipeline_id: wal_pipeline.id,
            record: %{
              "id" => character.id,
              "planet" => :unchanged_toast
            },
            source_table_schema: "public",
            source_table_name: "characters",
            source_table_oid: Character.table_oid(),
            record_pks: [to_string(character.id)]
          )
        end
      )

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self()
         ]}
      )

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 2}, 1000

      # Verify that both TOAST values were loaded correctly
      logs = Repo.all(TestEventLog)
      assert length(logs) == 2

      [log1, log2] = Enum.sort_by(logs, & &1.record["id"])

      assert log1.record["planet"] == "Tatooine1",
             "Expected TOAST value to be loaded found #{inspect(log1.record["planet"])}"

      assert log2.record["planet"] == "Tatooine2",
             "Expected TOAST value to be loaded found #{inspect(log2.record["planet"])}"
    end

    test "handles TOAST values with multiple primary keys", %{
      wal_pipeline: wal_pipeline
    } do
      character = CharacterFactory.insert_character_multi_pk!()
      attrs = character |> Sequin.Map.from_ecto() |> Map.new(fn {k, v} -> {to_string(k), v} end)

      # Insert WAL event with composite primary key
      ReplicationFactory.insert_wal_event!(
        wal_pipeline_id: wal_pipeline.id,
        record: %{attrs | "name" => :unchanged_toast},
        source_table_schema: "public",
        source_table_name: "characters_multi_pk",
        source_table_oid: CharacterMultiPK.table_oid(),
        record_pks: [character.id_integer, character.id_string, character.id_uuid]
      )

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self()
         ]}
      )

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify that the TOAST value was loaded correctly
      [log] = Repo.all(TestEventLog)

      assert log.record["name"] == character.name,
             "Expected TOAST value to be loaded found #{inspect(log.record["name"])}"
    end
  end
end
