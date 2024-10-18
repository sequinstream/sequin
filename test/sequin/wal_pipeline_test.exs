defmodule Sequin.WalPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Replication.WalPipeline

  describe "wal_pipelines" do
    test "list_wal_pipelines/0 returns all wal pipelines" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      assert Replication.list_wal_pipelines() == [wal_pipeline]
    end

    test "get_wal_pipeline/1 returns the wal pipeline with given id" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      assert {:ok, fetched_pipeline} = Replication.get_wal_pipeline(wal_pipeline.id)
      assert fetched_pipeline == wal_pipeline
    end

    test "delete_wal_pipeline/1 deletes the wal pipeline" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      assert {:ok, %WalPipeline{}} = Replication.delete_wal_pipeline(wal_pipeline)
      assert {:error, _} = Replication.get_wal_pipeline(wal_pipeline.id)
    end

    test "create_wal_pipeline/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Replication.create_wal_pipeline("account-id", %{name: nil})
    end

    test "update_wal_pipeline/2 with invalid data returns error changeset" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      assert {:error, %Ecto.Changeset{}} = Replication.update_wal_pipeline(wal_pipeline, %{name: nil})
    end

    test "list_wal_pipelines_for_replication_slot/1 returns all wal pipelines for a given replication slot" do
      account = AccountsFactory.insert_account!()
      replication_slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)

      wal_pipeline1 =
        ReplicationFactory.insert_wal_pipeline!(replication_slot_id: replication_slot.id, account_id: account.id)

      wal_pipeline2 =
        ReplicationFactory.insert_wal_pipeline!(replication_slot_id: replication_slot.id, account_id: account.id)

      _other_wal_pipeline = ReplicationFactory.insert_wal_pipeline!(account_id: account.id)

      result = Replication.list_wal_pipelines_for_replication_slot(replication_slot.id)

      assert length(result) == 2
      assert Enum.all?(result, fn wp -> wp.id in [wal_pipeline1.id, wal_pipeline2.id] end)
    end

    test "create_wal_pipeline_with_lifecycle/2 creates a wal pipeline and notifies" do
      account = AccountsFactory.insert_account!()
      replication_slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)
      destination_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      valid_attrs =
        ReplicationFactory.wal_pipeline_attrs(%{
          name: "test_pipeline",
          source_tables: [%{oid: 12_345, actions: [:insert, :update], column_filters: []}],
          replication_slot_id: replication_slot.id,
          destination_oid: 54_321,
          destination_database_id: destination_database.id
        })

      assert {:ok, %WalPipeline{} = wal_pipeline} =
               Replication.create_wal_pipeline_with_lifecycle(account.id, valid_attrs)

      assert wal_pipeline.name == "test_pipeline"
      assert length(wal_pipeline.source_tables) == 1
      assert wal_pipeline.replication_slot_id == replication_slot.id
      assert wal_pipeline.destination_oid == 54_321
      assert wal_pipeline.destination_database_id == destination_database.id
      assert wal_pipeline.account_id == account.id
    end

    test "update_wal_pipeline_with_lifecycle/2 updates the wal pipeline and notifies" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      update_attrs = %{name: "updated_pipeline"}

      assert {:ok, %WalPipeline{} = updated_pipeline} =
               Replication.update_wal_pipeline_with_lifecycle(wal_pipeline, update_attrs)

      assert updated_pipeline.name == "updated_pipeline"
    end

    test "delete_wal_pipeline_with_lifecycle/1 deletes the wal pipeline and notifies" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      assert {:ok, %WalPipeline{}} = Replication.delete_wal_pipeline_with_lifecycle(wal_pipeline)
      assert {:error, _} = Replication.get_wal_pipeline(wal_pipeline.id)
    end
  end
end
