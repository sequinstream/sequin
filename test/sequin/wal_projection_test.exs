defmodule Sequin.WalProjectionTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Replication.WalProjection

  describe "wal_projections" do
    test "list_wal_projections/0 returns all wal projections" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      assert Replication.list_wal_projections() == [wal_projection]
    end

    test "get_wal_projection/1 returns the wal projection with given id" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      assert {:ok, fetched_projection} = Replication.get_wal_projection(wal_projection.id)
      assert fetched_projection == wal_projection
    end

    test "delete_wal_projection/1 deletes the wal projection" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      assert {:ok, %WalProjection{}} = Replication.delete_wal_projection(wal_projection)
      assert {:error, _} = Replication.get_wal_projection(wal_projection.id)
    end

    test "create_wal_projection/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Replication.create_wal_projection(%{name: nil})
    end

    test "update_wal_projection/2 with invalid data returns error changeset" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      assert {:error, %Ecto.Changeset{}} = Replication.update_wal_projection(wal_projection, %{name: nil})
    end

    test "list_wal_projections_for_replication_slot/1 returns all wal projections for a given replication slot" do
      account = AccountsFactory.insert_account!()
      replication_slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)

      wal_projection1 =
        ReplicationFactory.insert_wal_projection!(replication_slot_id: replication_slot.id, account_id: account.id)

      wal_projection2 =
        ReplicationFactory.insert_wal_projection!(replication_slot_id: replication_slot.id, account_id: account.id)

      _other_wal_projection = ReplicationFactory.insert_wal_projection!(account_id: account.id)

      result = Replication.list_wal_projections_for_replication_slot(replication_slot.id)

      assert length(result) == 2
      assert Enum.all?(result, fn wp -> wp.id in [wal_projection1.id, wal_projection2.id] end)
    end

    test "create_wal_projection_with_lifecycle/2 creates a wal projection and notifies" do
      account = AccountsFactory.insert_account!()
      replication_slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)
      destination_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      valid_attrs =
        ReplicationFactory.wal_projection_attrs(%{
          name: "test_projection",
          source_tables: [%{oid: 12_345, actions: [:insert, :update], column_filters: []}],
          replication_slot_id: replication_slot.id,
          destination_oid: 54_321,
          destination_database_id: destination_database.id
        })

      assert {:ok, %WalProjection{} = wal_projection} =
               Replication.create_wal_projection_with_lifecycle(account.id, valid_attrs)

      assert wal_projection.name == "test_projection"
      assert length(wal_projection.source_tables) == 1
      assert wal_projection.replication_slot_id == replication_slot.id
      assert wal_projection.destination_oid == 54_321
      assert wal_projection.destination_database_id == destination_database.id
      assert wal_projection.account_id == account.id
    end

    test "update_wal_projection_with_lifecycle/2 updates the wal projection and notifies" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      update_attrs = %{name: "updated_projection"}

      assert {:ok, %WalProjection{} = updated_projection} =
               Replication.update_wal_projection_with_lifecycle(wal_projection, update_attrs)

      assert updated_projection.name == "updated_projection"
    end

    test "delete_wal_projection_with_lifecycle/1 deletes the wal projection and notifies" do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      assert {:ok, %WalProjection{}} = Replication.delete_wal_projection_with_lifecycle(wal_projection)
      assert {:error, _} = Replication.get_wal_projection(wal_projection.id)
    end
  end
end
