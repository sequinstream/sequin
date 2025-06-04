defmodule Sequin.Consumers.BackfillTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers.Backfill
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Repo

  describe "backfill" do
    test "inserts a valid backfill for a consumer" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      table_oid = Factory.integer()

      attrs = %{
        account_id: consumer.account_id,
        sink_consumer_id: consumer.id,
        table_oid: table_oid,
        state: :active,
        initial_min_cursor: %{0 => 0},
        rows_initial_count: 100
      }

      assert {:ok, backfill} = Repo.insert(Backfill.create_changeset(%Backfill{}, attrs))
      assert backfill.state == :active
      assert backfill.table_oid == table_oid
      assert backfill.rows_initial_count == 100
      assert backfill.rows_processed_count == 0
      assert backfill.rows_ingested_count == 0
      assert backfill.initial_min_cursor == %{0 => 0}
      assert is_nil(backfill.completed_at)
      assert is_nil(backfill.canceled_at)
    end

    test "updates backfill from active to completed" do
      backfill = ConsumersFactory.insert_active_backfill!()

      attrs = %{
        state: :completed,
        rows_processed_count: 100,
        rows_ingested_count: 95
      }

      assert {:ok, updated_backfill} = Repo.update(Backfill.update_changeset(backfill, attrs))
      assert updated_backfill.state == :completed
      assert updated_backfill.rows_processed_count == 100
      assert updated_backfill.rows_ingested_count == 95
      assert updated_backfill.completed_at
    end

    test "preloads active_backfill association on sink consumer" do
      account = AccountsFactory.insert_account!()
      consumer = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
      backfill = ConsumersFactory.insert_active_backfill!(account_id: account.id, sink_consumer_id: consumer.id)

      consumer_with_backfill = Repo.preload(consumer, :active_backfill)
      assert consumer_with_backfill.active_backfill.id == backfill.id

      # Verify completed backfills don't show up in active_backfill
      ConsumersFactory.insert_completed_backfill!(account_id: account.id, sink_consumer_id: consumer.id)
      consumer_with_backfill = Repo.preload(consumer, :active_backfill)
      assert consumer_with_backfill.active_backfill.id == backfill.id
    end

    test "prevents inserting multiple active backfills for the same consumer" do
      account = AccountsFactory.insert_account!()
      consumer = ConsumersFactory.insert_sink_consumer!(account_id: account.id)

      # Insert first active backfill
      ConsumersFactory.insert_active_backfill!(account_id: account.id, sink_consumer_id: consumer.id)

      # Attempt to insert second active backfill
      assert {:error, changeset} =
               %Backfill{}
               |> Backfill.create_changeset(%{
                 account_id: consumer.account_id,
                 sink_consumer_id: consumer.id,
                 state: :active,
                 table_oid: Factory.integer(),
                 initial_min_cursor: %{0 => 0}
               })
               |> Repo.insert()

      assert %{sink_consumer_id: ["already has an active backfill"]} = errors_on(changeset)
    end
  end
end
