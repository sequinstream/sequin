defmodule Sequin.Repo.Migrations.CreateBackfillsTable do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Create enum type for backfill states
    execute """
    CREATE TYPE #{@config_schema}.backfill_state AS ENUM (
      'active', 'completed', 'cancelled'
    );
    """

    create table(:backfills, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :sink_consumer_id,
          references(:sink_consumers,
            with: [account_id: :account_id],
            prefix: @config_schema
          ),
          null: false

      add :initial_min_cursor, :map
      add :state, :"#{@config_schema}.backfill_state", null: false
      add :rows_initial_count, :integer
      add :rows_processed_count, :integer, default: 0, null: false
      add :rows_ingested_count, :integer, default: 0, null: false
      add :completed_at, :utc_datetime_usec
      add :canceled_at, :utc_datetime_usec

      timestamps()
    end

    # Create unique constraint for active backfills per sink consumer
    create unique_index(
             :backfills,
             [:sink_consumer_id],
             where: "state = 'active'",
             prefix: @config_schema
           )

    # Create trigger function to set timestamps based on state changes
    execute """
    CREATE OR REPLACE FUNCTION #{@config_schema}.set_backfill_timestamps()
    RETURNS TRIGGER AS $$
    BEGIN
      IF NEW.state = 'completed' AND OLD.state != 'completed' THEN
        NEW.completed_at = NOW();
      ELSIF NEW.state = 'cancelled' AND OLD.state != 'cancelled' THEN
        NEW.canceled_at = NOW();
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    # Create trigger
    execute """
    CREATE TRIGGER set_backfill_timestamps
    BEFORE UPDATE ON #{@config_schema}.backfills
    FOR EACH ROW
    EXECUTE FUNCTION #{@config_schema}.set_backfill_timestamps();
    """
  end

  def down do
    drop table(:backfills, prefix: @config_schema)
    execute "DROP FUNCTION IF EXISTS #{@config_schema}.set_backfill_timestamps() CASCADE;"
    execute "DROP TYPE IF EXISTS #{@config_schema}.backfill_state;"
  end
end
