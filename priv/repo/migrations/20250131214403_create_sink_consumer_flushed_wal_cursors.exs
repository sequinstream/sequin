defmodule Sequin.Repo.Migrations.CreateSinkConsumerFlushedWalCursors do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:sink_consumer_flushed_wal_cursors, prefix: @config_schema) do
      add :commit_lsn, :bigint, null: false, default: 0
      add :commit_idx, :bigint, null: false, default: 0

      add :sink_consumer_id,
          references(:sink_consumers, on_delete: :delete_all, prefix: @config_schema),
          null: false

      timestamps()
    end

    create unique_index(:sink_consumer_flushed_wal_cursors, [:sink_consumer_id],
             prefix: @config_schema
           )

    # Add trigger to prevent backwards movement of commit values
    execute """
            CREATE OR REPLACE FUNCTION #{@config_schema}.check_wal_cursor_advance()
            RETURNS TRIGGER AS $$
            BEGIN
              IF (TG_OP = 'UPDATE') THEN
                IF (NEW.commit_lsn < OLD.commit_lsn) OR
                   (NEW.commit_lsn = OLD.commit_lsn AND NEW.commit_idx < OLD.commit_idx) THEN
                  RAISE EXCEPTION 'Cannot move WAL cursor backwards. New values must be greater than or equal to old values.';
                END IF;
              END IF;
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP FUNCTION IF EXISTS #{@config_schema}.check_wal_cursor_advance();
            """

    execute """
            CREATE TRIGGER ensure_wal_cursor_advance
            BEFORE UPDATE ON #{@config_schema}.sink_consumer_flushed_wal_cursors
            FOR EACH ROW
            EXECUTE FUNCTION #{@config_schema}.check_wal_cursor_advance();
            """,
            """
            DROP TRIGGER IF EXISTS ensure_wal_cursor_advance ON #{@config_schema}.sink_consumer_flushed_wal_cursors;
            """

    # Insert a record for each existing sink consumer
    execute """
            INSERT INTO #{@config_schema}.sink_consumer_flushed_wal_cursors
            (sink_consumer_id, commit_lsn, commit_idx, inserted_at, updated_at)
            SELECT
              id,
              0,
              0,
              NOW(),
              NOW()
            FROM #{@config_schema}.sink_consumers
            ON CONFLICT (sink_consumer_id) DO NOTHING;
            """,
            """
            DELETE FROM #{@config_schema}.sink_consumer_flushed_wal_cursors
            WHERE sink_consumer_id IN (SELECT id FROM #{@config_schema}.sink_consumers);
            """
  end
end
