defmodule Sequin.Repo.Migrations.CreateWatermarkWalCursorTables do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:sink_consumer_high_watermark_wal_cursors, prefix: @config_schema) do
      add :commit_lsn, :bigint, null: false, default: 0
      add :commit_idx, :bigint, null: false, default: 0

      add :sink_consumer_id,
          references(:sink_consumers, on_delete: :delete_all, prefix: @config_schema),
          null: false

      timestamps()
    end

    create unique_index(:sink_consumer_high_watermark_wal_cursors, [:sink_consumer_id],
             prefix: @config_schema
           )

    # Add trigger to prevent backwards movement of commit values
    execute """
            CREATE OR REPLACE FUNCTION #{@config_schema}.check_high_watermark_wal_cursor_advance()
            RETURNS TRIGGER AS $$
            BEGIN
              IF (TG_OP = 'UPDATE') THEN
                IF (NEW.commit_lsn < OLD.commit_lsn) OR
                   (NEW.commit_lsn = OLD.commit_lsn AND NEW.commit_idx < OLD.commit_idx) THEN
                  RAISE EXCEPTION 'Cannot move high watermark WAL cursor backwards. New values must be greater than or equal to old values.';
                END IF;
              END IF;
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP FUNCTION IF EXISTS #{@config_schema}.check_high_watermark_wal_cursor_advance();
            """

    execute """
            CREATE TRIGGER ensure_high_watermark_wal_cursor_advance
            BEFORE UPDATE ON #{@config_schema}.sink_consumer_high_watermark_wal_cursors
            FOR EACH ROW
            EXECUTE FUNCTION #{@config_schema}.check_high_watermark_wal_cursor_advance();
            """,
            """
            DROP TRIGGER IF EXISTS ensure_high_watermark_wal_cursor_advance ON #{@config_schema}.sink_consumer_high_watermark_wal_cursors;
            """

    execute "create type #{@config_schema}.watermark_boundary as enum ('high', 'low')",
            "drop type #{@config_schema}.watermark_boundary"

    create table(:replication_slot_watermark_wal_cursor, prefix: @config_schema) do
      add :commit_lsn, :bigint, null: false
      add :commit_idx, :bigint, null: false

      add :replication_slot_id,
          references(:postgres_replication_slots, on_delete: :delete_all, prefix: @config_schema),
          null: false

      add :boundary, :"#{@config_schema}.watermark_boundary", null: false

      timestamps()
    end

    # Create unique index on replication_slot_id and boundary combination
    create unique_index(
             :replication_slot_watermark_wal_cursor,
             [:replication_slot_id, :boundary],
             prefix: @config_schema
           )

    # Add trigger to prevent backwards movement of watermark values
    execute """
            CREATE OR REPLACE FUNCTION #{@config_schema}.check_replication_slot_watermark_advance()
            RETURNS TRIGGER AS $$
            BEGIN
              IF (TG_OP = 'UPDATE') THEN
                IF (NEW.commit_lsn < OLD.commit_lsn) OR
                   (NEW.commit_lsn = OLD.commit_lsn AND NEW.commit_idx < OLD.commit_idx) THEN
                  RAISE EXCEPTION 'Cannot move % watermark backwards. New values must be greater than or equal to old values.', NEW.boundary;
                END IF;
              END IF;
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP FUNCTION IF EXISTS #{@config_schema}.check_replication_slot_watermark_advance();
            """

    execute """
            CREATE TRIGGER ensure_replication_slot_watermark_advance
            BEFORE UPDATE ON #{@config_schema}.replication_slot_watermark_wal_cursor
            FOR EACH ROW
            EXECUTE FUNCTION #{@config_schema}.check_replication_slot_watermark_advance();
            """,
            """
            DROP TRIGGER IF EXISTS ensure_replication_slot_watermark_advance ON #{@config_schema}.replication_slot_watermark_wal_cursor;
            """
  end
end
