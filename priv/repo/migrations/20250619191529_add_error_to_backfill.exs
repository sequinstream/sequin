defmodule Sequin.Repo.Migrations.AddErrorToBackfill do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # Add failed state to backfill state enum
    execute "ALTER TYPE #{@config_schema}.backfill_state ADD VALUE 'failed';"

    alter table(:backfills, prefix: @config_schema) do
      add :error, :jsonb
      add :failed_at, :naive_datetime
    end

    execute """
    CREATE OR REPLACE FUNCTION #{@config_schema}.set_backfill_timestamps()
    RETURNS TRIGGER AS $$
    BEGIN
      IF NEW.state = 'completed' AND OLD.state != 'completed' THEN
        NEW.completed_at = NOW() AT TIME ZONE 'UTC';
      ELSIF NEW.state = 'cancelled' AND OLD.state != 'cancelled' THEN
        NEW.canceled_at = NOW() AT TIME ZONE 'UTC';
      ELSIF NEW.state = 'failed' AND OLD.state != 'failed' THEN
        NEW.failed_at = NOW() AT TIME ZONE 'UTC';
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
  end
end
