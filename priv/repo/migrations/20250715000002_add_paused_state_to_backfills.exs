defmodule Sequin.Repo.Migrations.AddPausedStateToBackfills do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Add paused state to backfill state enum (idempotent)
    execute "ALTER TYPE #{@config_schema}.backfill_state ADD VALUE IF NOT EXISTS 'paused';"
  end

  def down do
    # We don't drop values from enum types
    :ok
  end
end
