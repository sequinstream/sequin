defmodule Sequin.Repo.Migrations.AddPausedStatusToSinkConsumers do
  use Ecto.Migration
  # This allows DDL operations that can't run in transactions
  @disable_ddl_transaction true

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Add new enum value
    execute """
    ALTER TYPE #{@config_schema}.consumer_status ADD VALUE IF NOT EXISTS 'paused' AFTER 'disabled';
    """

    # Update existing disabled consumers to paused
    execute """
    UPDATE #{@config_schema}.sink_consumers
    SET status = 'paused'
    WHERE status = 'disabled';
    """
  end

  def down do
    # Update any paused consumers back to disabled
    execute """
    UPDATE #{@config_schema}.sink_consumers
    SET status = 'disabled'
    WHERE status = 'paused';
    """

    # Note: Postgres doesn't support removing enum values, so we can't remove 'paused'
    # from the type. This is a limitation we'll have to live with.
  end
end
