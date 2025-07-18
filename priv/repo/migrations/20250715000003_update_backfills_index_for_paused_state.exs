defmodule Sequin.Repo.Migrations.UpdateBackfillsIndexForPausedState do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Update the unique index to allow multiple paused backfills
    drop unique_index(:backfills, [:sink_consumer_id, :table_oid], prefix: @config_schema)

    create unique_index(
             :backfills,
             [:sink_consumer_id, :table_oid],
             where: "state IN ('active', 'paused')",
             prefix: @config_schema,
             name: "backfills_sink_consumer_id_table_oid_index"
           )
  end

  def down do
    # Drop the new index
    drop unique_index(:backfills, [:sink_consumer_id, :table_oid], prefix: @config_schema)

    # Recreate the old index
    create unique_index(
             :backfills,
             [:sink_consumer_id, :table_oid],
             where: "state = 'active'",
             prefix: @config_schema,
             name: "backfills_sink_consumer_id_table_oid_index"
           )
  end
end
