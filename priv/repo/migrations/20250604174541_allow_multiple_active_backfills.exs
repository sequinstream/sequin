defmodule Sequin.Repo.Migrations.AllowMultipleActiveBackfills do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # Drop the existing unique index
    drop_if_exists index(:backfills, [:sink_consumer_id],
                     where: "state = 'active'",
                     prefix: @config_schema
                   )

    # Create new unique index including table_oid
    create unique_index(
             :backfills,
             [:sink_consumer_id, :table_oid],
             where: "state = 'active'",
             prefix: @config_schema
           )
  end
end
