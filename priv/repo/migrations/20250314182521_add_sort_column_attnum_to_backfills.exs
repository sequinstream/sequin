defmodule Sequin.Repo.Migrations.AddSortColumnAttnumToBackfills do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      add :sort_column_attnum, :integer
    end

    # Remove NOT NULL constraint from sort_column_attnum in sequences table
    alter table(:sequences, prefix: @config_schema) do
      modify :sort_column_attnum, :integer, null: true
    end

    # Populate sort_column_attnum for existing backfills
    # Traverse from backfills -> sink_consumer_id -> sequences to get sort_column_attnum
    execute """
            update #{@config_schema}.backfills b
            set sort_column_attnum = s.sort_column_attnum
            from #{@config_schema}.sink_consumers sc
            join #{@config_schema}.sequences s on sc.sequence_id = s.id
            where b.sink_consumer_id = sc.id
            """,
            "select 1"
  end
end
