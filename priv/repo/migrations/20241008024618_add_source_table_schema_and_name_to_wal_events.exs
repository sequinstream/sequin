defmodule Sequin.Repo.Migrations.AddSourceTableSchemaAndNameToWalEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:wal_events, prefix: @stream_schema) do
      add :source_table_schema, :text
      add :source_table_name, :text
    end

    execute "UPDATE #{@stream_schema}.wal_events SET source_table_schema = 'n/a' WHERE source_table_schema IS NULL",
            "select 1"

    execute "UPDATE #{@stream_schema}.wal_events SET source_table_name = 'n/a' WHERE source_table_name IS NULL",
            "select 1"

    alter table(:wal_events, prefix: @stream_schema) do
      modify :source_table_schema, :text, null: false
      modify :source_table_name, :text, null: false
    end
  end
end
