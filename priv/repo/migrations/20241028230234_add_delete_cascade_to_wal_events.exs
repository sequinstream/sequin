defmodule Sequin.Repo.Migrations.AddDeleteCascadeToWalEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    drop constraint(:wal_events, "wal_events_wal_pipeline_id_fkey", prefix: @stream_schema)

    alter table(:wal_events, prefix: @stream_schema) do
      modify :wal_pipeline_id,
             references(:wal_pipelines,
               prefix: @config_schema,
               type: :uuid,
               on_delete: :delete_all
             ),
             null: false
    end
  end

  def down do
    drop constraint(:wal_events, "wal_events_wal_pipeline_id_fkey", prefix: @stream_schema)

    alter table(:wal_events, prefix: @stream_schema) do
      modify :wal_pipeline_id,
             references(:wal_pipelines,
               prefix: @config_schema,
               type: :uuid
             ),
             null: false
    end
  end
end
