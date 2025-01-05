defmodule Sequin.Repo.Migrations.CreateHealthSnapshots do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute "create type #{@config_schema}.health_status as enum ('healthy', 'warning', 'error', 'initializing', 'waiting')"

    execute "create type #{@config_schema}.health_entity_kind as enum ('http_endpoint', 'sink_consumer', 'postgres_database', 'wal_pipeline')"

    create table(:health_snapshots, prefix: @config_schema) do
      add :entity_id, :string, null: false
      add :entity_kind, :"#{@config_schema}.health_entity_kind", null: false
      add :name, :string, null: false
      add :status, :"#{@config_schema}.health_status", null: false
      add :health_json, :map, null: false
      add :sampled_at, :utc_datetime_usec, null: false

      timestamps()
    end

    create index(:health_snapshots, [:entity_id, :sampled_at], prefix: @config_schema)
    create unique_index(:health_snapshots, [:entity_kind, :entity_id], prefix: @config_schema)
    create index(:health_snapshots, [:sampled_at], prefix: @config_schema)
  end

  def down do
    drop table(:health_snapshots, prefix: @config_schema)
    execute "drop type #{@config_schema}.health_status"
    execute "drop type #{@config_schema}.health_entity_kind"
  end
end
