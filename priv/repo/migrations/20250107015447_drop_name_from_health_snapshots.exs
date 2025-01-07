defmodule Sequin.Repo.Migrations.DropNameFromHealthSnapshots do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    execute "alter type #{@config_schema}.health_entity_kind add value 'postgres_replication_slot'"

    alter table(:health_snapshots, prefix: @config_schema) do
      remove :name
    end
  end

  def down do
    alter table(:health_snapshots, prefix: @config_schema) do
      add :name, :string
    end
  end
end
