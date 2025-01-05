defmodule Sequin.Repo.Migrations.AddAnnotationsToMoreTables do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:postgres_databases, prefix: @config_schema) do
      add :annotations, :map, default: "{}", null: false
    end

    alter table(:postgres_replication_slots, prefix: @config_schema) do
      add :annotations, :map, default: "{}", null: false
    end

    alter table(:sink_consumers, prefix: @config_schema) do
      add :annotations, :map, default: "{}", null: false
    end

    alter table(:accounts, prefix: @config_schema) do
      add :annotations, :map, default: "{}", null: false
    end
  end
end
