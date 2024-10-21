defmodule Sequin.Repo.Migrations.AddAnnotationsToWalPipelines do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    alter table(:wal_pipelines, prefix: @config_schema) do
      add :annotations, :map, default: "{}"
    end

    alter table(:http_push_consumers, prefix: @config_schema) do
      remove :replica_warning_dismissed
    end
  end

  def down do
    alter table(:wal_pipelines, prefix: @config_schema) do
      remove :annotations
    end

    alter table(:http_push_consumers, prefix: @config_schema) do
      add :replica_warning_dismissed, :boolean, default: false
    end
  end
end
