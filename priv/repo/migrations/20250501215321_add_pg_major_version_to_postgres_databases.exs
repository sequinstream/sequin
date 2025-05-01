defmodule Sequin.Repo.Migrations.AddPgMajorVersionToPostgresDatabases do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:postgres_databases, prefix: @config_schema) do
      add :pg_major_version, :integer
    end
  end
end
