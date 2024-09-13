defmodule Sequin.Repo.Migrations.AddTablesSortColumnAttnumsToPostgresDatabases do
  use Ecto.Migration

  @config_schema_prefix Application.compile_env!(:sequin, Sequin.Repo)
                        |> Keyword.fetch!(:config_schema_prefix)

  def change do
    alter table(:postgres_databases) do
      add :tables_sort_column_attnums, :map
    end

    execute "update #{@config_schema_prefix}.postgres_databases set tables_sort_column_attnums = '{}'::jsonb",
            "select 1"

    alter table(:postgres_databases) do
      modify :tables_sort_column_attnums, :map, null: false
    end
  end
end
