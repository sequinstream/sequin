defmodule Sequin.Repo.Migrations.AddPrimaryJsonbToPostgresdatabase do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            alter table #{@config_schema}.postgres_databases add column "primary" jsonb
            """,
            """
            alter table #{@config_schema}.postgres_databases drop column "primary"
            """
  end
end
