defmodule Sequin.Repo.Migrations.AddTableOidToBackfills do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      add :table_oid, :integer
    end

    # TODO: Add a migration to update the table_oid for all backfills
    # TODO: Set table_oid to not null
  end
end
