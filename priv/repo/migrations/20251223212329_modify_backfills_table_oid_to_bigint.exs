defmodule Sequin.Repo.Migrations.ModifyBackfillsTableOidToBigint do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      modify :table_oid, :bigint
    end
  end
end
