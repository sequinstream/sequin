defmodule Sequin.Repo.Migrations.AlterRowsIntitialCountToInt8 do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      modify :rows_initial_count, :bigint
    end
  end
end
