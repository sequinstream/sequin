defmodule Sequin.Repo.Migrations.ModifyBackfillStatsToBigint do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      modify :rows_processed_count, :bigint, default: 0, null: false
      modify :rows_ingested_count, :bigint, default: 0, null: false
    end
  end
end
