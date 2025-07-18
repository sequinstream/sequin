defmodule Sequin.Repo.Migrations.AddMaxTimeoutToBackfills do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:backfills, prefix: @config_schema) do
      add :max_timeout_ms, :integer, default: 5000
    end
  end
end
