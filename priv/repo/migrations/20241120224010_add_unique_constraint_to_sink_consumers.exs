defmodule Sequin.Repo.Migrations.AddUniqueConstraintToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    create unique_index(:sink_consumers, [:account_id, :id], prefix: @config_schema)
  end

  def down do
    drop unique_index(:sink_consumers, [:account_id, :id], prefix: @config_schema)
  end
end
