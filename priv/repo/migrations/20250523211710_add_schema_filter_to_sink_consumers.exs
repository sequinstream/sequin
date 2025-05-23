defmodule Sequin.Repo.Migrations.AddSchemaFilterToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :schema_filter, :jsonb, null: true
    end
  end
end
