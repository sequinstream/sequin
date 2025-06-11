defmodule Sequin.Repo.Migrations.AddSinkConsumerSource do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :actions, {:array, :string}, null: false, default: ["insert", "update", "delete"]
      add :source, :jsonb, null: true
      add :source_tables, {:array, :jsonb}, null: true
    end
  end
end
