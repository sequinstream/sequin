defmodule Sequin.Repo.Migrations.AddMessageGroupingToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :message_grouping, :boolean, default: true, null: false
    end
  end
end
