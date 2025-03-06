defmodule Sequin.Repo.Migrations.AddPartitionCountToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :partition_count, :integer, default: 1, null: false
    end
  end
end
