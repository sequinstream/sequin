defmodule Sequin.Repo.Migrations.AddMaxMemoryMbToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :max_memory_mb, :integer, default: 1024, null: false
    end
  end
end
