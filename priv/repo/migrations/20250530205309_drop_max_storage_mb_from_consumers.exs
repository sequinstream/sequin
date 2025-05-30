defmodule Sequin.Repo.Migrations.DropMaxStorageMbFromConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    alter table(:sink_consumers, prefix: @config_schema) do
      remove :max_storage_mb
    end
  end

  def down do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :max_storage_mb, :integer, null: true
    end

    create constraint(:sink_consumers, :max_storage_mb_greater_than_max_memory_mb,
             check: "(max_storage_mb IS NULL) OR (max_storage_mb > max_memory_mb)"
           )
  end
end
