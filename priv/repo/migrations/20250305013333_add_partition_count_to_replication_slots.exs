defmodule Sequin.Repo.Migrations.AddPartitionCountToReplicationSlots do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:postgres_replication_slots, prefix: @config_schema) do
      add :partition_count, :integer, null: false, default: 1
    end
  end
end
