defmodule Sequin.Repo.Migrations.AddPgReplicationDisabled do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute "CREATE TYPE #{@config_schema}.pg_replication_slot_status AS ENUM ('active', 'disabled')",
            "DROP TYPE #{@config_schema}.pg_replication_slot_status"

    alter table(:postgres_replication_slots) do
      add :status, :"#{@config_schema}.pg_replication_slot_status",
        null: false,
        default: "active"
    end
  end
end
