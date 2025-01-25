defmodule Sequin.Repo.Migrations.AddSafeSlotAdvanceLsnToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :safe_slot_advance_lsn, :bigint, null: true
    end

    execute "UPDATE #{@config_schema}.sink_consumers SET safe_slot_advance_lsn = 0"

    alter table(:sink_consumers, prefix: @config_schema) do
      modify :safe_slot_advance_lsn, :bigint, null: false
    end
  end
end
