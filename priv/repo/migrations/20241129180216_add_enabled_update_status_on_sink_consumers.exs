defmodule Sequin.Repo.Migrations.AddEnabledUpdateStatusOnSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :enabled, :boolean, default: true
    end

    execute """
      update #{@config_schema}.sink_consumers
      set enabled = (status = 'active')
    """

    alter table(:sink_consumers, prefix: @config_schema) do
      modify :enabled, :boolean, null: false, default: true
    end

    alter table(:sink_consumers, prefix: @config_schema) do
      remove :status
    end

    execute "drop type #{@config_schema}.consumer_status"

    execute "create type #{@config_schema}.consumer_status as enum ('ready', 'creating', 'deleting', 'updating');"

    alter table(:sink_consumers, prefix: @config_schema) do
      add :status, :"#{@config_schema}.consumer_status", default: "ready"
    end

    execute """
      update #{@config_schema}.sink_consumers
      set status = 'ready'
    """

    alter table(:sink_consumers, prefix: @config_schema) do
      modify :status, :"#{@config_schema}.consumer_status", null: false, default: "ready"
    end
  end

  def down do
    alter table(:sink_consumers, prefix: @config_schema) do
      remove :status
    end

    execute "drop type #{@config_schema}.consumer_status"

    # Recreate the original consumer_status enum type
    execute "create type #{@config_schema}.consumer_status as enum ('active', 'disabled');"

    alter table(:sink_consumers, prefix: @config_schema) do
      add :status, :"#{@config_schema}.consumer_status", default: "active"
    end

    # Convert enabled boolean back to status enum
    execute """
      update #{@config_schema}.sink_consumers
      set status = case when enabled then 'active' else 'disabled' end
    """

    alter table(:sink_consumers, prefix: @config_schema) do
      modify :status, :"#{@config_schema}.consumer_status", null: false, default: "active"
    end

    alter table(:sink_consumers, prefix: @config_schema) do
      remove :enabled
    end
  end
end
