defmodule Sequin.Repo.Migrations.AddStatusToHealthSnapshots do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    execute "alter type #{@config_schema}.health_status add value 'paused'"
  end

  def down do
    :ok
  end
end
