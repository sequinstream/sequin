defmodule Sequin.Repo.Migrations.DropHttpPullConsumersTable do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    drop table(:http_pull_consumers, prefix: @config_schema)
  end
end
